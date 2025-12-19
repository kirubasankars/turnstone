package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type Server struct {
	store       *Store
	addr        string
	logger      *slog.Logger
	listener    net.Listener
	maxConns    int
	maxSyncs    int    // Limit for CDC clients
	authPass    string // Bcrypt Hash of the Password
	readOnly    bool   // Server is in read-only mode (Follower)
	sem         chan struct{}
	wg          sync.WaitGroup
	totalConns  uint64
	activeConns int64
	activeSyncs int64 // Counter for active CDC clients
	usedMemory  int64
	activeTxs   int64
}

func NewServer(addr string, store *Store, logger *slog.Logger, maxConns int, maxSyncs int, authPass string, readOnly bool) *Server {
	return &Server{
		addr:     addr,
		store:    store,
		logger:   logger,
		maxConns: maxConns,
		maxSyncs: maxSyncs,
		authPass: authPass,
		readOnly: readOnly,
		sem:      make(chan struct{}, maxConns),
	}
}

func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{KeepAlive: 30 * time.Second}
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.logger.Info("Binary Server listening", "addr", s.addr, "readonly", s.readOnly)

	acceptErr := make(chan error, 1)
	go func() {
		defer close(acceptErr)
		for {
			conn, err := ln.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				s.logger.Error("Accept error", "err", err)
				continue
			}
			atomic.AddUint64(&s.totalConns, 1)

			select {
			case s.sem <- struct{}{}:
				atomic.AddInt64(&s.activeConns, 1)
				s.wg.Add(1)
				go s.handleConnection(ctx, conn)
			default:
				s.logger.Warn("Max connections reached")
				_ = s.writeBinaryResponse(conn, ResStatusServerBusy, []byte("Max connections"))
				_ = conn.Close()
			}
		}
	}()

	<-ctx.Done()
	s.logger.Info("Shutdown received, draining connections...")
	_ = ln.Close()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("Connections drained")
	case <-time.After(ShutdownTimeout):
		s.logger.Warn("Shutdown timeout, forcing exit")
	}
	return nil
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	tx := &txState{}

	// If authPass is empty, authentication is disabled (authed = true)
	authed := s.authPass == ""
	authFailures := 0

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Panic", "stack", string(debug.Stack()))
		}
		if tx.memUsage > 0 {
			atomic.AddInt64(&s.usedMemory, -tx.memUsage)
		}
		if tx.active {
			s.abortTx(tx)
		}
		// If this connection was a Sync client, release the slot
		if tx.isSyncClient {
			atomic.AddInt64(&s.activeSyncs, -1)
		}
		if err := conn.Close(); err != nil {
			s.logger.Debug("Error closing connection", "err", err)
		}
		atomic.AddInt64(&s.activeConns, -1)
		s.wg.Done()
		<-s.sem
	}()

	r := bufio.NewReader(conn)
	headerBuf := make([]byte, ProtoHeaderSize)

	for {
		if ctx.Err() != nil {
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(IdleTimeout)); err != nil {
			s.logger.Debug("Failed to set read deadline", "err", err)
			return
		}

		keepGoing := func() bool {
			// 1. Read Header
			if _, err := io.ReadFull(r, headerBuf); err != nil {
				if err != io.EOF {
					s.logger.Debug("Read error", "err", err)
				}
				return false
			}

			opCode := headerBuf[0]
			payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

			if payloadLen > MaxCommandSize {
				_ = s.writeBinaryResponse(conn, ResStatusEntityTooLarge, []byte("Payload too large"))
				return false
			}

			// 2. Read Payload
			var payload []byte
			var bufPtr *[]byte

			if payloadLen > 0 {
				bufPtr = getBuffer(int(payloadLen))
				payload = *bufPtr
				defer putBuffer(bufPtr)

				if _, err := io.ReadFull(r, payload); err != nil {
					return false
				}
			}

			if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
				s.logger.Debug("Failed to set write deadline", "err", err)
				return false
			}

			// Handle AUTH immediately
			if opCode == OpCodeAuth {
				if s.handleAuth(conn, payload, &authed) {
					authFailures = 0 // Reset on success
				} else {
					authFailures++
					if authFailures > 3 {
						// Silent close or send error then close
						_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Too many authentication failures"))
						return false
					}
				}
				return true
			}

			// Check Authentication
			if !authed {
				switch opCode {
				case OpCodePing, OpCodeQuit, OpCodeStat:
					// Allowed without auth
				default:
					_ = s.writeBinaryResponse(conn, ResStatusAuthRequired, []byte("Authentication required"))
					return true
				}
			}

			// Check Transaction Constraints
			if tx.active {
				switch opCode {
				case OpCodePing, OpCodeQuit, OpCodeStat, OpCodeCompact, OpCodeSync:
					_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Command not allowed in transaction"))
					return true
				}
			}

			switch opCode {
			case OpCodeBegin:
				s.handleBegin(conn, tx)
			case OpCodeCommit:
				s.handleEnd(conn, tx)
			case OpCodeAbort:
				s.handleAbort(conn, tx)
			case OpCodeGet:
				s.handleGet(conn, payload, tx)
			case OpCodeSet:
				s.handleSet(conn, payload, tx)
			case OpCodeDel:
				s.handleDelete(conn, payload, tx)
			case OpCodeStat:
				s.handleStat(conn)
			case OpCodeCompact:
				s.handleCompact(conn)
			case OpCodeSync:
				s.handleSync(conn, payload, tx)
			case OpCodePing:
				_ = s.writeBinaryResponse(conn, ResStatusOK, []byte("PONG"))
			case OpCodeQuit:
				return false
			default:
				_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Unknown OpCode"))
			}

			return true
		}()

		if !keepGoing {
			return
		}
	}
}

func (s *Server) handleAuth(w io.Writer, payload []byte, authed *bool) bool {
	if s.authPass == "" {
		// Redis behavior: ERR Client sent AUTH, but no password is set
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Client sent AUTH, but no password is set"))
		return false
	}

	// Compare stored bcrypt hash with the received plaintext payload
	// payload is assumed to be the plaintext password sent by the client.
	// s.authPass is the bcrypt hash loaded from config.
	err := bcrypt.CompareHashAndPassword([]byte(s.authPass), payload)

	if err == nil {
		*authed = true
		_ = s.writeBinaryResponse(w, ResStatusOK, []byte("OK"))
		return true
	} else {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid password"))
		return false
	}
}

func (s *Server) handleSync(w io.Writer, payload []byte, tx *txState) {
	// CDC Client Quota Check
	if !tx.isSyncClient {
		current := atomic.LoadInt64(&s.activeSyncs)
		if current >= int64(s.maxSyncs) {
			s.logger.Warn("Max CDC sync clients reached", "current", current, "max", s.maxSyncs)
			_ = s.writeBinaryResponse(w, ResStatusServerBusy, []byte("Max sync clients reached"))
			return
		}
		// Upgrade connection to Sync Client
		atomic.AddInt64(&s.activeSyncs, 1)
		tx.isSyncClient = true
	}

	// Payload Request: [Generation 8 bytes][Offset 8 bytes]
	if len(payload) != 16 {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid payload size, expected 16 bytes"))
		return
	}

	reqGen := binary.BigEndian.Uint64(payload[0:8])
	reqOffset := int64(binary.BigEndian.Uint64(payload[8:16]))

	// Sync now returns a ReadCloser
	rc, nextOffset, currentGen, err := s.store.Sync(reqGen, reqOffset)
	if err != nil {
		if err == ErrGenerationMismatch {
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, currentGen)
			_ = s.writeBinaryResponse(w, ResStatusGenMismatch, resp)
			return
		}
		if err == ErrGenerationSwitch {
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, currentGen)
			_ = s.writeBinaryResponse(w, ResStatusGenSwitch, resp)
			return
		}
		s.logger.Error("Sync error", "err", err)
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Internal sync error"))
		return
	}

	// Ensure cleanup if rc was returned
	if rc != nil {
		defer rc.Close()
	}

	// Calculate payload size
	// Note: We are streaming only the data that was requested.
	// If NextOffset > ReqOffset, the length is the difference.
	dataLen := int64(0)
	if nextOffset > reqOffset {
		dataLen = nextOffset - reqOffset
	}

	// Response Format: [CurrentGen (8)] [NextOffset (8)] [Data (dataLen)]
	metaLen := 16
	totalLen := int64(metaLen) + dataLen

	// Write Header (5 bytes)
	header := make([]byte, 5)
	header[0] = ResStatusOK
	binary.BigEndian.PutUint32(header[1:], uint32(totalLen))
	if _, err := w.Write(header); err != nil {
		return
	}

	// Write Metadata (16 bytes)
	meta := make([]byte, 16)
	binary.BigEndian.PutUint64(meta[0:8], currentGen)
	binary.BigEndian.PutUint64(meta[8:16], uint64(nextOffset))
	if _, err := w.Write(meta); err != nil {
		return
	}

	// Stream Data using io.Copy (Zero-copy optimization)
	if dataLen > 0 && rc != nil {
		if _, err := io.Copy(w, rc); err != nil {
			s.logger.Warn("Sync stream error", "err", err)
			return
		}
	}
}

func (s *Server) writeBinaryResponse(w io.Writer, status byte, body []byte) error {
	header := make([]byte, 5)
	header[0] = status
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

	if _, err := w.Write(header); err != nil {
		s.logger.Debug("Write response header failed", "err", err)
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			s.logger.Debug("Write response body failed", "err", err)
			return err
		}
	}
	return nil
}

func (s *Server) checkTx(w io.Writer, tx *txState) bool {
	if !tx.active {
		_ = s.writeBinaryResponse(w, ResStatusTxRequired, []byte("Transaction required"))
		return false
	}
	if time.Now().After(tx.deadline) {
		s.abortTx(tx)
		_ = s.writeBinaryResponse(w, ResStatusTxTimeout, []byte("Transaction timed out"))
		return false
	}
	return true
}

func (s *Server) abortTx(tx *txState) {
	if tx.active {
		tx.active = false
		atomic.AddInt64(&s.activeTxs, -1)
		if tx.memUsage > 0 {
			atomic.AddInt64(&s.usedMemory, -tx.memUsage)
			tx.memUsage = 0
		}
		s.store.ReleaseSnapshot(tx.readVersion)
		tx.ops = nil
	}
}

func (s *Server) handleBegin(w io.Writer, tx *txState) {
	if s.readOnly {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Server is read-only"))
		return
	}

	if tx.active {
		if time.Now().Before(tx.deadline) {
			_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Transaction already active"))
			return
		}
		s.abortTx(tx)
	}

	tx.active = true
	tx.readOnly = true // Default state
	atomic.AddInt64(&s.activeTxs, 1)
	tx.readVersion, tx.generation = s.store.AcquireSnapshot()
	tx.deadline = time.Now().Add(MaxTxDuration)
	tx.ops = make([]bufferedOp, 0)
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleEnd(w io.Writer, tx *txState) {
	if !s.checkTx(w, tx) {
		return
	}

	defer func() {
		if tx.memUsage > 0 {
			atomic.AddInt64(&s.usedMemory, -tx.memUsage)
			tx.memUsage = 0
		}
	}()

	if tx.readOnly {
		s.abortTx(tx)
		_ = s.writeBinaryResponse(w, ResStatusOK, nil)
		return
	}

	if err := s.store.ApplyBatch(tx.ops, tx.readVersion, tx.generation); err != nil {
		if err == ErrConflict {
			_ = s.writeBinaryResponse(w, ResStatusTxConflict, []byte("Conflict detected"))
		} else {
			s.logger.Error("Commit failed", "err", err)
			_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Internal commit failure"))
		}
		s.abortTx(tx)
		return
	}

	s.abortTx(tx)
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleAbort(w io.Writer, tx *txState) {
	if !s.checkTx(w, tx) {
		return
	}
	s.abortTx(tx)
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleGet(w io.Writer, payload []byte, tx *txState) {
	if len(payload) == 0 {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Missing Key"))
		return
	}
	if !s.checkTx(w, tx) {
		return
	}

	if len(tx.ops) >= MaxTxOps {
		_ = s.writeBinaryResponse(w, ResStatusEntityTooLarge, []byte("Tx too large"))
		return
	}

	key := string(payload)
	tx.ops = append(tx.ops, bufferedOp{opType: OpJournalGet, key: key})

	for i := len(tx.ops) - 2; i >= 0; i-- {
		op := tx.ops[i]
		if op.key == key && op.opType != OpJournalGet {
			if op.opType == OpJournalDelete {
				_ = s.writeBinaryResponse(w, ResStatusNotFound, nil)
			} else {
				_ = s.writeBinaryResponse(w, ResStatusOK, op.val)
			}
			return
		}
	}

	val, err := s.store.Get(key, tx.readVersion, tx.generation)
	if err != nil {
		if err == ErrKeyNotFound {
			_ = s.writeBinaryResponse(w, ResStatusNotFound, nil)
		} else if err == ErrConflict {
			_ = s.writeBinaryResponse(w, ResStatusTxConflict, []byte("Snapshot lost"))
			s.abortTx(tx)
		} else {
			s.logger.Error("Store read error", "err", err)
			_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Internal Error"))
		}
		return
	}
	_ = s.writeBinaryResponse(w, ResStatusOK, val)
}

func (s *Server) handleSet(conn net.Conn, payload []byte, tx *txState) {
	if s.readOnly {
		_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Server is read-only"))
		return
	}

	if !s.checkTx(conn, tx) {
		return
	}

	if len(payload) < 4 {
		_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Bad format"))
		return
	}

	keyLen := int(binary.BigEndian.Uint32(payload[0:4]))
	if keyLen == 0 || keyLen > MaxKeySize {
		_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Invalid Key Length"))
		return
	}

	if len(payload) < 4+keyLen {
		_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Short Payload"))
		return
	}

	key := string(payload[4 : 4+keyLen])
	val := payload[4+keyLen:]
	valLen := len(val)

	if valLen > MaxValueSize {
		_ = s.writeBinaryResponse(conn, ResStatusEntityTooLarge, []byte("Value too large"))
		return
	}

	newUsage := atomic.AddInt64(&s.usedMemory, int64(valLen))
	if newUsage > MaxMemoryLimit {
		atomic.AddInt64(&s.usedMemory, -int64(valLen))
		_ = s.writeBinaryResponse(conn, ResStatusServerBusy, []byte("Memory limit exceeded"))
		_ = conn.Close()
		return
	}

	valCopy := make([]byte, valLen)
	copy(valCopy, val)

	tx.memUsage += int64(valLen)
	tx.readOnly = false

	tx.ops = append(tx.ops, bufferedOp{
		opType: OpJournalSet,
		key:    key,
		val:    valCopy,
	})
	_ = s.writeBinaryResponse(conn, ResStatusOK, nil)
}

func (s *Server) handleDelete(w io.Writer, payload []byte, tx *txState) {
	if s.readOnly {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Server is read-only"))
		return
	}

	if len(payload) == 0 {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Missing Key"))
		return
	}
	if !s.checkTx(w, tx) {
		return
	}

	key := string(payload)
	tx.readOnly = false

	tx.ops = append(tx.ops, bufferedOp{opType: OpJournalDelete, key: key})
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleStat(w io.Writer) {
	count, uptime, pending, activeSnaps, offset, generation := s.store.Stats()
	active := atomic.LoadInt64(&s.activeConns)
	activeSyncs := atomic.LoadInt64(&s.activeSyncs)
	mem := atomic.LoadInt64(&s.usedMemory)

	msg := fmt.Sprintf("Keys:%d Uptime:%s Conns:%d Syncs:%d/%d TxMem:%d Pending:%d Snaps:%d Offset:%d,%d",
		count, uptime, active, activeSyncs, s.maxSyncs, mem, pending, activeSnaps, generation, offset)

	_ = s.writeBinaryResponse(w, ResStatusOK, []byte(msg))
}

func (s *Server) handleCompact(w io.Writer) {
	if err := s.store.Compact(); err != nil {
		if err == ErrCompactionInProgress {
			_ = s.writeBinaryResponse(w, ResStatusServerBusy, []byte("Compaction running"))
		} else {
			s.logger.Error("Compaction failed", "err", err)
			_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Internal error"))
		}
		return
	}
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}
