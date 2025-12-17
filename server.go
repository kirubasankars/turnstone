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
)

type Server struct {
	store       *Store
	addr        string
	logger      *slog.Logger
	listener    net.Listener
	maxConns    int
	sem         chan struct{}
	wg          sync.WaitGroup
	totalConns  uint64
	activeConns int64
	usedMemory  int64
	activeTxs   int64
}

func NewServer(addr string, store *Store, logger *slog.Logger, maxConns int) *Server {
	return &Server{
		addr:     addr,
		store:    store,
		logger:   logger,
		maxConns: maxConns,
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
	s.logger.Info("Binary Server listening", "addr", s.addr)

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
		_ = conn.Close()
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
		_ = conn.SetReadDeadline(time.Now().Add(IdleTimeout))

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

			_ = conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))

			if tx.active {
				switch opCode {
				case OpCodePing, OpCodeQuit, OpCodeStat, OpCodeCompact:
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

func (s *Server) writeBinaryResponse(w io.Writer, status byte, body []byte) error {
	header := make([]byte, 5)
	header[0] = status
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
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

	// Read-Only Optimization: Snapshot Isolation consistency is guaranteed without commit.
	if tx.readOnly {
		s.abortTx(tx)
		_ = s.writeBinaryResponse(w, ResStatusOK, nil)
		return
	}

	// Read-Write Transaction: Check conflicts using full ops list (Gets+Sets+Dels)
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
	// Record operation for conflict detection
	tx.ops = append(tx.ops, bufferedOp{opType: OpJournalGet, key: key})

	// Read-Your-Writes logic (scan backwards)
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

	// Memory Limit Check
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
	tx.readOnly = false // Mutation flips flag

	tx.ops = append(tx.ops, bufferedOp{
		opType: OpJournalSet,
		key:    key,
		val:    valCopy,
	})
	_ = s.writeBinaryResponse(conn, ResStatusOK, nil)
}

func (s *Server) handleDelete(w io.Writer, payload []byte, tx *txState) {
	if len(payload) == 0 {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Missing Key"))
		return
	}
	if !s.checkTx(w, tx) {
		return
	}

	key := string(payload)
	tx.readOnly = false // Mutation flips flag

	tx.ops = append(tx.ops, bufferedOp{opType: OpJournalDelete, key: key})
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleStat(w io.Writer) {
	count, uptime, pending, activeSnaps, offset, generation := s.store.Stats()
	active := atomic.LoadInt64(&s.activeConns)
	mem := atomic.LoadInt64(&s.usedMemory)

	msg := fmt.Sprintf("Keys:%d Uptime:%s Conns:%d TxMem:%d Pending:%d Snaps:%d Offset:%d,%d",
		count, uptime, active, mem, pending, activeSnaps, generation, offset)

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
