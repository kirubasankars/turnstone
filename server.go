package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StoreConfig struct {
	Dir           string
	Logger        *slog.Logger
	Fsync         bool
	FsyncInterval time.Duration
}

type Server struct {
	stores      []*Store // Slice of stores [0..15], entries may be nil if not initialized
	storeMu     sync.RWMutex
	storeConfig StoreConfig

	addr        string
	metricsAddr string
	logger      *slog.Logger
	listener    net.Listener
	maxConns    int
	maxSyncs    int
	readOnly    bool // If true, only GET/SYNC operations are allowed
	sem         chan struct{}
	wg          sync.WaitGroup
	totalConns  uint64
	activeConns int64
	activeSyncs int64
	usedMemory  int64
	activeTxs   int64
	tlsConfig   *tls.Config
}

func NewServer(addr string, metricsAddr string, storeConfig StoreConfig, logger *slog.Logger, maxConns int, maxSyncs int, readOnly bool, tlsCert, tlsKey, tlsCA string) *Server {
	if tlsCert == "" || tlsKey == "" || tlsCA == "" {
		logger.Error("Server requires TLS cert, key, and CA file for mTLS")
		os.Exit(1)
	}

	cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
	if err != nil {
		logger.Error("Failed to load TLS keys", "err", err)
		os.Exit(1)
	}

	caCert, err := os.ReadFile(tlsCA)
	if err != nil {
		logger.Error("Failed to load CA", "err", err)
		os.Exit(1)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	return &Server{
		addr:        addr,
		metricsAddr: metricsAddr,
		stores:      make([]*Store, MaxDatabases), // Pre-allocate slice, but stores are nil
		storeConfig: storeConfig,
		logger:      logger,
		maxConns:    maxConns,
		maxSyncs:    maxSyncs,
		readOnly:    readOnly,
		sem:         make(chan struct{}, maxConns),
		tlsConfig:   tlsConfig,
	}
}

// getStore returns the store for the given index, initializing it if necessary.
func (s *Server) getStore(idx int) (*Store, error) {
	if idx < 0 || idx >= MaxDatabases {
		return nil, ErrInvalidDB
	}

	// Fast path: Check if already initialized
	s.storeMu.RLock()
	store := s.stores[idx]
	s.storeMu.RUnlock()

	if store != nil {
		return store, nil
	}

	// Slow path: Initialize under lock
	s.storeMu.Lock()
	defer s.storeMu.Unlock()

	// Double-check
	if s.stores[idx] != nil {
		return s.stores[idx], nil
	}

	dbPath := filepath.Join(s.storeConfig.Dir, strconv.Itoa(idx))
	// Pass allowTruncate=false, skipCrc=false by default for safety
	newStore, err := NewStore(dbPath, s.storeConfig.Logger, false, false, s.storeConfig.Fsync, s.storeConfig.FsyncInterval)
	if err != nil {
		return nil, err
	}

	s.stores[idx] = newStore
	s.logger.Info("Database initialized", "db", idx, "path", dbPath)
	return newStore, nil
}

// CloseAll closes all initialized stores.
func (s *Server) CloseAll() {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	for i, store := range s.stores {
		if store != nil {
			if err := store.Close(); err != nil {
				s.logger.Error("Failed to close store", "db", i, "err", err)
			}
			s.stores[i] = nil
		}
	}
}

func (s *Server) Run(ctx context.Context) error {
	if s.metricsAddr != "" {
		StartMetricsServer(s.metricsAddr, s, s.logger)
	}

	ln, err := tls.Listen("tcp", s.addr, s.tlsConfig)
	if err != nil {
		return err
	}
	s.listener = ln
	s.logger.Info("mTLS Server listening", "addr", s.addr, "readonly", s.readOnly, "lazy_init", true)

	// Ensure default DB 0 is initialized immediately
	if _, err := s.getStore(0); err != nil {
		return fmt.Errorf("failed to initialize default db 0: %w", err)
	}

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
	tx := &txState{
		dbIndex: 0, // Default to DB 0
	}

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
		if tx.isSyncClient {
			atomic.AddInt64(&s.activeSyncs, -1)
		}
		conn.Close()
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
			return
		}

		keepGoing := func() bool {
			if _, err := io.ReadFull(r, headerBuf); err != nil {
				return false
			}

			opCode := headerBuf[0]
			payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

			if payloadLen > MaxCommandSize {
				_ = s.writeBinaryResponse(conn, ResStatusEntityTooLarge, []byte("Payload too large"))
				return false
			}

			var payload []byte
			if payloadLen > 0 {
				payload = make([]byte, payloadLen)
				if _, err := io.ReadFull(r, payload); err != nil {
					return false
				}
			}

			if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
				return false
			}

			// Check Tx Constraints
			if tx.active {
				switch opCode {
				case OpCodePing, OpCodeQuit, OpCodeStat, OpCodeCompact, OpCodeSync, OpCodeSelect:
					_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Command not allowed in transaction"))
					return true
				}
			}

			switch opCode {
			case OpCodeSelect:
				s.handleSelect(conn, payload, tx)
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
				s.handleStat(conn, tx)
			case OpCodeCompact:
				s.handleCompact(conn, tx)
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

func (s *Server) handleSelect(w io.Writer, payload []byte, tx *txState) {
	if len(payload) != 1 {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid DB index format"))
		return
	}
	idx := int(payload[0])
	if idx < 0 || idx >= MaxDatabases {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("DB index out of range"))
		return
	}

	// Trigger Lazy Initialization on Select
	if _, err := s.getStore(idx); err != nil {
		s.logger.Error("Failed to lazy init db", "db", idx, "err", err)
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Failed to initialize database"))
		return
	}

	tx.dbIndex = idx
	_ = s.writeBinaryResponse(w, ResStatusOK, []byte(fmt.Sprintf("OK Selected %d", idx)))
}

func (s *Server) handleSync(w io.Writer, payload []byte, tx *txState) {
	if !tx.isSyncClient {
		current := atomic.LoadInt64(&s.activeSyncs)
		if current >= int64(s.maxSyncs) {
			_ = s.writeBinaryResponse(w, ResStatusServerBusy, []byte("Max sync clients reached"))
			return
		}
		atomic.AddInt64(&s.activeSyncs, 1)
		tx.isSyncClient = true
	}

	if len(payload) != 17 {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid payload size, expected 17 bytes"))
		return
	}

	dbIdx := int(payload[0])

	// Ensure DB is initialized for Sync
	targetStore, err := s.getStore(dbIdx)
	if err != nil {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid DB or Init Failed"))
		return
	}

	// Compaction Hold
	for targetStore.compactionRunning.Load() {
		if conn, ok := w.(net.Conn); ok {
			if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	reqGen := binary.BigEndian.Uint64(payload[1:9])
	reqOffset := int64(binary.BigEndian.Uint64(payload[9:17]))

	rc, nextOffset, currentGen, err := targetStore.Sync(reqGen, reqOffset)
	if err != nil {
		if err == ErrGenerationMismatch {
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, currentGen)
			_ = s.writeBinaryResponse(w, ResStatusGenMismatch, resp)
			return
		}
		s.logger.Error("Sync error", "err", err)
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Internal sync error"))
		return
	}
	if rc != nil {
		defer rc.Close()
	}

	dataLen := int64(0)
	if nextOffset > reqOffset {
		dataLen = nextOffset - reqOffset
	}

	metaLen := 16
	totalLen := int64(metaLen) + dataLen

	header := make([]byte, 5)
	header[0] = ResStatusOK
	binary.BigEndian.PutUint32(header[1:], uint32(totalLen))
	if _, err := w.Write(header); err != nil {
		return
	}

	meta := make([]byte, 16)
	binary.BigEndian.PutUint64(meta[0:8], currentGen)
	binary.BigEndian.PutUint64(meta[8:16], uint64(nextOffset))
	if _, err := w.Write(meta); err != nil {
		return
	}

	if dataLen > 0 && rc != nil {
		if _, err := io.Copy(w, rc); err != nil {
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

		// Ensure store exists (it should, as Tx started)
		if store, err := s.getStore(tx.dbIndex); err == nil {
			store.ReleaseSnapshot(tx.readVersion)
		}
		tx.ops = nil
	}
}

func (s *Server) handleBegin(w io.Writer, tx *txState) {
	if tx.active {
		s.abortTx(tx)
	}

	store, err := s.getStore(tx.dbIndex)
	if err != nil {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("DB init error"))
		return
	}

	tx.active = true
	tx.readOnly = true // Start as read-only
	atomic.AddInt64(&s.activeTxs, 1)
	tx.readVersion, tx.generation = store.AcquireSnapshot()
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

	if tx.readOnly || len(tx.ops) == 0 {
		s.abortTx(tx)
		_ = s.writeBinaryResponse(w, ResStatusOK, nil)
		return
	}

	if s.readOnly {
		s.abortTx(tx)
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Server is read-only"))
		return
	}

	store, err := s.getStore(tx.dbIndex)
	if err != nil {
		s.abortTx(tx)
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("DB init error"))
		return
	}

	if err := store.ApplyBatch(tx.ops, tx.readVersion, tx.generation); err != nil {
		if err == ErrConflict {
			_ = s.writeBinaryResponse(w, ResStatusTxConflict, []byte("Conflict detected"))
		} else {
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

	store, err := s.getStore(tx.dbIndex)
	if err != nil {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("DB init error"))
		return
	}

	val, err := store.Get(key, tx.readVersion, tx.generation)
	if err != nil {
		if err == ErrKeyNotFound {
			_ = s.writeBinaryResponse(w, ResStatusNotFound, nil)
		} else if err == ErrConflict {
			_ = s.writeBinaryResponse(w, ResStatusTxConflict, []byte("Snapshot lost"))
			s.abortTx(tx)
		} else {
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
	if len(payload) < 4+keyLen {
		_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Short Payload"))
		return
	}
	key := string(payload[4 : 4+keyLen])
	val := payload[4+keyLen:]
	valLen := len(val)

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
	tx.ops = append(tx.ops, bufferedOp{opType: OpJournalSet, key: key, val: valCopy})
	_ = s.writeBinaryResponse(conn, ResStatusOK, nil)
}

func (s *Server) handleDelete(w io.Writer, payload []byte, tx *txState) {
	if s.readOnly {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("Server is read-only"))
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

func (s *Server) handleStat(w io.Writer, tx *txState) {
	// For stats, we don't necessarily want to create the DB if it doesn't exist
	s.storeMu.RLock()
	store := s.stores[tx.dbIndex]
	s.storeMu.RUnlock()

	var count int
	var uptime string
	var pending int64
	var activeSnaps int
	var offset int64
	var generation uint64

	if store != nil {
		count, uptime, pending, activeSnaps, offset, generation = store.Stats()
	} else {
		uptime = "0s (Uninitialized)"
	}

	active := atomic.LoadInt64(&s.activeConns)
	mem := atomic.LoadInt64(&s.usedMemory)

	msg := fmt.Sprintf("DB:%d Keys:%d Uptime:%s Conns:%d TxMem:%d Pending:%d Snaps:%d Offset:%d,%d",
		tx.dbIndex, count, uptime, active, mem, pending, activeSnaps, generation, offset)

	_ = s.writeBinaryResponse(w, ResStatusOK, []byte(msg))
}

func (s *Server) handleCompact(w io.Writer, tx *txState) {
	store, err := s.getStore(tx.dbIndex)
	if err != nil {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte("DB init error"))
		return
	}

	if err := store.Compact(); err != nil {
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
