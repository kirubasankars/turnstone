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
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Server represents the main network listener and connection manager.
// It handles mTLS termination, concurrency limiting, and protocol parsing.
type Server struct {
	store       *Store
	addr        string
	metricsAddr string
	logger      *slog.Logger
	listener    net.Listener

	// Concurrency Control
	maxConns    int
	readOnly    bool
	sem         chan struct{}  // Semaphore to limit concurrent connections.
	wg          sync.WaitGroup // WaitGroup to ensure graceful shutdown.
	totalConns  uint64         // Metric: Total accepted connections.
	activeConns int64          // Metric: Current active connections.
	activeTxs   int64          // Metric: Current active transactions.

	// TLS Configuration
	tlsConfig        *tls.Config
	tlsCertFile      string
	tlsKeyFile       string
	tlsCAFile        string
	currentTLSConfig atomic.Value // Stores *tls.Config for atomic hot-reloading.
}

// NewServer initializes the server with mTLS configuration.
func NewServer(addr string, metricsAddr string, store *Store, logger *slog.Logger, maxConns int, readOnly bool, tlsCert, tlsKey, tlsCA string) *Server {
	if tlsCert == "" || tlsKey == "" || tlsCA == "" {
		logger.Error("Server requires TLS cert, key, and CA file for mTLS")
		os.Exit(1)
	}

	s := &Server{
		addr:        addr,
		metricsAddr: metricsAddr,
		store:       store,
		logger:      logger,
		maxConns:    maxConns,
		readOnly:    readOnly,
		sem:         make(chan struct{}, maxConns), // Buffered channel acts as a semaphore.
		tlsCertFile: tlsCert,
		tlsKeyFile:  tlsKey,
		tlsCAFile:   tlsCA,
	}

	// Load initial TLS config
	initialConfig, err := s.loadTLSConfig()
	if err != nil {
		logger.Error("Failed to load initial TLS config", "err", err)
		os.Exit(1)
	}
	s.currentTLSConfig.Store(initialConfig)

	// Use GetConfigForClient to allow hot-swapping certificates via SIGHUP.
	s.tlsConfig = &tls.Config{
		GetConfigForClient: func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			return s.currentTLSConfig.Load().(*tls.Config), nil
		},
	}

	return s
}

// loadTLSConfig reads certs from disk and builds the TLS config.
func (s *Server) loadTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(s.tlsCertFile, s.tlsKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS keys: %w", err)
	}

	caCert, err := os.ReadFile(s.tlsCAFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to append CA certs")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert, // Enforce mTLS
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ReloadTLS re-reads certificates from disk and updates the active configuration.
func (s *Server) ReloadTLS() error {
	newConfig, err := s.loadTLSConfig()
	if err != nil {
		return err
	}
	s.currentTLSConfig.Store(newConfig)
	return nil
}

// CloseAll performs a hard close on the storage engine.
func (s *Server) CloseAll() {
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			s.logger.Error("Failed to close store", "err", err)
		}
	}
}

// Run starts the TCP listener and the main accept loop.
func (s *Server) Run(ctx context.Context) error {
	if s.metricsAddr != "" {
		StartMetricsServer(s.metricsAddr, s, s.logger)
	}

	ln, err := tls.Listen("tcp", s.addr, s.tlsConfig)
	if err != nil {
		return err
	}
	s.listener = ln
	s.logger.Info("mTLS Server listening", "addr", s.addr, "readonly", s.readOnly, "mode", "wal-fsync")

	// Setup signal handling for config reload
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)

	// Goroutine for handling signals
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sigChan:
				s.logger.Info("Received SIGHUP. Reloading TLS certificates...")
				if err := s.ReloadTLS(); err != nil {
					s.logger.Error("Failed to reload TLS", "err", err)
				} else {
					s.logger.Info("TLS certificates reloaded successfully")
				}
			}
		}
	}()

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

			// Semaphore pattern to limit concurrency
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

// handleConnection manages the lifecycle of a single client connection.
// It acts as the protocol event loop.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	tx := &txState{}

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Panic", "stack", string(debug.Stack()))
		}
		if tx.active {
			s.abortTx(tx)
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

		if _, err := io.ReadFull(r, headerBuf); err != nil {
			if err != io.EOF {
				s.logger.Debug("Connection read error", "err", err)
			}
			return
		}

		opCode := headerBuf[0]
		payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

		if payloadLen > MaxCommandSize {
			_ = s.writeBinaryResponse(conn, ResStatusEntityTooLarge, []byte("Payload too large"))
			return
		}

		var payload []byte
		if payloadLen > 0 {
			payload = make([]byte, payloadLen)
			if _, err := io.ReadFull(r, payload); err != nil {
				return
			}
		}

		if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
			return
		}

		shouldClose, err := s.processCommand(conn, opCode, payload, tx)
		if err != nil {
			s.logger.Error("Command processing error", "err", err)
		}
		if shouldClose {
			return
		}
	}
}

// processCommand dispatches the opcode to the specific handler.
func (s *Server) processCommand(conn net.Conn, opCode byte, payload []byte, tx *txState) (bool, error) {
	if tx.active {
		switch opCode {
		case OpCodePing, OpCodeQuit, OpCodeStat, OpCodeReplHello:
			_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Command not allowed in transaction"))
			return false, nil
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
		s.handleStat(conn, tx)
	case OpCodeCompact:
		s.handleCompact(conn)
	case OpCodeReplHello:
		s.HandleReplicaConnection(conn, payload)
		return true, nil
	case OpCodePing:
		_ = s.writeBinaryResponse(conn, ResStatusOK, []byte("PONG"))
	case OpCodeQuit:
		return true, nil
	default:
		_ = s.writeBinaryResponse(conn, ResStatusErr, []byte("Unknown OpCode"))
	}

	return false, nil
}

func (s *Server) writeBinaryResponse(w io.Writer, status byte, body []byte) error {
	header := make([]byte, ProtoHeaderSize)
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

// checkTx ensures a transaction is active and not timed out.
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

// abortTx rolls back the transaction state.
func (s *Server) abortTx(tx *txState) {
	if tx.active {
		tx.active = false
		atomic.AddInt64(&s.activeTxs, -1)
		s.store.ReleaseSnapshot(tx.readLSN)
		tx.ops = nil
	}
}

func (s *Server) handleBegin(w io.Writer, tx *txState) {
	if tx.active {
		s.abortTx(tx)
	}
	tx.active = true
	tx.readOnly = true
	atomic.AddInt64(&s.activeTxs, 1)
	tx.readLSN = s.store.AcquireSnapshot()
	tx.deadline = time.Now().Add(MaxTxDuration)
	tx.ops = make([]bufferedOp, 0)
	_ = s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleEnd(w io.Writer, tx *txState) {
	if !s.checkTx(w, tx) {
		return
	}

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

	if err := s.store.ApplyBatch(tx.ops, tx.readLSN); err != nil {
		if err == ErrConflict {
			_ = s.writeBinaryResponse(w, ResStatusTxConflict, []byte("Conflict detected"))
		} else if err == ErrMemoryLimitExceeded {
			_ = s.writeBinaryResponse(w, ResStatusMemoryLimit, []byte("Memory limit exceeded"))
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

	// Read-your-own-writes: Check transaction buffer first
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

	// Fetch from store using Snapshot Isolation
	val, err := s.store.Get(key, tx.readLSN)
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

	valCopy := make([]byte, len(val))
	copy(valCopy, val)

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

func (s *Server) handleCompact(w io.Writer) {
	if err := s.store.Compact(); err != nil {
		_ = s.writeBinaryResponse(w, ResStatusErr, []byte(err.Error()))
		return
	}
	_ = s.writeBinaryResponse(w, ResStatusOK, []byte("Compaction triggered"))
}

func (s *Server) handleStat(w io.Writer, tx *txState) {
	stats := s.store.Stats()
	active := atomic.LoadInt64(&s.activeConns)

	msg := fmt.Sprintf("Keys:%d Uptime:%s Conns:%d PendingOps:%d IndexSize:%d Offset:%d LSN:%d Conflicts:%d Recovery:%s",
		stats.KeyCount, stats.Uptime, active, stats.PendingOps, stats.IndexSizeBytes, stats.Offset, stats.NextLSN,
		stats.ConflictCount, stats.RecoveryDuration)

	_ = s.writeBinaryResponse(w, ResStatusOK, []byte(msg))
}
