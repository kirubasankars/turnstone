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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Server struct {
	stores           map[string]*Store
	defaultDB        string
	addr             string
	metricsAddr      string
	logger           *slog.Logger
	listener         net.Listener
	maxConns         int
	sem              chan struct{}
	wg               sync.WaitGroup
	totalConns       uint64
	activeConns      int64
	activeTxs        int64
	txDuration       time.Duration
	tlsConfig        *tls.Config
	tlsCertFile      string
	tlsKeyFile       string
	tlsCAFile        string
	currentTLSConfig atomic.Value
	replManager      *ReplicationManager
}

func NewServer(addr, metricsAddr string, stores map[string]*Store, logger *slog.Logger, maxConns int, txDuration time.Duration, tlsCert, tlsKey, tlsCA string, rm *ReplicationManager) (*Server, error) {
	if tlsCert == "" || tlsKey == "" || tlsCA == "" {
		return nil, fmt.Errorf("tls cert, key, and ca required")
	}

	var dbNames []string
	for k := range stores {
		dbNames = append(dbNames, k)
	}
	sort.Strings(dbNames)
	defDB := ""
	if len(dbNames) > 0 {
		defDB = dbNames[0]
	}

	s := &Server{
		addr:        addr,
		metricsAddr: metricsAddr,
		stores:      stores,
		defaultDB:   defDB,
		logger:      logger,
		maxConns:    maxConns,
		txDuration:  txDuration,
		sem:         make(chan struct{}, maxConns),
		tlsCertFile: tlsCert,
		tlsKeyFile:  tlsKey,
		tlsCAFile:   tlsCA,
		replManager: rm,
	}

	if err := s.ReloadTLS(); err != nil {
		return nil, err
	}

	s.tlsConfig = &tls.Config{
		GetConfigForClient: func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			return s.currentTLSConfig.Load().(*tls.Config), nil
		},
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}

	return s, nil
}

func (s *Server) ReloadTLS() error {
	cert, err := tls.LoadX509KeyPair(s.tlsCertFile, s.tlsKeyFile)
	if err != nil {
		return err
	}
	caCert, err := os.ReadFile(s.tlsCAFile)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)

	s.currentTLSConfig.Store(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	return nil
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
	s.logger.Info("Server listening", "addr", s.addr, "dbs", len(s.stores), "default", s.defaultDB)

	go s.handleSignals(ctx)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				return nil
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
			s.writeBinaryResponse(conn, ResStatusServerBusy, []byte("Max connections"))
			conn.Close()
		}
	}
}

func (s *Server) handleSignals(ctx context.Context) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	for {
		select {
		case <-ctx.Done():
			return
		case <-sig:
			s.logger.Info("Reloading TLS...")
			if err := s.ReloadTLS(); err != nil {
				s.logger.Error("TLS reload failed", "err", err)
			}
		}
	}
}

type connState struct {
	dbName   string
	db       *Store
	active   bool
	readOnly bool
	deadline time.Time
	readTxID uint64 // Previously readLSN
	ops      []bufferedOp
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.activeConns, -1)
		s.wg.Done()
		<-s.sem
	}()

	state := &connState{
		dbName: s.defaultDB,
		db:     s.stores[s.defaultDB],
	}

	r := bufio.NewReader(conn)
	header := make([]byte, ProtoHeaderSize)

	for {
		if ctx.Err() != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(IdleTimeout))

		if _, err := io.ReadFull(r, header); err != nil {
			return
		}

		opCode := header[0]
		payloadLen := binary.BigEndian.Uint32(header[1:])
		if payloadLen > MaxCommandSize {
			s.writeBinaryResponse(conn, ResStatusEntityTooLarge, nil)
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}

		conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout))

		if s.dispatchCommand(conn, r, opCode, payload, state) {
			return
		}
	}
}

func (s *Server) dispatchCommand(conn net.Conn, r *bufio.Reader, opCode byte, payload []byte, st *connState) bool {
	// Guard active transactions
	if st.active && !isTxAllowedOp(opCode) {
		s.writeBinaryResponse(conn, ResStatusErr, []byte("Tx active"))
		return false
	}

	switch opCode {
	case OpCodePing:
		s.writeBinaryResponse(conn, ResStatusOK, []byte("PONG"))
	case OpCodeQuit:
		return true
	case OpCodeSelect:
		s.handleSelect(conn, payload, st)
	case OpCodeBegin:
		s.handleBegin(conn, st)
	case OpCodeCommit:
		s.handleCommit(conn, st)
	case OpCodeAbort:
		s.handleAbort(conn, st)
	case OpCodeGet:
		s.handleGet(conn, payload, st)
	case OpCodeSet:
		s.handleSet(conn, payload, st)
	case OpCodeDel:
		s.handleDel(conn, payload, st)
	case OpCodeCompact:
		s.handleCompact(conn, st)
	case OpCodeStat:
		s.handleStat(conn, st)
	case OpCodeReplicaOf:
		s.handleReplicaOf(conn, payload, st)
	case OpCodeReplHello:
		s.HandleReplicaConnection(conn, r, payload)
		return true
	default:
		s.writeBinaryResponse(conn, ResStatusErr, []byte("Unknown OpCode"))
	}
	return false
}

func isTxAllowedOp(op byte) bool {
	return op == OpCodeGet || op == OpCodeSet || op == OpCodeDel || op == OpCodeCommit || op == OpCodeAbort
}

func (s *Server) writeBinaryResponse(w io.Writer, status byte, body []byte) error {
	header := make([]byte, ProtoHeaderSize)
	header[0] = status
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(body) > 0 {
		_, err := w.Write(body)
		return err
	}
	return nil
}

// --- Handlers ---

func (s *Server) handleSelect(w io.Writer, payload []byte, st *connState) {
	name := string(payload)
	if store, ok := s.stores[name]; ok {
		st.dbName = name
		st.db = store
		s.writeBinaryResponse(w, ResStatusOK, nil)
	} else {
		s.writeBinaryResponse(w, ResStatusErr, []byte("DB not found"))
	}
}

func (s *Server) handleBegin(w io.Writer, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}
	if st.active {
		s.abortTx(st)
	}
	st.active = true
	st.readOnly = true
	st.deadline = time.Now().Add(s.txDuration)
	st.readTxID = st.db.AcquireSnapshot()
	st.ops = make([]bufferedOp, 0)
	atomic.AddInt64(&s.activeTxs, 1)
	s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleCommit(w io.Writer, st *connState) {
	if !st.active {
		s.writeBinaryResponse(w, ResStatusTxRequired, nil)
		return
	}
	defer s.abortTx(st)

	if time.Now().After(st.deadline) {
		s.writeBinaryResponse(w, ResStatusTxTimeout, nil)
		return
	}
	if st.readOnly || len(st.ops) == 0 {
		s.writeBinaryResponse(w, ResStatusOK, nil)
		return
	}

	// Prevent writes to System DB "0"
	if st.dbName == "0" {
		hasWrites := false
		for _, op := range st.ops {
			if op.opType == OpJournalSet || op.opType == OpJournalDelete {
				hasWrites = true
				break
			}
		}
		if hasWrites {
			s.writeBinaryResponse(w, ResStatusErr, []byte("System DB '0' is read-only"))
			return
		}
	}

	if err := st.db.ApplyBatch(st.ops, st.readTxID); err != nil {
		status := byte(ResStatusErr)
		if err == ErrConflict {
			status = ResStatusTxConflict
		}
		s.writeBinaryResponse(w, status, []byte(err.Error()))
		return
	}
	s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) abortTx(st *connState) {
	if st.active {
		if st.db != nil {
			st.db.ReleaseSnapshot(st.readTxID)
		}
		st.active = false
		st.ops = nil
		atomic.AddInt64(&s.activeTxs, -1)
	}
}

func (s *Server) handleAbort(w io.Writer, st *connState) {
	s.abortTx(st)
	s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleGet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}
	if !st.active {
		s.writeBinaryResponse(w, ResStatusTxRequired, nil)
		return
	}
	key := string(payload)
	st.ops = append(st.ops, bufferedOp{opType: OpJournalGet, key: key})

	// Read-your-own-writes
	for i := len(st.ops) - 2; i >= 0; i-- {
		if st.ops[i].key == key && st.ops[i].opType != OpJournalGet {
			if st.ops[i].opType == OpJournalDelete {
				s.writeBinaryResponse(w, ResStatusNotFound, nil)
			} else {
				s.writeBinaryResponse(w, ResStatusOK, st.ops[i].val)
			}
			return
		}
	}

	val, err := st.db.Get(key, st.readTxID)
	if err == ErrKeyNotFound {
		s.writeBinaryResponse(w, ResStatusNotFound, nil)
	} else if err != nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte(err.Error()))
	} else {
		s.writeBinaryResponse(w, ResStatusOK, val)
	}
}

func (s *Server) handleSet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}
	// Explicitly check for read-only system database
	if st.dbName == "0" {
		s.writeBinaryResponse(w, ResStatusErr, []byte("System DB '0' is read-only"))
		return
	}
	// Check replica status
	if s.replManager != nil && s.replManager.IsReplica(st.dbName) {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Replica database is read-only"))
		return
	}

	if !st.active {
		s.writeBinaryResponse(w, ResStatusTxRequired, nil)
		return
	}
	if len(payload) < 4 {
		s.writeBinaryResponse(w, ResStatusErr, nil)
		return
	}
	kLen := binary.BigEndian.Uint32(payload[:4])
	key := string(payload[4 : 4+kLen])
	val := payload[4+kLen:]

	// Check internal keys
	if strings.HasPrefix(key, "_") {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Key starting with '_' is read-only"))
		return
	}

	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	st.ops = append(st.ops, bufferedOp{opType: OpJournalSet, key: key, val: valCopy})
	st.readOnly = false
	s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleDel(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}
	// Explicitly check for read-only system database
	if st.dbName == "0" {
		s.writeBinaryResponse(w, ResStatusErr, []byte("System DB '0' is read-only"))
		return
	}
	// Check replica status
	if s.replManager != nil && s.replManager.IsReplica(st.dbName) {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Replica database is read-only"))
		return
	}

	if !st.active {
		s.writeBinaryResponse(w, ResStatusTxRequired, nil)
		return
	}

	key := string(payload)
	// Check internal keys
	if strings.HasPrefix(key, "_") {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Key starting with '_' is read-only"))
		return
	}

	st.ops = append(st.ops, bufferedOp{opType: OpJournalDelete, key: key})
	st.readOnly = false
	s.writeBinaryResponse(w, ResStatusOK, nil)
}

func (s *Server) handleCompact(w io.Writer, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}
	if err := st.db.Compact(); err != nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte(err.Error()))
	} else {
		s.writeBinaryResponse(w, ResStatusOK, nil)
	}
}

func (s *Server) handleStat(w io.Writer, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}
	stats := st.db.Stats()
	msg := fmt.Sprintf("[%s] Keys:%d Index:%d NextTxID:%d", st.dbName, stats.KeyCount, stats.IndexSizeBytes, stats.NextTxID)
	s.writeBinaryResponse(w, ResStatusOK, []byte(msg))
}

// handleReplicaOf handles the admin command to configure replication dynamically.
// Payload format: [AddrLen(4)][AddrBytes][RemoteDBNameBytes]
func (s *Server) handleReplicaOf(w io.Writer, payload []byte, st *connState) {
	if s.replManager == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Replication client not configured"))
		return
	}
	if st.db == nil {
		s.writeBinaryResponse(w, ResStatusErr, []byte("No DB selected"))
		return
	}

	if st.dbName == "0" {
		s.writeBinaryResponse(w, ResStatusErr, []byte("System DB '0' cannot be replicated"))
		return
	}

	if len(payload) < 4 {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid payload"))
		return
	}

	addrLen := binary.BigEndian.Uint32(payload[:4])
	if len(payload) < 4+int(addrLen) {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Invalid payload length"))
		return
	}

	addr := string(payload[4 : 4+addrLen])
	remoteDB := string(payload[4+addrLen:])

	// Empty Address signals STOP REPLICATION
	if addr == "" {
		s.replManager.StopReplication(st.dbName)
		s.logger.Info("ReplicaOf Stop Command", "local_db", st.dbName)
		s.writeBinaryResponse(w, ResStatusOK, []byte("Replication stopped"))
		return
	}

	if remoteDB == "" {
		s.writeBinaryResponse(w, ResStatusErr, []byte("Remote DB name required"))
		return
	}

	s.logger.Info("ReplicaOf Command", "local_db", st.dbName, "remote_addr", addr, "remote_db", remoteDB)
	s.replManager.AddReplica(st.dbName, addr, remoteDB)

	s.writeBinaryResponse(w, ResStatusOK, []byte("Replication started"))
}

func (s *Server) CloseAll() {
	if s.listener != nil {
		s.listener.Close()
	}
	for _, store := range s.stores {
		store.Close()
	}
}
