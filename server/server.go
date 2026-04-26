package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"turnstone/protocol"
	"turnstone/replication"
	"turnstone/stonedb"
	"turnstone/store"
)

// Roles
const (
	RoleClient = "client"
	RoleAdmin  = "admin"
	RoleCDC    = "cdc"
	RoleServer = "server" // usually implies admin-like privileges for internal replication
)

type Server struct {
	stores           map[string]*store.Store
	defaultDB        string
	id               string // Unique Server ID
	addr             string
	logger           *slog.Logger
	listener         net.Listener
	maxConns         int
	sem              chan struct{}
	wg               sync.WaitGroup
	totalConns       uint64
	activeConns      int64
	tlsConfig        *tls.Config
	tlsCertFile      string
	tlsKeyFile       string
	tlsCAFile        string
	currentTLSConfig atomic.Value
	replManager      *replication.ReplicationManager
	devMode          bool

	// Metrics per database
	connsMu sync.Mutex
	dbConns map[string]int64

	// Active connections object tracking for StepDown
	activeClientsMu sync.Mutex
	activeClients   map[string]map[net.Conn]struct{}

	// Buffer Pool for Request Reading to prevent DoS via allocation churn
	bufPool sync.Pool
}

func NewServer(id string, addr string, stores map[string]*store.Store, logger *slog.Logger, maxConns int, tlsCert, tlsKey, tlsCA string, rm *replication.ReplicationManager, devMode bool) (*Server, error) {
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
		id:            id,
		addr:          addr,
		stores:        stores,
		defaultDB:     defDB,
		logger:        logger,
		maxConns:      maxConns,
		sem:           make(chan struct{}, maxConns),
		tlsCertFile:   tlsCert,
		tlsKeyFile:    tlsKey,
		tlsCAFile:     tlsCA,
		replManager:   rm,
		devMode:       devMode,
		dbConns:       make(map[string]int64),
		activeClients: make(map[string]map[net.Conn]struct{}),
		// Initialize Buffer Pool for standard requests
		bufPool: sync.Pool{
			New: func() interface{} {
				// Default 4KB buffer covers 99% of GET/PING/SELECT requests
				b := make([]byte, 4096)
				return &b
			},
		},
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

	// Start Replication Manager if present
	if s.replManager != nil {
		go s.replManager.Start()
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

// Addr returns the listener's network address.
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := tls.Listen("tcp", s.addr, s.tlsConfig)
	if err != nil {
		return err
	}
	s.listener = ln

	// INFO: Startup configuration
	s.logger.Info("TurnstoneDB Server Started",
		"version", "1.0.0",
		"addr", s.addr,
		"id", s.id,
		"databases", len(s.stores),
		"default_db", s.defaultDB,
		"max_conns", s.maxConns,
		"dev_mode", s.devMode,
	)

	go s.handleSignals(ctx)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				return nil
			}
			s.logger.Error("Accept failed", "err", err)
			continue
		}

		atomic.AddUint64(&s.totalConns, 1)
		select {
		case s.sem <- struct{}{}:
			atomic.AddInt64(&s.activeConns, 1)
			s.wg.Add(1)
			go s.handleConnection(ctx, conn)
		default:
			// WARN: Capacity limits
			s.logger.Warn("Max connections reached, rejecting client", "remote", conn.RemoteAddr().String())
			_ = s.writeBinaryResponse(conn, protocol.ResStatusServerBusy, []byte("Max connections"))
			_ = conn.Close()
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
			// INFO: Operator requested config reload
			s.logger.Info("Reloading TLS configuration...")
			if err := s.ReloadTLS(); err != nil {
				s.logger.Error("TLS reload failed", "err", err)
			} else {
				s.logger.Info("TLS configuration reloaded successfully")
			}
		}
	}
}

func (s *Server) trackConn(dbName string, delta int64) {
	s.connsMu.Lock()
	s.dbConns[dbName] += delta
	s.connsMu.Unlock()
}

// DatabaseConns returns number of active connections for a specific DB.
func (s *Server) DatabaseConns(dbName string) int64 {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	return s.dbConns[dbName]
}

func (s *Server) registerConn(db string, conn net.Conn) {
	s.activeClientsMu.Lock()
	defer s.activeClientsMu.Unlock()
	if s.activeClients[db] == nil {
		s.activeClients[db] = make(map[net.Conn]struct{})
	}
	s.activeClients[db][conn] = struct{}{}
}

func (s *Server) unregisterConn(db string, conn net.Conn) {
	s.activeClientsMu.Lock()
	defer s.activeClientsMu.Unlock()
	if m, ok := s.activeClients[db]; ok {
		delete(m, conn)
		if len(m) == 0 {
			delete(s.activeClients, db)
		}
	}
}

func (s *Server) killDBConnections(db string) {
	s.activeClientsMu.Lock()
	defer s.activeClientsMu.Unlock()
	if m, ok := s.activeClients[db]; ok {
		// We copy keys to avoid issues while iterating (though Close() is safe)
		// and we deleted the map entry anyway.
		for conn := range m {
			_ = conn.Close()
		}
		delete(s.activeClients, db)
	}
}

// connState is shared with replication.go
type connState struct {
	dbName      string
	db          *store.Store
	tx          *stonedb.Transaction
	txStartTime time.Time // Track start time for timeouts
	role        string
	clientID    string
	logger      *slog.Logger // Context-aware logger for this connection
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	// Base logger with remote IP (available immediately)
	connLogger := s.logger.With("remote", conn.RemoteAddr().String())

	// PANIC RECOVERY Middleware
	defer func() {
		if r := recover(); r != nil {
			connLogger.Error("CRITICAL: Panic recovered in client handler",
				"err", r,
				"stack", string(debug.Stack()))
			// Close connection explicitly to prevent leaks
			_ = conn.Close()
		}
	}()

	defer func() {
		_ = conn.Close()
		atomic.AddInt64(&s.activeConns, -1)
		s.wg.Done()
		<-s.sem
	}()

	// Identity defaults
	role := RoleClient
	clientID := "unknown"
	authType := "mtls"

	if tlsConn, ok := conn.(*tls.Conn); ok {
		// Handshake already happened in Accept but we ensure it's complete to get certs.
		if err := tlsConn.Handshake(); err == nil {
			state := tlsConn.ConnectionState()
			if len(state.PeerCertificates) > 0 {
				cert := state.PeerCertificates[0]
				// Use Organization as Role: "TurnstoneDB admin" -> "admin"
				if len(cert.Subject.Organization) > 0 {
					org := cert.Subject.Organization[0]
					if strings.HasPrefix(org, "TurnstoneDB ") {
						role = strings.TrimPrefix(org, "TurnstoneDB ")
					}
				}
				// Use CommonName as Unique Replica/Client ID
				if cert.Subject.CommonName != "" {
					clientID = cert.Subject.CommonName
				}
			}
		} else {
			// WARN: Security handshake issues
			connLogger.Warn("TLS Handshake failed", "err", err)
			return
		}
	}

	// Enrich logger with identity info
	connLogger = connLogger.With("client_id", clientID, "role", role)

	// DEBUG: High frequency connection events
	connLogger.Debug("Connection accepted", "auth_type", authType)

	state := &connState{
		dbName:   s.defaultDB,
		db:       s.stores[s.defaultDB],
		tx:       nil,
		role:     role,
		clientID: clientID,
		logger:   connLogger,
	}

	// Track connection for default DB
	s.trackConn(state.dbName, 1)
	s.registerConn(state.dbName, conn)
	defer func() {
		s.trackConn(state.dbName, -1)
		s.unregisterConn(state.dbName, conn)
		// DEBUG: Connection close
		connLogger.Debug("Connection closed")
	}()

	// Ensure active transaction is cleaned up if connection drops
	defer func() {
		if state.tx != nil {
			connLogger.Debug("Rolling back uncommitted transaction on disconnect")
			state.tx.Discard()
		}
	}()

	r := bufio.NewReader(conn)
	header := make([]byte, protocol.ProtoHeaderSize)

	for {
		if ctx.Err() != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(protocol.IdleTimeout))

		if _, err := io.ReadFull(r, header); err != nil {
			if err != io.EOF {
				// Don't log EOF as warning, it's normal client disconnect
				connLogger.Warn("Read header failed", "err", err)
			}
			return
		}

		opCode := header[0]
		payloadLen := binary.BigEndian.Uint32(header[1:])

		// 1. Safety Check: Enforce Protocol Limit BEFORE allocation
		if payloadLen > protocol.MaxCommandSize {
			connLogger.Warn("Protocol violation: entity too large", "size", payloadLen, "limit", protocol.MaxCommandSize)
			_ = s.writeBinaryResponse(conn, protocol.ResStatusEntityTooLarge, nil)
			return
		}

		var payload []byte
		var bufPtr *[]byte

		// 2. Allocation Strategy: Use Pool for standard requests to prevent GC churn
		// Only use pool if request is reasonably small (<= 4MB).
		// Huge payloads (e.g. 50MB bulk load) should be allocated directly
		// to avoid polluting the pool with oversized buffers that won't be reused often.
		const MaxPoolableSize = 4 * 1024 * 1024

		if payloadLen <= MaxPoolableSize {
			bufPtr = s.bufPool.Get().(*[]byte)
			buf := *bufPtr

			// Grow buffer if needed, but respect cap limits
			if uint32(cap(buf)) < payloadLen {
				// Too small, make a new one. The old one in bufPtr is lost to GC, which is fine.
				buf = make([]byte, payloadLen)
			} else {
				buf = buf[:payloadLen]
			}
			payload = buf
		} else {
			// Direct allocation for huge payloads
			payload = make([]byte, payloadLen)
		}

		// 3. Read Body
		if _, err := io.ReadFull(r, payload); err != nil {
			connLogger.Warn("Read payload failed (truncated)", "err", err)
			return
		}

		_ = conn.SetWriteDeadline(time.Now().Add(protocol.DefaultWriteTimeout))

		if s.dispatchCommand(conn, r, opCode, payload, state) {
			return
		}

		// 4. Return to Pool (if used and still efficient)
		if bufPtr != nil && uint32(cap(payload)) <= MaxPoolableSize {
			*bufPtr = payload
			s.bufPool.Put(bufPtr)
		}
	}
}

func (s *Server) dispatchCommand(conn net.Conn, r io.Reader, opCode uint8, payload []byte, st *connState) bool {
	// Access Control Check
	if !s.isOpAllowed(st.role, opCode) {
		st.logger.Warn("Access denied", "opcode", fmt.Sprintf("0x%02x", opCode))
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Permission Denied for role: "+st.role))
		return false
	}

	// Transaction Context Check: Ensure state-changing or administrative commands are not executed inside a transaction
	if st.tx != nil {
		switch opCode {
		case protocol.OpCodeSelect, protocol.OpCodeReplicaOf, protocol.OpCodeStepDown,
			protocol.OpCodePromote, protocol.OpCodeFlushDB, protocol.OpCodeCheckpoint,
			protocol.OpCodeStat, protocol.OpCodeQuit:
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Command not allowed inside a transaction"))
			return false
		}
	}

	// DEBUG: Verbose protocol logging (usually disabled)
	if s.logger.Enabled(context.Background(), slog.LevelDebug) && opCode != protocol.OpCodePing {
		st.logger.Debug("Dispatching command", "opcode", fmt.Sprintf("0x%02x", opCode), "payload_len", len(payload))
	}

	switch opCode {
	case protocol.OpCodePing:
		_ = s.writeBinaryResponse(conn, protocol.ResStatusOK, []byte("PONG"))
	case protocol.OpCodeQuit:
		return true
	case protocol.OpCodeSelect:
		s.handleSelect(conn, payload, st)
	case protocol.OpCodeBegin:
		s.handleBegin(conn, st)
	case protocol.OpCodeCommit:
		s.handleCommit(conn, st)
	case protocol.OpCodeAbort:
		s.handleAbort(conn, st)
	case protocol.OpCodeGet:
		s.handleGet(conn, payload, st)
	case protocol.OpCodeSet:
		s.handleSet(conn, payload, st)
	case protocol.OpCodeDel:
		s.handleDel(conn, payload, st)
	case protocol.OpCodeMGet:
		s.handleMGet(conn, payload, st)
	case protocol.OpCodeMSet:
		s.handleMSet(conn, payload, st)
	case protocol.OpCodeMDel:
		s.handleMDel(conn, payload, st)
	case protocol.OpCodeReplicaOf:
		s.handleReplicaOf(conn, payload, st)
	case protocol.OpCodePromote:
		s.handlePromote(conn, payload, st)
	case protocol.OpCodeStepDown:
		s.handleStepDown(conn, st)
	case protocol.OpCodeCheckpoint:
		s.handleCheckpoint(conn, st)
	case protocol.OpCodeFlushDB:
		s.handleFlushDB(conn, st)
	case protocol.OpCodeStat:
		s.handleStat(conn, st)
	case protocol.OpCodeReplHello:
		// Hand off control to specialized handler in replication.go
		s.HandleReplicaConnection(conn, r, payload, st)
		return true // Connection takeover
	default:
		st.logger.Warn("Unknown OpCode received", "opcode", fmt.Sprintf("0x%02x", opCode))
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Unknown OpCode"))
	}
	return false
}

func (s *Server) isOpAllowed(role string, opCode uint8) bool {
	// Admin and Server roles can do everything
	if role == RoleAdmin || role == RoleServer {
		return true
	}

	// Client Role
	if role == RoleClient {
		switch opCode {
		case protocol.OpCodePing, protocol.OpCodeQuit,
			protocol.OpCodeSelect, protocol.OpCodeBegin, protocol.OpCodeCommit, protocol.OpCodeAbort,
			protocol.OpCodeGet, protocol.OpCodeSet, protocol.OpCodeDel,
			protocol.OpCodeMGet, protocol.OpCodeMSet, protocol.OpCodeMDel, protocol.OpCodeStat:
			return true
		default:
			return false
		}
	}

	// CDC Role
	if role == RoleCDC {
		switch opCode {
		case protocol.OpCodePing, protocol.OpCodeQuit, protocol.OpCodeReplHello:
			return true
		default:
			return false
		}
	}

	return false
}

func (s *Server) writeBinaryResponse(w io.Writer, status byte, body []byte) error {
	header := make([]byte, protocol.ProtoHeaderSize)
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

// Handlers with updated logging usage (using st.logger)

func (s *Server) handleSelect(conn net.Conn, payload []byte, st *connState) {
	name := string(payload)
	if db, ok := s.stores[name]; ok {
		// Update connection counts
		s.trackConn(st.dbName, -1)
		s.unregisterConn(st.dbName, conn)

		st.dbName = name
		st.db = db

		s.trackConn(st.dbName, 1)
		s.registerConn(st.dbName, conn)

		// DEBUG: Client state change
		st.logger.Debug("Switched database", "db", name)
		_ = s.writeBinaryResponse(conn, protocol.ResStatusOK, nil)
	} else {
		// DEBUG: Invalid DB request
		st.logger.Debug("Select failed: DB not found", "db", name)
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Database not found"))
	}
}

func (s *Server) handleBegin(w io.Writer, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	// State Check: Only Primary or Replica allow transactions
	state := st.db.GetState()
	if state != store.StatePrimary && state != store.StateReplica {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database not available for transactions"))
		return
	}

	if st.tx != nil {
		_ = s.writeBinaryResponse(w, protocol.ResTxInProgress, nil)
		return
	}

	// Start a read-write transaction and record start time
	st.tx = st.db.NewTransaction(true)
	st.txStartTime = time.Now()
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleCommit(w io.Writer, st *connState) {
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	// Enforce 5s Timeout on Commit (unless in Dev mode)
	if !s.devMode && time.Since(st.txStartTime) > protocol.MaxTxDuration {
		st.tx.Discard()
		st.tx = nil
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxTimeout, []byte("Transaction exceeded 5s limit"))
		return
	}

	err := st.tx.Commit()
	st.tx = nil

	if err != nil {
		switch err {
		case stonedb.ErrDiskFull:
			s.logger.Error("Commit failed: Disk Full")
			_ = s.writeBinaryResponse(w, protocol.ResStatusServerBusy, []byte("ERR disk is full"))
		case stonedb.ErrWriteConflict:
			// DEBUG: Conflicts are normal operation, not system errors
			st.logger.Debug("Commit failed: Conflict")
			_ = s.writeBinaryResponse(w, protocol.ResStatusTxConflict, []byte(err.Error()))
		default:
			s.logger.Error("Commit failed: Internal Error", "err", err)
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		}
		return
	}
	// Wait for Quorum if required
	if st.db.MinReplicas() > 0 {
		// Wait for the latest OpID (which includes the tx we just committed)
		lastOpID := st.db.LastOpID()
		st.db.WaitForQuorum(lastOpID)
	}

	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleAbort(w io.Writer, st *connState) {
	if st.tx != nil {
		st.tx.Discard()
		st.tx = nil
	}
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleGet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	if st.db.GetState() == store.StateUndefined {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database undefined (no reads)"))
		return
	}

	key := string(payload)

	if !protocol.IsASCII(key) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
		return
	}

	val, err := st.tx.Get([]byte(key))
	if err == stonedb.ErrKeyNotFound {
		_ = s.writeBinaryResponse(w, protocol.ResStatusNotFound, nil)
	} else if err != nil {
		st.logger.Error("Get operation failed", "key", key, "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, val)
	}
}

func (s *Server) handleSet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.db.GetState() != store.StatePrimary && st.db.GetState() != store.StateSteppingDown {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Read-only/Undefined state"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, nil)
		return
	}
	kLen := binary.BigEndian.Uint32(payload[:4])
	if len(payload) < 4+int(kLen) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid payload size"))
		return
	}

	key := make([]byte, kLen)
	copy(key, payload[4:4+kLen])
	val := make([]byte, len(payload[4+kLen:]))
	copy(val, payload[4+kLen:])

	// Strict Size Checks
	if len(val) > protocol.MaxValueSize {
		_ = s.writeBinaryResponse(w, protocol.ResStatusEntityTooLarge, []byte("Value size exceeds 64KB limit"))
		return
	}
	if len(key)+len(val) > protocol.MaxCommandSize {
		_ = s.writeBinaryResponse(w, protocol.ResStatusEntityTooLarge, []byte("Key+Value size exceeds limit"))
		return
	}

	keyStr := string(key)

	if !protocol.IsASCII(keyStr) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
		return
	}

	if err := st.tx.Put(key, val); err != nil {
		st.logger.Error("Set operation failed", "key", keyStr, "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	}
}

func (s *Server) handleDel(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.db.GetState() != store.StatePrimary && st.db.GetState() != store.StateSteppingDown {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Read-only/Undefined state"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	key := string(payload)

	if !protocol.IsASCII(key) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
		return
	}

	// Check if key exists before deleting to provide feedback
	_, err := st.tx.Get([]byte(key))
	if err == stonedb.ErrKeyNotFound {
		// Key doesn't exist, return NotFound status (client will see "(nil)" or ErrNotFound)
		_ = s.writeBinaryResponse(w, protocol.ResStatusNotFound, nil)
		return
	} else if err != nil {
		st.logger.Error("Del existence check failed", "key", key, "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		return
	}

	if err := st.tx.Delete([]byte(key)); err != nil {
		st.logger.Error("Del operation failed", "key", key, "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	}
}

func (s *Server) handleMGet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	if st.db.GetState() == store.StateUndefined {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database undefined (no reads)"))
		return
	}
	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid mget payload"))
		return
	}

	numKeys := binary.BigEndian.Uint32(payload[0:4])
	offset := uint64(4)
	respBuf := new(bytes.Buffer)
	binary.Write(respBuf, binary.BigEndian, numKeys)

	// Use explicit bounds check to prevent DoS via huge numKeys
	// (though MaxCommandSize limits this effectively)
	if uint64(len(payload)) < offset {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mget payload"))
		return
	}

	for i := 0; i < int(numKeys); i++ {
		if offset+4 > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mget payload"))
			return
		}
		kLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4

		// Overflow safety check
		if offset+uint64(kLen) > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mget payload key"))
			return
		}

		key := payload[offset : offset+uint64(kLen)]
		offset += uint64(kLen)

		keyStr := string(key)

		if !protocol.IsASCII(keyStr) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
			return
		}

		val, err := st.tx.Get(key)
		if err == stonedb.ErrKeyNotFound {
			binary.Write(respBuf, binary.BigEndian, uint32(0xFFFFFFFF))
		} else if err != nil {
			st.logger.Error("MGet operation failed", "key", keyStr, "err", err)
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
			return
		} else {
			binary.Write(respBuf, binary.BigEndian, uint32(len(val)))
			respBuf.Write(val)
		}
	}

	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, respBuf.Bytes())
}

func (s *Server) handleMSet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.db.GetState() != store.StatePrimary && st.db.GetState() != store.StateSteppingDown {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Read-only/Undefined state"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid mset payload"))
		return
	}

	numPairs := binary.BigEndian.Uint32(payload[0:4])
	offset := uint64(4)

	for i := 0; i < int(numPairs); i++ {
		if offset+4 > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset payload"))
			return
		}
		kLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		if offset+uint64(kLen) > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset key"))
			return
		}
		key := payload[offset : offset+uint64(kLen)]
		offset += uint64(kLen)

		if offset+4 > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset val len"))
			return
		}
		vLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		if offset+uint64(vLen) > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset value"))
			return
		}
		val := payload[offset : offset+uint64(vLen)]
		offset += uint64(vLen)

		// Strict Size Checks
		if len(val) > protocol.MaxValueSize {
			_ = s.writeBinaryResponse(w, protocol.ResStatusEntityTooLarge, []byte("Value size exceeds 64KB limit"))
			return
		}
		if len(key)+len(val) > protocol.MaxCommandSize {
			_ = s.writeBinaryResponse(w, protocol.ResStatusEntityTooLarge, []byte("Key+Value size exceeds limit"))
			return
		}

		keyStr := string(key)

		if !protocol.IsASCII(keyStr) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
			return
		}

		if err := st.tx.Put(key, val); err != nil {
			st.logger.Error("MSet operation failed", "key", keyStr, "err", err)
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
			return
		}
	}

	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleMDel(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.db.GetState() != store.StatePrimary && st.db.GetState() != store.StateSteppingDown {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Read-only/Undefined state"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid mdel payload"))
		return
	}

	numKeys := binary.BigEndian.Uint32(payload[0:4])
	offset := uint64(4)
	deletedCount := uint32(0)

	for i := 0; i < int(numKeys); i++ {
		if offset+4 > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mdel payload"))
			return
		}
		kLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		if offset+uint64(kLen) > uint64(len(payload)) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mdel key"))
			return
		}
		key := payload[offset : offset+uint64(kLen)]
		offset += uint64(kLen)

		keyStr := string(key)

		if !protocol.IsASCII(keyStr) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
			return
		}

		_, err := st.tx.Get(key)
		exists := err == nil

		if err := st.tx.Delete(key); err != nil {
			st.logger.Error("MDel operation failed", "key", keyStr, "err", err)
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
			return
		}

		if exists {
			deletedCount++
		}
	}

	respBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(respBuf, deletedCount)
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, respBuf)
}

func (s *Server) handleReplicaOf(w io.Writer, payload []byte, st *connState) {
	if s.replManager == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Replication disabled on server"))
		return
	}

	// VALID ONLY IF UNDEFINED
	if st.db.GetState() != store.StateUndefined {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Must be in UNDEFINED state to call REPLICAOF"))
		return
	}

	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid payload"))
		return
	}
	addrLen := binary.BigEndian.Uint32(payload[:4])
	if len(payload) < 4+int(addrLen) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid payload addr len"))
		return
	}
	addr := string(payload[4 : 4+addrLen])
	remoteDB := string(payload[4+addrLen:])

	// Must provide an address (no empty stop command anymore, use STEPDOWN)
	if addr == "" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("REPLICAOF requires primary address"))
		return
	}

	// INFO: Significant role change
	st.logger.Info("Starting replication (Transition to REPLICA)", "db", st.dbName, "source", addr, "remote_db", remoteDB)

	// Transition State
	// We optimistically set state to REPLICA to allow the handshake (which might check state),
	// but we must revert if the handshake fails.
	st.db.SetState(store.StateReplica)

	if err := s.replManager.AddReplica(st.dbName, addr, remoteDB); err != nil {
		st.logger.Error("Replication handshake failed", "err", err)
		// Revert state on failure so the user can try again or Promote
		st.db.SetState(store.StateUndefined)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		return
	}

	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handlePromote(w io.Writer, payload []byte, st *connState) {
	currentState := st.db.GetState()

	// Consolidated check for consistency with REPLICAOF message style
	// Valid only if UNDEFINED or REPLICA
	if currentState != store.StateUndefined && currentState != store.StateReplica {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Must be in UNDEFINED or REPLICA state to call PROMOTE"))
		return
	}

	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid PROMOTE payload"))
		return
	}
	minReplicas := binary.BigEndian.Uint32(payload[:4])

	// INFO: Significant role change
	st.logger.Info("Promoting database to PRIMARY", "db", st.dbName, "min_replicas", minReplicas)

	// Stop existing replication if any
	s.replManager.StopReplication(st.dbName)

	// Configure the minimum replicas required for future writes on this primary
	// This is set BEFORE promotion logic completes to ensure consistency from op 1.
	st.db.SetMinReplicas(int(minReplicas))

	if err := st.db.Promote(); err != nil {
		st.logger.Error("Promote failed", "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		return
	}
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleStepDown(w io.Writer, st *connState) {
	// Only allow StepDown if actively running as Primary or Replica
	currentState := st.db.GetState()
	if currentState != store.StatePrimary && currentState != store.StateReplica {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Command only valid for PRIMARY or REPLICA state"))
		return
	}

	// INFO: Significant role change
	st.logger.Info("Stepping down database...", "db", st.dbName, "current_state", currentState)

	if currentState == store.StateReplica {
		// State 1: Stop incoming replication immediately
		if s.replManager != nil {
			s.replManager.StopReplication(st.dbName)
		}
		// Transition to UNDEFINED immediately
		st.db.SetState(store.StateUndefined)
	} else if currentState == store.StatePrimary {
		// State 1: Block NEW transactions (Transition to STEPPING_DOWN)
		st.db.SetState(store.StateSteppingDown)

		// State 2: Drain ACTIVE transactions
		st.logger.Info("Waiting for active transactions to drain...", "db", st.dbName)
		if err := st.db.WaitForActiveTransactions(5 * time.Second); err != nil {
			st.logger.Warn("Timeout waiting for active transactions", "err", err)
		}

		// State 3: Propagate changes to Replicas
		// Wait for all replicas to acknowledge the latest OpID
		st.logger.Info("Waiting for replicas to catch up...", "db", st.dbName)
		if err := st.db.WaitForReplication(5 * time.Second); err != nil {
			st.logger.Warn("Timeout waiting for replication sync", "err", err)
		}

		// State 4: Send Safe OpID (Trigger SafePoint Broadcast)
		st.logger.Info("Broadcasting final safe point...", "db", st.dbName)
		st.db.TriggerSafePoint()
		// Give a small moment for the broadcast loop to pick it up and write to network
		time.Sleep(50 * time.Millisecond)

		// State 5: Reset ReplicaSet (Clear slots)
		st.db.ResetReplicas()

		// Transition to UNDEFINED
		st.db.SetState(store.StateUndefined)
	}

	// Disconnect ALL clients (including self/current connection)
	go func(dbName string) {
		time.Sleep(20 * time.Millisecond) // Attempt to flush OK response
		s.killDBConnections(dbName)
	}(st.dbName)

	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleCheckpoint(w io.Writer, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}

	if st.role != RoleAdmin && st.role != RoleServer {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Permission Denied"))
		return
	}

	st.logger.Info("Manual checkpoint requested", "db", st.dbName)
	if err := st.db.Checkpoint(); err != nil {
		st.logger.Error("Manual checkpoint failed", "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		return
	}
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleFlushDB(w io.Writer, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}

	if st.role != RoleAdmin && st.role != RoleServer {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Permission Denied: FLUSHDB requires admin role"))
		return
	}

	st.logger.Warn("FLUSHDB requested", "db", st.dbName, "client", st.clientID)

	// 1. Stop Upstream Replication (Remove REPLICAOF)
	if s.replManager != nil && s.replManager.IsReplicating(st.dbName) {
		s.replManager.StopReplication(st.dbName)
	}

	// 2. Perform Reset (Wipes data, disconnects downstream replicas)
	// Reset() inside store.go handles s.RemoveAllReplicas() which kills existing downstream connections.
	if err := st.db.Reset(); err != nil {
		st.logger.Error("FLUSHDB failed", "err", err)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(fmt.Sprintf("FLUSHDB failed: %v", err)))
		return
	}

	// 3. Transition to UNDEFINED
	// The database is now empty and has no role. It requires explicit promotion or replication configuration.
	st.db.SetState(store.StateUndefined)

	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleStat(w io.Writer, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}

	stats := st.db.Stats()
	conns := s.DatabaseConns(st.dbName)
	dbState := st.db.GetState()
	minReplicas := st.db.MinReplicas()

	if dbState == store.StateReplica && s.replManager != nil {
		addr, remoteDB := s.replManager.GetReplicationSource(st.dbName)
		if addr != "" {
			dbState = fmt.Sprintf("REPLICAOF %s %s", addr, remoteDB)
		}
	}

	response := struct {
		State             string `json:"state"`
		KeyCount          int64  `json:"key_count"`
		GarbageBytes      int64  `json:"garbage_bytes"`
		Conflicts         uint64 `json:"conflicts"`
		ActiveConnections int64  `json:"active_connections"`
		VLogFiles         int    `json:"vlog_files"`
		WALFiles          int    `json:"wal_files"`
		ActiveTxs         int    `json:"active_txs"`
		ReplicaLag        uint64 `json:"replica_lag"`
		Uptime            string `json:"uptime"`
		MinReplicas       int    `json:"min_replicas"`
	}{
		State:             dbState,
		KeyCount:          stats.KeyCount,
		GarbageBytes:      stats.GarbageBytes,
		Conflicts:         stats.Conflicts,
		ActiveConnections: conns,
		VLogFiles:         stats.VLogFiles,
		WALFiles:          stats.WALFiles,
		ActiveTxs:         stats.ActiveTxs,
		ReplicaLag:        stats.ReplicaLag,
		Uptime:            stats.Uptime,
		MinReplicas:       minReplicas,
	}

	data, err := json.Marshal(response)
	if err != nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		return
	}
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, data)
}

func (s *Server) CloseAll() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for _, store := range s.stores {
		_ = store.Close()
	}
}

// ActiveConns returns number of activeConns
func (s *Server) ActiveConns() int64 {
	return atomic.LoadInt64(&s.activeConns)
}

// TotalConns returns number of totalConns
func (s *Server) TotalConns() uint64 {
	return atomic.LoadUint64(&s.totalConns)
}

// ActiveTxs return number of active transactions aggregated from all stores
func (s *Server) ActiveTxs() int64 {
	var total int64
	for _, st := range s.stores {
		total += int64(st.ActiveTransactionCount())
	}
	return total
}
