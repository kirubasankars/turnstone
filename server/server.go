package server

import (
	"bufio"
	"bytes"
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
	defaultDB        string // Renamed from defaultPartition
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

	// Metrics per database
	connsMu sync.Mutex
	dbConns map[string]int64 // Renamed from partitionConns
}

func NewServer(id string, addr string, stores map[string]*store.Store, logger *slog.Logger, maxConns int, tlsCert, tlsKey, tlsCA string, rm *replication.ReplicationManager) (*Server, error) {
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
		id:          id,
		addr:        addr,
		stores:      stores,
		defaultDB:   defDB,
		logger:      logger,
		maxConns:    maxConns,
		sem:         make(chan struct{}, maxConns),
		tlsCertFile: tlsCert,
		tlsKeyFile:  tlsKey,
		tlsCAFile:   tlsCA,
		replManager: rm,
		dbConns:     make(map[string]int64),
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

func (s *Server) Run(ctx context.Context) error {
	ln, err := tls.Listen("tcp", s.addr, s.tlsConfig)
	if err != nil {
		return err
	}
	s.listener = ln
	s.logger.Info("TurnstoneDB Server Ready",
		"version", "1.0.0",
		"addr", s.addr,
		"id", s.id,
		"databases", len(s.stores),
		"default_db", s.defaultDB,
		"max_conns", s.maxConns,
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
			// Rate limit logging for rejected connections to avoid DOS logging
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

// PartitionConns is kept for interface compatibility if needed, aliasing DatabaseConns.
func (s *Server) PartitionConns(partition string) int64 {
	return s.DatabaseConns(partition)
}

// connState is shared with replication.go
type connState struct {
	dbName   string
	db       *store.Store
	tx       *stonedb.Transaction
	role     string
	clientID string
	logger   *slog.Logger // Context-aware logger for this connection
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	// Base logger with remote IP (available immediately)
	connLogger := s.logger.With("remote", conn.RemoteAddr().String())

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
			connLogger.Warn("TLS Handshake failed", "err", err)
			return
		}
	}

	// Enrich logger with identity info
	connLogger = connLogger.With("client_id", clientID, "role", role)
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
	defer func() {
		s.trackConn(state.dbName, -1)
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
		if payloadLen > protocol.MaxCommandSize {
			connLogger.Warn("Protocol violation: entity too large", "size", payloadLen, "limit", protocol.MaxCommandSize)
			_ = s.writeBinaryResponse(conn, protocol.ResStatusEntityTooLarge, nil)
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			connLogger.Warn("Read payload failed (truncated)", "err", err)
			return
		}

		_ = conn.SetWriteDeadline(time.Now().Add(protocol.DefaultWriteTimeout))

		if s.dispatchCommand(conn, r, opCode, payload, state) {
			return
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

	// Debug log only for mutation or complex ops to reduce noise
	if opCode != protocol.OpCodePing && opCode != protocol.OpCodeGet {
		if s.logger.Enabled(context.Background(), slog.LevelDebug) {
			st.logger.Debug("Dispatching command", "opcode", fmt.Sprintf("0x%02x", opCode), "payload_len", len(payload))
		}
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
			protocol.OpCodeMGet, protocol.OpCodeMSet, protocol.OpCodeMDel:
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

func (s *Server) handleSelect(w io.Writer, payload []byte, st *connState) {
	name := string(payload)
	if db, ok := s.stores[name]; ok {
		// Update connection counts
		s.trackConn(st.dbName, -1)
		st.dbName = name
		st.db = db
		s.trackConn(st.dbName, 1)

		st.logger.Debug("Switched database", "db", name)
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	} else {
		st.logger.Debug("Select failed: DB not found", "db", name)
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database not found"))
	}
}

func (s *Server) handleBegin(w io.Writer, st *connState) {
	if st.db == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Database selected"))
		return
	}
	if st.tx != nil {
		_ = s.writeBinaryResponse(w, protocol.ResTxInProgress, nil)
		return
	}
	// Start a read-write transaction
	st.tx = st.db.NewTransaction(true)
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleCommit(w io.Writer, st *connState) {
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	err := st.tx.Commit()
	st.tx = nil

	if err != nil {
		if err == stonedb.ErrDiskFull {
			st.logger.Error("Commit failed: Disk Full")
			_ = s.writeBinaryResponse(w, protocol.ResStatusServerBusy, []byte("ERR disk is full"))
		} else if err == stonedb.ErrWriteConflict {
			// Conflicts are normal operations, log as Debug or Info
			st.logger.Debug("Commit failed: Conflict")
			_ = s.writeBinaryResponse(w, protocol.ResStatusTxConflict, []byte(err.Error()))
		} else {
			st.logger.Error("Commit failed: Internal Error", "err", err)
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		}
		return
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

	key := string(payload)
	if len(key) > 0 && key[0] == '_' {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Keys starting with '_' are reserved"))
		return
	}

	if !isASCII(key) {
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
	if st.dbName == "0" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database 0 is read-only"))
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

	keyStr := string(key)
	if len(keyStr) > 0 && keyStr[0] == '_' {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Keys starting with '_' are reserved"))
		return
	}

	if !isASCII(keyStr) {
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
	if st.dbName == "0" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database 0 is read-only"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	key := string(payload)
	if len(key) > 0 && key[0] == '_' {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Keys starting with '_' are reserved"))
		return
	}

	if !isASCII(key) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
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
	if len(payload) < 4 {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid mget payload"))
		return
	}

	numKeys := binary.BigEndian.Uint32(payload[0:4])
	offset := 4
	respBuf := new(bytes.Buffer)
	binary.Write(respBuf, binary.BigEndian, numKeys)

	for i := 0; i < int(numKeys); i++ {
		if offset+4 > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mget payload"))
			return
		}
		kLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if offset+kLen > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mget payload key"))
			return
		}
		key := payload[offset : offset+kLen]
		offset += kLen

		keyStr := string(key)
		if len(keyStr) > 0 && keyStr[0] == '_' {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Keys starting with '_' are reserved"))
			return
		}

		if !isASCII(keyStr) {
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
	if st.dbName == "0" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database 0 is read-only"))
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
	offset := 4

	for i := 0; i < int(numPairs); i++ {
		if offset+4 > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset payload"))
			return
		}
		kLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if offset+kLen > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset key"))
			return
		}
		key := payload[offset : offset+kLen]
		offset += kLen

		if offset+4 > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset val len"))
			return
		}
		vLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if offset+vLen > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mset value"))
			return
		}
		val := payload[offset : offset+vLen]
		offset += vLen

		keyStr := string(key)
		if len(keyStr) > 0 && keyStr[0] == '_' {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Keys starting with '_' are reserved"))
			return
		}

		if !isASCII(keyStr) {
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
	if st.dbName == "0" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Database 0 is read-only"))
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
	offset := 4
	deletedCount := uint32(0)

	for i := 0; i < int(numKeys); i++ {
		if offset+4 > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mdel payload"))
			return
		}
		kLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if offset+kLen > len(payload) {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Malformed mdel key"))
			return
		}
		key := payload[offset : offset+kLen]
		offset += kLen

		keyStr := string(key)
		if len(keyStr) > 0 && keyStr[0] == '_' {
			_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Keys starting with '_' are reserved"))
			return
		}

		if !isASCII(keyStr) {
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

	if addr == "" && addrLen == 0 {
		st.logger.Info("Stopping replication", "db", st.dbName)
		s.replManager.StopReplication(st.dbName)
	} else {
		st.logger.Info("Starting replication", "db", st.dbName, "source", addr, "remote_db", remoteDB)
		s.replManager.AddReplica(st.dbName, addr, remoteDB)
	}
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) CloseAll() {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for _, store := range s.stores {
		_ = store.Close()
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 127 {
			return false
		}
	}
	return true
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
