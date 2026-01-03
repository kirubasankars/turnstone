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
	"turnstone/store"
)

type Server struct {
	stores           map[string]*store.Store
	defaultDB        string
	addr             string
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
	replManager      *replication.ReplicationManager
}

func NewServer(addr string, stores map[string]*store.Store, logger *slog.Logger, maxConns int, txDuration time.Duration, tlsCert, tlsKey, tlsCA string, rm *replication.ReplicationManager) (*Server, error) {
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
			s.writeBinaryResponse(conn, protocol.ResStatusServerBusy, []byte("Max connections"))
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
	dbName string
	db     *store.Store
	active bool
	ops    []protocol.LogEntry
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
	header := make([]byte, protocol.ProtoHeaderSize)

	for {
		if ctx.Err() != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(protocol.IdleTimeout))

		if _, err := io.ReadFull(r, header); err != nil {
			return
		}

		opCode := header[0]
		payloadLen := binary.BigEndian.Uint32(header[1:])
		if payloadLen > protocol.MaxCommandSize {
			s.writeBinaryResponse(conn, protocol.ResStatusEntityTooLarge, nil)
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}

		conn.SetWriteDeadline(time.Now().Add(protocol.DefaultWriteTimeout))

		if s.dispatchCommand(conn, r, opCode, payload, state) {
			return
		}
	}
}

func (s *Server) dispatchCommand(conn net.Conn, r *bufio.Reader, opCode uint8, payload []byte, st *connState) bool {
	if st.active && !isTxAllowedOp(opCode) {
		s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Tx active"))
		return false
	}

	switch opCode {
	case protocol.OpCodePing:
		s.writeBinaryResponse(conn, protocol.ResStatusOK, []byte("PONG"))
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
	case protocol.OpCodeStat:
		s.handleStat(conn, st)
	case protocol.OpCodeReplicaOf:
		s.handleReplicaOf(conn, payload, st)
	case protocol.OpCodeReplHello:
		s.HandleReplicaConnection(conn, r, payload)
		return true
	default:
		s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Unknown OpCode"))
	}
	return false
}

func isTxAllowedOp(op uint8) bool {
	return op == protocol.OpCodeGet || op == protocol.OpCodeSet || op == protocol.OpCodeDel || op == protocol.OpCodeCommit || op == protocol.OpCodeAbort
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

func (s *Server) handleSelect(w io.Writer, payload []byte, st *connState) {
	name := string(payload)
	if db, ok := s.stores[name]; ok { // Renamed 'store' to 'db' to avoid shadowing package
		st.dbName = name
		st.db = db
		s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	} else {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("DB not found"))
	}
}

func (s *Server) handleBegin(w io.Writer, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No DB selected"))
		return
	}
	if st.active {
		s.abortTx(st)
	}
	st.active = true
	st.ops = make([]protocol.LogEntry, 0)
	atomic.AddInt64(&s.activeTxs, 1)
	s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleCommit(w io.Writer, st *connState) {
	if !st.active {
		s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	defer s.abortTx(st)

	if len(st.ops) == 0 {
		s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
		return
	}

	if st.dbName == "0" {
		for _, op := range st.ops {
			if op.OpCode == protocol.OpJournalSet || op.OpCode == protocol.OpJournalDelete {
				s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("System DB '0' is read-only"))
				return
			}
		}
	}

	if err := st.db.ApplyBatch(st.ops); err != nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
		return
	}
	s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) abortTx(st *connState) {
	if st.active {
		st.active = false
		st.ops = nil
		atomic.AddInt64(&s.activeTxs, -1)
	}
}

func (s *Server) handleAbort(w io.Writer, st *connState) {
	s.abortTx(st)
	s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleGet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No DB selected"))
		return
	}
	if !st.active {
		s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	key := string(payload)
	for i := len(st.ops) - 1; i >= 0; i-- {
		if string(st.ops[i].Key) == key {
			if st.ops[i].OpCode == protocol.OpJournalDelete {
				s.writeBinaryResponse(w, protocol.ResStatusNotFound, nil)
			} else {
				s.writeBinaryResponse(w, protocol.ResStatusOK, st.ops[i].Value)
			}
			return
		}
	}

	val, err := st.db.Get(key)
	if err == protocol.ErrKeyNotFound {
		s.writeBinaryResponse(w, protocol.ResStatusNotFound, nil)
	} else if err != nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		s.writeBinaryResponse(w, protocol.ResStatusOK, val)
	}
}

func (s *Server) handleSet(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No DB selected"))
		return
	}
	if st.dbName == "0" {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("System DB '0' is read-only"))
		return
	}
	if s.replManager != nil && s.replManager.IsReplica(st.dbName) {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Replica database is read-only"))
		return
	}

	if !st.active {
		s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	if len(payload) < 4 {
		s.writeBinaryResponse(w, protocol.ResStatusErr, nil)
		return
	}
	kLen := binary.BigEndian.Uint32(payload[:4])
	key := make([]byte, kLen)
	copy(key, payload[4:4+kLen])
	val := make([]byte, len(payload[4+kLen:]))
	copy(val, payload[4+kLen:])

	if strings.HasPrefix(string(key), "_") {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key starting with '_' is read-only"))
		return
	}

	st.ops = append(st.ops, protocol.LogEntry{OpCode: protocol.OpJournalSet, Key: key, Value: val})
	s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleDel(w io.Writer, payload []byte, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No DB selected"))
		return
	}
	if st.dbName == "0" {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("System DB '0' is read-only"))
		return
	}
	if s.replManager != nil && s.replManager.IsReplica(st.dbName) {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Replica database is read-only"))
		return
	}

	if !st.active {
		s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	key := make([]byte, len(payload))
	copy(key, payload)

	if strings.HasPrefix(string(key), "_") {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key starting with '_' is read-only"))
		return
	}

	st.ops = append(st.ops, protocol.LogEntry{OpCode: protocol.OpJournalDelete, Key: key})
	s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleStat(w io.Writer, st *connState) {
	if st.db == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No DB selected"))
		return
	}
	stats := st.db.Stats()
	msg := fmt.Sprintf("[%s] Keys:%d Mem:%d LogSeq:%d", st.dbName, stats.KeyCount, stats.MemorySizeBytes, stats.NextLogSeq)
	s.writeBinaryResponse(w, protocol.ResStatusOK, []byte(msg))
}

func (s *Server) handleReplicaOf(w io.Writer, payload []byte, st *connState) {
	if s.replManager == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Replication client not configured"))
		return
	}
	if st.db == nil {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No DB selected"))
		return
	}

	if st.dbName == "0" {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("System DB '0' cannot be replicated"))
		return
	}

	if len(payload) < 4 {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid payload"))
		return
	}

	addrLen := binary.BigEndian.Uint32(payload[:4])
	if len(payload) < 4+int(addrLen) {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Invalid payload length"))
		return
	}

	addr := string(payload[4 : 4+addrLen])
	remoteDB := string(payload[4+addrLen:])

	if addr == "" {
		s.replManager.StopReplication(st.dbName)
		s.logger.Info("ReplicaOf Stop Command", "local_db", st.dbName)
		s.writeBinaryResponse(w, protocol.ResStatusOK, []byte("Replication stopped"))
		return
	}

	if remoteDB == "" {
		s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Remote DB name required"))
		return
	}

	s.logger.Info("ReplicaOf Command", "local_db", st.dbName, "remote_addr", addr, "remote_db", remoteDB)
	s.replManager.AddReplica(st.dbName, addr, remoteDB)

	s.writeBinaryResponse(w, protocol.ResStatusOK, []byte("Replication started"))
}

func (s *Server) CloseAll() {
	if s.listener != nil {
		s.listener.Close()
	}
	for _, store := range s.stores {
		store.Close()
	}
}

// Stats Accessors for Metrics
func (s *Server) ActiveConns() int64 {
	return atomic.LoadInt64(&s.activeConns)
}

func (s *Server) TotalConns() uint64 {
	return atomic.LoadUint64(&s.totalConns)
}

func (s *Server) ActiveTxs() int64 {
	return atomic.LoadInt64(&s.activeTxs)
}

// --- Replication Multiplexer ---
type replPacket struct {
	dbName string
	data   []byte
	count  uint32
}

func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte) {
	if len(payload) < 8 {
		s.logger.Error("HandleReplicaConnection: payload too short for header")
		return
	}
	count := binary.BigEndian.Uint32(payload[4:8])

	cursor := 8
	type subReq struct {
		name   string
		logSeq uint64
	}
	var subs []subReq
	replicaID := conn.RemoteAddr().String()

	for i := 0; i < int(count); i++ {
		if cursor+4 > len(payload) {
			s.logger.Error("HandleReplicaConnection: payload truncated reading name len", "replica", replicaID)
			return
		}
		nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
		cursor += 4
		if cursor+nLen+8 > len(payload) {
			s.logger.Error("HandleReplicaConnection: payload truncated reading name/logSeq", "replica", replicaID)
			return
		}
		name := string(payload[cursor : cursor+nLen])
		cursor += nLen
		logSeq := binary.BigEndian.Uint64(payload[cursor : cursor+8])
		cursor += 8
		subs = append(subs, subReq{name, logSeq})

		if st, ok := s.stores[name]; ok {
			s.logger.Info("Replica subscribed", "replica", replicaID, "db", name, "startLogSeq", logSeq)
			st.RegisterReplica(replicaID, logSeq)
			defer st.UnregisterReplica(replicaID)
		} else {
			s.logger.Warn("Replica requested unknown db", "replica", replicaID, "db", name)
		}
	}

	outCh := make(chan replPacket, 10)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup
	for _, req := range subs {
		db, ok := s.stores[req.name] // Renamed 'store' to 'db'
		if !ok {
			continue
		}
		wg.Add(1)
		go func(name string, st *store.Store, startLogSeq uint64) {
			defer wg.Done()
			if err := s.streamDB(name, st, startLogSeq, outCh, done); err != nil {
				s.logger.Error("StreamDB failed", "db", name, "err", err)
				select {
				case errCh <- err:
				default:
				}
			}
		}(req.name, db, req.logSeq)
	}

	go func() {
		h := make([]byte, 5)
		for {
			if _, err := io.ReadFull(r, h); err != nil {
				return
			}
			if h[0] == protocol.OpCodeReplAck {
				ln := binary.BigEndian.Uint32(h[1:])
				b := make([]byte, ln)
				if _, err := io.ReadFull(r, b); err != nil {
					return
				}
				if len(b) > 4 {
					nL := binary.BigEndian.Uint32(b[:4])
					if len(b) >= 4+int(nL)+8 {
						dbName := string(b[4 : 4+nL])
						logSeq := binary.BigEndian.Uint64(b[4+nL:])
						if st, ok := s.stores[dbName]; ok {
							st.UpdateReplicaLogSeq(replicaID, logSeq)
						}
					}
				}
			}
		}
	}()

	for {
		select {
		case err := <-errCh:
			s.logger.Warn("Dropping slow/failed replica", "addr", replicaID, "err", err)
			return

		case p := <-outCh:
			totalLen := 4 + len(p.dbName) + 4 + len(p.data)
			header := make([]byte, 5)
			header[0] = protocol.OpCodeReplBatch
			binary.BigEndian.PutUint32(header[1:], uint32(totalLen))

			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(header); err != nil {
				return
			}

			nameH := make([]byte, 4)
			binary.BigEndian.PutUint32(nameH, uint32(len(p.dbName)))
			if _, err := conn.Write(nameH); err != nil {
				return
			}
			if _, err := conn.Write([]byte(p.dbName)); err != nil {
				return
			}

			cntH := make([]byte, 4)
			binary.BigEndian.PutUint32(cntH, p.count)
			if _, err := conn.Write(cntH); err != nil {
				return
			}
			if _, err := conn.Write(p.data); err != nil {
				return
			}
		}
	}
}

func (s *Server) streamDB(name string, st *store.Store, minLogSeq uint64, outCh chan<- replPacket, done <-chan struct{}) error {
	startOffset := st.FindOffsetForLogSeq(minLogSeq)
	curr := startOffset
	buf := make([]byte, 1024*1024)
	batchBuf := new(bytes.Buffer)
	scratch := make([]byte, 8)
	var count uint32

	flush := func() error {
		if count > 0 {
			data := make([]byte, batchBuf.Len())
			copy(data, batchBuf.Bytes())
			select {
			case outCh <- replPacket{dbName: name, data: data, count: count}:
			case <-time.After(protocol.ReplicationTimeout):
				return fmt.Errorf("replication send timeout for db %s", name)
			case <-done:
				return nil
			}
			batchBuf.Reset()
			count = 0
		}
		return nil
	}

	wal := st.WAL()
	for {
		select {
		case <-done:
			return nil
		default:
		}

		walSize := wal.Size()
		if curr >= walSize {
			if err := flush(); err != nil {
				return err
			}
			wal.Wait(curr)
			walSize = wal.Size()
		}

		readLen := int64(len(buf))
		if curr+readLen > walSize {
			readLen = walSize - curr
		}

		n, err := wal.ReadAt(buf[:readLen], curr)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			continue
		}

		parseOff := 0
		for parseOff < n {
			if parseOff+protocol.HeaderSize > n {
				break
			}
			if buf[parseOff] == 0 && buf[parseOff+1] == 0 && buf[parseOff+2] == 0 {
				aligned := (parseOff/4096 + 1) * 4096
				if aligned > n {
					break
				}
				parseOff = aligned
				continue
			}

			packed := binary.BigEndian.Uint32(buf[parseOff : parseOff+4])
			// Logic matches wal.go's unpackMeta
			isDel := (packed & 0x80000000) != 0
			kLen := (packed & 0x7FF80000) >> 19
			vLen := (packed & 0x0007FFFF)

			logSeq := binary.BigEndian.Uint64(buf[parseOff+4 : parseOff+12])

			payloadLen := int(kLen)
			if !isDel {
				payloadLen += int(vLen)
			}

			if parseOff+protocol.HeaderSize+payloadLen > n {
				break
			}

			if logSeq > minLogSeq {
				binary.BigEndian.PutUint64(scratch, logSeq)
				batchBuf.Write(scratch)

				op := protocol.OpJournalSet
				if isDel {
					op = protocol.OpJournalDelete
				}
				batchBuf.WriteByte(op)

				binary.BigEndian.PutUint32(scratch[:4], uint32(kLen))
				batchBuf.Write(scratch[:4])
				kStart := parseOff + protocol.HeaderSize
				batchBuf.Write(buf[kStart : kStart+int(kLen)])

				binary.BigEndian.PutUint32(scratch[:4], uint32(vLen))
				batchBuf.Write(scratch[:4])
				if !isDel {
					batchBuf.Write(buf[kStart+int(kLen) : kStart+payloadLen])
				}
				count++
				if batchBuf.Len() >= 64*1024 {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			parseOff += protocol.HeaderSize + payloadLen
		}
		curr += int64(parseOff)
	}
}
