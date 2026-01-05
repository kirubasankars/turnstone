// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package server

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

	"turnstone/protocol"
	"turnstone/replication"
	"turnstone/stonedb"
	"turnstone/store"
)

type Server struct {
	stores           map[string]*store.Store
	defaultPartition string
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
}

func NewServer(addr string, stores map[string]*store.Store, logger *slog.Logger, maxConns int, tlsCert, tlsKey, tlsCA string, rm *replication.ReplicationManager) (*Server, error) {
	if tlsCert == "" || tlsKey == "" || tlsCA == "" {
		return nil, fmt.Errorf("tls cert, key, and ca required")
	}

	var partitionNames []string
	for k := range stores {
		partitionNames = append(partitionNames, k)
	}
	sort.Strings(partitionNames)
	defPartition := ""
	if len(partitionNames) > 0 {
		defPartition = partitionNames[0]
	}

	s := &Server{
		addr:             addr,
		stores:           stores,
		defaultPartition: defPartition,
		logger:           logger,
		maxConns:         maxConns,
		sem:              make(chan struct{}, maxConns),
		tlsCertFile:      tlsCert,
		tlsKeyFile:       tlsKey,
		tlsCAFile:        tlsCA,
		replManager:      rm,
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
	s.logger.Info("Server listening (Persistent Mode)", "addr", s.addr, "partitions", len(s.stores), "default", s.defaultPartition)

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
			s.logger.Info("Reloading TLS...")
			if err := s.ReloadTLS(); err != nil {
				s.logger.Error("TLS reload failed", "err", err)
			}
		}
	}
}

type connState struct {
	partitionName string
	partition     *store.Store
	tx            *stonedb.Transaction
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		_ = conn.Close()
		atomic.AddInt64(&s.activeConns, -1)
		s.wg.Done()
		<-s.sem
	}()

	state := &connState{
		partitionName: s.defaultPartition,
		partition:     s.stores[s.defaultPartition],
		tx:            nil,
	}

	// Ensure active transaction is cleaned up if connection drops
	defer func() {
		if state.tx != nil {
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
			return
		}

		opCode := header[0]
		payloadLen := binary.BigEndian.Uint32(header[1:])
		if payloadLen > protocol.MaxCommandSize {
			_ = s.writeBinaryResponse(conn, protocol.ResStatusEntityTooLarge, nil)
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}

		_ = conn.SetWriteDeadline(time.Now().Add(protocol.DefaultWriteTimeout))

		if s.dispatchCommand(conn, r, opCode, payload, state) {
			return
		}
	}
}

func (s *Server) dispatchCommand(conn net.Conn, r io.Reader, opCode uint8, payload []byte, st *connState) bool {
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
	case protocol.OpCodeStat:
		s.handleStat(conn, st)
	case protocol.OpCodeReplicaOf:
		s.handleReplicaOf(conn, payload, st)
	case protocol.OpCodeReplHello:
		// Hand off control to specialized handler
		s.HandleReplicaConnection(conn, r, payload)
		return true // Connection takeover
	default:
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Unknown OpCode"))
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

func (s *Server) handleSelect(w io.Writer, payload []byte, st *connState) {
	name := string(payload)
	if partition, ok := s.stores[name]; ok {
		st.partitionName = name
		st.partition = partition
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Partition not found"))
	}
}

func (s *Server) handleBegin(w io.Writer, st *connState) {
	if st.partition == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Partition selected"))
		return
	}
	if st.tx != nil {
		_ = s.writeBinaryResponse(w, protocol.ResTxInProgress, nil)
		return
	}
	// Start a read-write transaction
	st.tx = st.partition.NewTransaction(true)
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
}

func (s *Server) handleCommit(w io.Writer, st *connState) {
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}
	err := st.tx.Commit()
	// Commit invalidates the transaction object, set to nil
	st.tx = nil

	if err != nil {
		if err == stonedb.ErrWriteConflict {
			_ = s.writeBinaryResponse(w, protocol.ResStatusTxConflict, []byte(err.Error()))
		} else {
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
	if st.partition == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Partition selected"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	key := string(payload)
	if !isASCII(key) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
		return
	}

	val, err := st.tx.Get([]byte(key))
	if err == stonedb.ErrKeyNotFound {
		_ = s.writeBinaryResponse(w, protocol.ResStatusNotFound, nil)
	} else if err != nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, val)
	}
}

func (s *Server) handleSet(w io.Writer, payload []byte, st *connState) {
	if st.partition == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Partition selected"))
		return
	}
	if st.partitionName == "0" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Partition 0 is read-only"))
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

	if !isASCII(string(key)) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
		return
	}

	if err := st.tx.Put(key, val); err != nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	}
}

func (s *Server) handleDel(w io.Writer, payload []byte, st *connState) {
	if st.partition == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Partition selected"))
		return
	}
	if st.partitionName == "0" {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Partition 0 is read-only"))
		return
	}
	if st.tx == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusTxRequired, nil)
		return
	}

	key := string(payload)
	if !isASCII(key) {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Key must be ASCII"))
		return
	}

	if err := st.tx.Delete([]byte(key)); err != nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte(err.Error()))
	} else {
		_ = s.writeBinaryResponse(w, protocol.ResStatusOK, nil)
	}
}

func (s *Server) handleStat(w io.Writer, st *connState) {
	if st.partition == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("No Partition selected"))
		return
	}
	stats := st.partition.Stats()
	msg := fmt.Sprintf("[%s] Keys:%d Uptime:%s", st.partitionName, stats.KeyCount, stats.Uptime)
	_ = s.writeBinaryResponse(w, protocol.ResStatusOK, []byte(msg))
}

func (s *Server) handleReplicaOf(w io.Writer, payload []byte, st *connState) {
	if s.replManager == nil {
		_ = s.writeBinaryResponse(w, protocol.ResStatusErr, []byte("Replication disabled on server"))
		return
	}
	// Decode: [AddrLen][Addr][RemotePartitionName]
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
	remotePart := string(payload[4+addrLen:])

	if addr == "" && addrLen == 0 {
		// Stop replication
		s.replManager.StopReplication(st.partitionName)
	} else {
		// Start replication
		s.replManager.AddReplica(st.partitionName, addr, remotePart)
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
