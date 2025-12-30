package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

var ReplicaSendTimeout = 5 * time.Second

// --- LEADER SIDE (Multiplexer) ---

// HandleReplicaConnection multiplexes streams from multiple databases onto one connection.
func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte) {
	// 1. Parse Hello: [Ver:4][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	if len(payload) < 8 {
		s.logger.Error("HandleReplicaConnection: payload too short for header")
		return
	}
	count := binary.BigEndian.Uint32(payload[4:8])

	cursor := 8
	type subReq struct {
		name  string
		logID uint64
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
			s.logger.Error("HandleReplicaConnection: payload truncated reading name/logID", "replica", replicaID)
			return
		}
		name := string(payload[cursor : cursor+nLen])
		cursor += nLen
		logID := binary.BigEndian.Uint64(payload[cursor : cursor+8])
		cursor += 8
		subs = append(subs, subReq{name, logID})

		if st, ok := s.stores[name]; ok {
			s.logger.Info("Replica subscribed", "replica", replicaID, "db", name, "startLogID", logID)
			st.RegisterReplica(replicaID, logID)
			defer st.UnregisterReplica(replicaID)
		} else {
			s.logger.Warn("Replica requested unknown db", "replica", replicaID, "db", name)
		}
	}

	// 2. Start Multiplexer
	outCh := make(chan replPacket, 10)
	errCh := make(chan error, 1) // Signal channel for slow/failed streams
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup

	// Start reader for each requested DB
	for _, req := range subs {
		store, ok := s.stores[req.name]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(name string, st *Store, startLogID uint64) {
			defer wg.Done()
			if err := s.streamDB(name, st, startLogID, outCh, done); err != nil {
				s.logger.Error("StreamDB failed", "db", name, "err", err)
				// Non-blocking send to avoid hanging if main loop is gone
				select {
				case errCh <- err:
				default:
				}
			}
		}(req.name, store, req.logID)
	}

	// ACK Reader
	go func() {
		// Just consume ACKs to keep connection alive and update offsets
		// Format: [OpCode][Len][DBNameLen][DBName][LogID]
		h := make([]byte, 5)
		for {
			if _, err := io.ReadFull(r, h); err != nil {
				if err != io.EOF {
					s.logger.Error("Replica ACK reader failed", "replica", replicaID, "err", err)
				}
				return
			}
			if h[0] == OpCodeReplAck {
				ln := binary.BigEndian.Uint32(h[1:])
				b := make([]byte, ln)
				if _, err := io.ReadFull(r, b); err != nil {
					s.logger.Error("Replica ACK body read failed", "replica", replicaID, "err", err)
					return
				}

				if len(b) > 4 {
					nL := binary.BigEndian.Uint32(b[:4])
					if len(b) >= 4+int(nL)+8 {
						dbName := string(b[4 : 4+nL])
						logID := binary.BigEndian.Uint64(b[4+nL:])
						if st, ok := s.stores[dbName]; ok {
							s.logger.Debug("Received ACK", "replica", replicaID, "db", dbName, "logID", logID)
							st.UpdateReplicaLogID(replicaID, logID)
						}
					}
				}
			}
		}
	}()

	// Central Writer
	for {
		select {
		case err := <-errCh:
			s.logger.Warn("Dropping slow/failed replica", "addr", replicaID, "err", err)
			return

		case p := <-outCh:
			s.logger.Debug("Sending batch to replica", "replica", replicaID, "db", p.dbName, "count", p.count, "bytes", len(p.data))
			// Frame: [OpCodeReplBatch][TotalLen] [DBNameLen][DBName][Count][BatchData...]
			totalLen := 4 + len(p.dbName) + 4 + len(p.data)
			header := make([]byte, 5)
			header[0] = OpCodeReplBatch
			binary.BigEndian.PutUint32(header[1:], uint32(totalLen))

			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(header); err != nil {
				s.logger.Error("Failed to write batch header", "replica", replicaID, "err", err)
				return
			}

			nameH := make([]byte, 4)
			binary.BigEndian.PutUint32(nameH, uint32(len(p.dbName)))
			if _, err := conn.Write(nameH); err != nil {
				s.logger.Error("Failed to write batch dbname len", "replica", replicaID, "err", err)
				return
			}
			if _, err := conn.Write([]byte(p.dbName)); err != nil {
				s.logger.Error("Failed to write batch dbname", "replica", replicaID, "err", err)
				return
			}

			cntH := make([]byte, 4)
			binary.BigEndian.PutUint32(cntH, p.count)
			if _, err := conn.Write(cntH); err != nil {
				s.logger.Error("Failed to write batch count", "replica", replicaID, "err", err)
				return
			}

			if _, err := conn.Write(p.data); err != nil {
				s.logger.Error("Failed to write batch data", "replica", replicaID, "err", err)
				return
			}
		}
	}
}

func (s *Server) streamDB(name string, store *Store, minLogID uint64, outCh chan<- replPacket, done <-chan struct{}) error {
	startOffset := store.FindOffsetForLogID(minLogID)
	s.logger.Info("StreamDB started", "db", name, "minLogID", minLogID, "startOffset", startOffset)
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
				// Success
			case <-time.After(ReplicaSendTimeout):
				// Timeout - Consumer too slow
				return fmt.Errorf("replication send timeout for db %s", name)
			case <-done:
				// Connection closed
				return nil
			}
			batchBuf.Reset()
			count = 0
		}
		return nil
	}

	for {
		select {
		case <-done:
			return nil
		default:
		}

		walSize := store.wal.Size()
		if curr >= walSize {
			if err := flush(); err != nil {
				return err
			}
			store.wal.Wait(curr)
			walSize = store.wal.Size()
		}

		readLen := int64(len(buf))
		if curr+readLen > walSize {
			readLen = walSize - curr
		}

		n, err := store.wal.ReadAt(buf[:readLen], curr)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			continue
		}

		parseOff := 0
		for parseOff < n {
			if parseOff+HeaderSize > n {
				break
			}
			// Skip Holes
			if buf[parseOff] == 0 && buf[parseOff+1] == 0 && buf[parseOff+2] == 0 {
				aligned := (parseOff/4096 + 1) * 4096
				if aligned > n {
					break
				}
				parseOff = aligned
				continue
			}

			packed := binary.BigEndian.Uint32(buf[parseOff : parseOff+4])
			keyLen, valLen, isDel := UnpackMeta(packed)
			lsn := binary.BigEndian.Uint64(buf[parseOff+4 : parseOff+12])
			logID := binary.BigEndian.Uint64(buf[parseOff+12 : parseOff+20])

			payloadLen := int(keyLen)
			if !isDel {
				payloadLen += int(valLen)
			}

			if parseOff+HeaderSize+payloadLen > n {
				break
			}

			if logID > minLogID {
				// Reconstruct Wire Format: [LogID][LSN][Op][KLen][Key][VLen][Val]
				binary.BigEndian.PutUint64(scratch, logID)
				batchBuf.Write(scratch)
				binary.BigEndian.PutUint64(scratch, lsn)
				batchBuf.Write(scratch)

				op := byte(OpJournalSet)
				if isDel {
					op = OpJournalDelete
				}
				batchBuf.WriteByte(op)

				binary.BigEndian.PutUint32(scratch[:4], keyLen)
				batchBuf.Write(scratch[:4])
				kStart := parseOff + HeaderSize
				batchBuf.Write(buf[kStart : kStart+int(keyLen)])

				binary.BigEndian.PutUint32(scratch[:4], valLen)
				batchBuf.Write(scratch[:4])
				if !isDel {
					batchBuf.Write(buf[kStart+int(keyLen) : kStart+payloadLen])
				}
				count++
				if batchBuf.Len() >= 64*1024 {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			parseOff += HeaderSize + payloadLen
		}
		curr += int64(parseOff)
	}
}

// --- FOLLOWER SIDE (Manager) ---

type ReplicaSource struct {
	LocalDB  string
	RemoteDB string
}

// ReplicationManager handles shared connections to upstream servers.
type ReplicationManager struct {
	mu         sync.Mutex
	peers      map[string][]ReplicaSource // Address -> []ReplicaSource
	cancelFunc map[string]context.CancelFunc
	stores     map[string]*Store
	tlsConf    *tls.Config
	logger     *slog.Logger
}

func NewReplicationManager(stores map[string]*Store, tlsConf *tls.Config, logger *slog.Logger) *ReplicationManager {
	return &ReplicationManager{
		peers:      make(map[string][]ReplicaSource),
		cancelFunc: make(map[string]context.CancelFunc),
		stores:     stores,
		tlsConf:    tlsConf,
		logger:     logger,
	}
}

func (rm *ReplicationManager) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for addr := range rm.peers {
		rm.spawnConnection(addr)
	}
}

func (rm *ReplicationManager) AddReplica(dbName, sourceAddr, sourceDB string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if this DB is already replicating from this source
	dbs, _ := rm.peers[sourceAddr]
	found := false
	for _, db := range dbs {
		if db.LocalDB == dbName {
			found = true
			break
		}
	}

	if !found {
		rm.peers[sourceAddr] = append(rm.peers[sourceAddr], ReplicaSource{LocalDB: dbName, RemoteDB: sourceDB})
		// We need to restart the connection to update the subscription list.
		if cancel, exists := rm.cancelFunc[sourceAddr]; exists {
			cancel() // Stop existing loop
		}
		rm.spawnConnection(sourceAddr)
		rm.logger.Info("Added replica source", "db", dbName, "source", sourceAddr, "remote_db", sourceDB)
	}
}

func (rm *ReplicationManager) StopReplication(dbName string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Iterate through all peers to find where this DB is being replicated from
	for addr, dbs := range rm.peers {
		newDBs := make([]ReplicaSource, 0, len(dbs))
		changed := false
		for _, db := range dbs {
			if db.LocalDB != dbName {
				newDBs = append(newDBs, db)
			} else {
				changed = true
			}
		}

		if changed {
			rm.peers[addr] = newDBs
			rm.logger.Info("Stopped replication", "db", dbName, "source", addr)

			// Kill existing connection
			if cancel, exists := rm.cancelFunc[addr]; exists {
				cancel()
			}

			// If other DBs are still replicating, restart the loop with new list
			if len(newDBs) > 0 {
				rm.spawnConnection(addr)
			} else {
				// No more DBs for this address, clean up
				delete(rm.peers, addr)
				delete(rm.cancelFunc, addr)
			}
			// A DB can only have one source, so we are done
			return
		}
	}
}

// spawnConnection starts the loop for a specific address. Caller must hold lock.
func (rm *ReplicationManager) spawnConnection(addr string) {
	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelFunc[addr] = cancel

	go rm.maintainConnection(ctx, addr)
}

func (rm *ReplicationManager) maintainConnection(ctx context.Context, addr string) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Grab current DB list for this address
		rm.mu.Lock()
		if _, ok := rm.peers[addr]; !ok {
			rm.mu.Unlock()
			return
		}
		dbs := make([]ReplicaSource, len(rm.peers[addr]))
		copy(dbs, rm.peers[addr])
		rm.mu.Unlock()

		if len(dbs) == 0 {
			return
		}

		if err := rm.connectAndSync(ctx, addr, dbs); err != nil {
			if ctx.Err() != nil {
				return
			}
			rm.logger.Error("Replication sync failed", "peer", addr, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (rm *ReplicationManager) connectAndSync(ctx context.Context, addr string, dbs []ReplicaSource) error {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", addr, rm.tlsConf)
	if err != nil {
		return err
	}
	// Ensure connection closes when context is cancelled or function returns
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	defer conn.Close()

	rm.logger.Info("Connected to Leader for replication", "addr", addr, "count", len(dbs))

	// Map RemoteDB -> []LocalDB
	remoteToLocal := make(map[string][]string)

	// Build Hello Payload
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(dbs)))

	for _, cfg := range dbs {
		stats := rm.stores[cfg.LocalDB].Stats()
		logID := stats.NextLogID - 1
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		binary.Write(buf, binary.BigEndian, logID)
		rm.logger.Debug("Sending Hello for DB", "remote_db", cfg.RemoteDB, "local_db", cfg.LocalDB, "startLogID", logID)

		remoteToLocal[cfg.RemoteDB] = append(remoteToLocal[cfg.RemoteDB], cfg.LocalDB)
	}

	// Send Hello
	header := make([]byte, 5)
	header[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return err
	}

	// Loop
	respHead := make([]byte, 5)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := io.ReadFull(conn, respHead); err != nil {
			return err
		}
		if respHead[0] == OpCodeReplBatch {
			ln := binary.BigEndian.Uint32(respHead[1:])
			payload := make([]byte, ln)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return err
			}

			// Decode: [DBNameLen][DBName][Count][Data]
			cursor := 0
			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed batch: no db len")
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			if cursor+nLen+4 > len(payload) {
				return fmt.Errorf("malformed batch: no db name or count")
			}
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]

			localDBNames, ok := remoteToLocal[remoteDBName]
			if !ok {
				rm.logger.Warn("Received batch for unknown remote db", "remote_db", remoteDBName)
				continue
			}

			for _, localDBName := range localDBNames {
				if st, ok := rm.stores[localDBName]; ok {
					if maxID, err := applyReplicationBatch(st, count, data); err == nil {
						// Send ACK with REMOTE DB Name, so leader knows which stream to update
						ackBuf := new(bytes.Buffer)
						binary.Write(ackBuf, binary.BigEndian, uint32(len(remoteDBName)))
						ackBuf.WriteString(remoteDBName)
						binary.Write(ackBuf, binary.BigEndian, maxID)

						h := make([]byte, 5)
						h[0] = OpCodeReplAck
						binary.BigEndian.PutUint32(h[1:], uint32(ackBuf.Len()))
						if _, err := conn.Write(h); err != nil {
							return err
						}
						if _, err := conn.Write(ackBuf.Bytes()); err != nil {
							return err
						}
					} else {
						rm.logger.Error("Failed to apply replication batch", "local_db", localDBName, "err", err)
					}
				}
			}
		} else {
			rm.logger.Warn("Unexpected OpCode in replication stream", "op", respHead[0])
		}
	}
}

func applyReplicationBatch(store *Store, count uint32, data []byte) (uint64, error) {
	cursor := 0
	var ops []LogEntry
	var maxID uint64

	for i := 0; i < int(count); i++ {
		if cursor+17 > len(data) {
			return 0, fmt.Errorf("malformed batch entry header")
		}
		lid := binary.BigEndian.Uint64(data[cursor : cursor+8])
		lsn := binary.BigEndian.Uint64(data[cursor+8 : cursor+16])
		op := data[cursor+16]
		cursor += 17

		if cursor+4 > len(data) {
			return 0, fmt.Errorf("malformed batch entry key len")
		}
		kLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4

		if cursor+kLen > len(data) {
			return 0, fmt.Errorf("malformed batch entry key")
		}
		key := data[cursor : cursor+kLen]
		cursor += kLen

		if cursor+4 > len(data) {
			return 0, fmt.Errorf("malformed batch entry val len")
		}
		vLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4

		var val []byte
		if op != OpJournalDelete {
			if cursor+vLen > len(data) {
				return 0, fmt.Errorf("malformed batch entry val")
			}
			val = data[cursor : cursor+vLen]
			cursor += vLen
		}
		ops = append(ops, LogEntry{LogID: lid, LSN: lsn, OpType: op, Key: key, Value: val})
		if lid > maxID {
			maxID = lid
		}
	}

	return maxID, store.ReplicateBatch(ops)
}
