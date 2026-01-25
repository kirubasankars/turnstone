package replication

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

	"turnstone/protocol"
	"turnstone/store"
)

var (
	// MaxTransactionBufferSize limits the size of a single transaction buffered in memory
	// to prevent OOM attacks or leaks from malicious/broken leaders.
	MaxTransactionBufferSize = uint64(64 * 1024 * 1024) // 64MB
)

type ReplicaSource struct {
	LocalDB  string `json:"local_db"`
	RemoteDB string `json:"remote_db"`
}

type ReplicationManager struct {
	mu         sync.Mutex
	serverID   string
	peers      map[string][]ReplicaSource // Addr -> List of DBs
	cancelFunc map[string]context.CancelFunc
	stores     map[string]*store.Store
	tlsConf    *tls.Config
	logger     *slog.Logger
}

// NewReplicationManager creates a manager for outgoing replication connections.
// Persistence is intentionally disabled; replication must be configured at runtime.
func NewReplicationManager(serverID string, stores map[string]*store.Store, tlsConf *tls.Config, logger *slog.Logger) *ReplicationManager {
	return &ReplicationManager{
		serverID:   serverID,
		peers:      make(map[string][]ReplicaSource),
		cancelFunc: make(map[string]context.CancelFunc),
		stores:     stores,
		tlsConf:    tlsConf,
		logger:     logger,
	}
}

// IsReplicating checks if the specific database is currently configured to replicate
// from an upstream source. Used to prevent cascading replication.
func (rm *ReplicationManager) IsReplicating(dbName string) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, dbs := range rm.peers {
		for _, db := range dbs {
			if db.LocalDB == dbName {
				return true
			}
		}
	}
	return false
}

func (rm *ReplicationManager) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for addr := range rm.peers {
		rm.spawnConnection(addr)
	}
}

func (rm *ReplicationManager) AddReplica(dbName, sourceAddr, sourceDB string) error {
	rm.mu.Lock()

	// Check existing
	dbs, _ := rm.peers[sourceAddr]
	for _, db := range dbs {
		if db.LocalDB == dbName {
			rm.mu.Unlock()
			return nil // Already added
		}
	}

	// Create candidate configuration
	candidateDBs := make([]ReplicaSource, len(dbs), len(dbs)+1)
	copy(candidateDBs, dbs)
	candidateDBs = append(candidateDBs, ReplicaSource{LocalDB: dbName, RemoteDB: sourceDB})

	// Release lock BEFORE network call to prevent deadlock
	rm.mu.Unlock()

	// Verify Handshake Synchronously (Network Call)
	// Handshake timeout is 10s.
	if err := rm.verifyHandshake(sourceAddr, candidateDBs); err != nil {
		return err
	}

	// Re-acquire lock to apply changes
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Re-fetch current state in case it changed while we were verifying
	currentDBs, _ := rm.peers[sourceAddr]
	for _, db := range currentDBs {
		if db.LocalDB == dbName {
			return nil // Someone else added it
		}
	}

	// Append to the *current* authoritative list
	finalDBs := append(currentDBs, ReplicaSource{LocalDB: dbName, RemoteDB: sourceDB})
	rm.peers[sourceAddr] = finalDBs

	if cancel, exists := rm.cancelFunc[sourceAddr]; exists {
		cancel()
	}
	rm.spawnConnection(sourceAddr)
	rm.logger.Info("Added replica source", "db", dbName, "source", sourceAddr, "remote_db", sourceDB)
	return nil
}

// verifyHandshake connects to the remote, sends Hello, and checks the response status.
func (rm *ReplicationManager) verifyHandshake(addr string, dbs []ReplicaSource) error {
	// Increased timeout to 10s to prevent flaky "i/o timeout" errors under load
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", addr, rm.tlsConf)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	// Construct Hello Payload
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version
	binary.Write(buf, binary.BigEndian, uint32(len(rm.serverID)))
	buf.WriteString(rm.serverID)
	binary.Write(buf, binary.BigEndian, uint32(len(dbs)))

	for _, cfg := range dbs {
		var logID uint64
		if st, ok := rm.stores[cfg.LocalDB]; ok {
			logID = st.LastOpID()
		}
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		binary.Write(buf, binary.BigEndian, logID)
	}

	// Send Header
	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return err
	}

	// Read Response Header
	respHead := make([]byte, 5)
	if _, err := io.ReadFull(conn, respHead); err != nil {
		return err
	}

	opCode := respHead[0]
	length := binary.BigEndian.Uint32(respHead[1:])

	// If Error, read body and return it
	if opCode == protocol.ResStatusErr {
		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}
		return fmt.Errorf("upstream rejected handshake: %s", string(payload))
	}

	return nil
}

func (rm *ReplicationManager) StopReplication(dbName string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

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
			rm.logger.Info("Stopped replication source", "db", dbName, "source", addr)
			if cancel, exists := rm.cancelFunc[addr]; exists {
				cancel()
			}
			if len(newDBs) > 0 {
				rm.spawnConnection(addr)
			} else {
				delete(rm.peers, addr)
				delete(rm.cancelFunc, addr)
			}
			return
		}
	}
}

func (rm *ReplicationManager) spawnConnection(addr string) {
	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelFunc[addr] = cancel
	go rm.maintainConnection(ctx, addr)
}

func (rm *ReplicationManager) maintainConnection(ctx context.Context, addr string) {
	peerLogger := rm.logger.With("peer_addr", addr)

	for {
		if ctx.Err() != nil {
			return
		}
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

		peerLogger.Debug("Attempting replication connection")

		if err := rm.connectAndSync(ctx, addr, dbs, peerLogger); err != nil {
			if ctx.Err() != nil {
				return
			}
			// WARN: Failure to connect/sync
			peerLogger.Warn("Replication sync failed, retrying in 3s", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (rm *ReplicationManager) connectAndSync(ctx context.Context, addr string, dbs []ReplicaSource, logger *slog.Logger) error {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", addr, rm.tlsConf)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	defer conn.Close()

	// INFO: Successfully connected to upstream
	logger.Info("Connected to Leader", "db_count", len(dbs))

	remoteToLocal := make(map[string][]string)
	txBuffers := make(map[string][]protocol.LogEntry)
	txBufferSizes := make(map[string]uint64)
	snapshotStarted := make(map[string]bool)

	// Handshake
	// Format: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(rm.serverID)))
	buf.WriteString(rm.serverID)
	binary.Write(buf, binary.BigEndian, uint32(len(dbs)))

	for _, cfg := range dbs {
		var logID uint64
		if st, ok := rm.stores[cfg.LocalDB]; ok {
			logID = st.LastOpID()
		}
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		binary.Write(buf, binary.BigEndian, logID)

		logger.Debug("Sending Hello for DB", "remote_db", cfg.RemoteDB, "local_db", cfg.LocalDB, "start_log_id", logID)

		remoteToLocal[cfg.RemoteDB] = append(remoteToLocal[cfg.RemoteDB], cfg.LocalDB)
		txBuffers[cfg.LocalDB] = make([]protocol.LogEntry, 0)
		txBufferSizes[cfg.LocalDB] = 0
	}

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return err
	}

	respHead := make([]byte, 5)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := io.ReadFull(conn, respHead); err != nil {
			return err
		}

		opCode := respHead[0]
		length := binary.BigEndian.Uint32(respHead[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}

		// Handle explicit error from server during streaming
		if opCode == protocol.ResStatusErr {
			return fmt.Errorf("remote error during stream: %s", string(payload))
		}

		// --- Handle Safe Point Propagation ---
		if opCode == protocol.OpCodeReplSafePoint {
			cursor := 0
			if cursor+4 > len(payload) {
				continue
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			if cursor+nLen > len(payload) {
				continue
			}
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			cursor += 4 // Skip Count/Reserved

			if cursor+8 <= len(payload) {
				safeSeq := binary.BigEndian.Uint64(payload[cursor:])

				if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
					for _, localDB := range localDBNames {
						if st, ok := rm.stores[localDB]; ok {
							st.SetLeaderSafeSeq(safeSeq)
						}
					}
				}
			}
			continue
		}

		// --- Handle Timeline Update ---
		if opCode == protocol.OpCodeReplTimeline {
			cursor := 0
			if cursor+4 > len(payload) {
				continue
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			nEnd := cursor + nLen
			if nEnd > len(payload) {
				continue
			}
			remoteDBName := string(payload[cursor:nEnd])
			cursor = nEnd
			cursor += 4 // Skip Count/Reserved

			if cursor+8 <= len(payload) {
				tli := binary.BigEndian.Uint64(payload[cursor:])
				if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
					for _, localDB := range localDBNames {
						if st, ok := rm.stores[localDB]; ok {
							if err := st.SetTimeline(tli); err != nil {
								logger.Warn("Failed to set timeline", "db", localDB, "tli", tli, "err", err)
							} else {
								logger.Info("Updated timeline from leader", "db", localDB, "tli", tli)
							}
						}
					}
				}
			}
			continue
		}

		// --- Handle Full Snapshot ---
		if opCode == protocol.OpCodeReplSnapshot {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]

			if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
				// Detect start of a new snapshot sequence and RESET local state
				if !snapshotStarted[remoteDBName] {
					logger.Info("Snapshot detected, resetting local databases", "remote_db", remoteDBName)
					for _, localDBName := range localDBNames {
						if st, ok := rm.stores[localDBName]; ok {
							if err := st.Reset(); err != nil {
								logger.Error("Failed to reset database", "db", localDBName, "err", err)
								return err
							}
						}
					}
					snapshotStarted[remoteDBName] = true
				}

				for _, localDBName := range localDBNames {
					if st, ok := rm.stores[localDBName]; ok {
						logger.Info("Applying snapshot batch", "db", localDBName, "count", count, "bytes", len(data))
						dCursor := 0
						var entries []protocol.LogEntry
						for i := 0; i < int(count); i++ {
							kLen := int(binary.BigEndian.Uint32(data[dCursor:]))
							dCursor += 4
							key := data[dCursor : dCursor+kLen]
							dCursor += kLen
							vLen := int(binary.BigEndian.Uint32(data[dCursor:]))
							dCursor += 4
							val := data[dCursor : dCursor+vLen]
							dCursor += vLen

							entries = append(entries, protocol.LogEntry{
								Key: key, Value: val, OpCode: protocol.OpCodeSet,
							})
						}
						st.ReplicateBatch(entries)
					}
				}
			}
			continue
		}

		// --- Handle Snapshot Done Signal ---
		if opCode == protocol.OpCodeReplSnapshotDone {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			cursor += 4 // Skip Count(0)

			if cursor+16 <= len(payload) {
				txID := binary.BigEndian.Uint64(payload[cursor:])
				opID := binary.BigEndian.Uint64(payload[cursor+8:])
				logger.Info("Snapshot finished, syncing clocks", "remote_db", remoteDBName, "tx_id", txID, "resume_seq", opID)

				if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
					for _, localDB := range localDBNames {
						if st, ok := rm.stores[localDB]; ok {
							st.DB.ForceSetClocks(txID, opID)
						}
					}
				}
			} else {
				logger.Warn("Snapshot done signal received with insufficient payload", "len", len(payload))
			}
			snapshotStarted[remoteDBName] = false
			continue
		}

		// --- Handle Standard WAL Batch ---
		if opCode == protocol.OpCodeReplBatch {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]

			if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
				for _, localDBName := range localDBNames {
					if st, ok := rm.stores[localDBName]; ok {
						buffer := txBuffers[localDBName]
						sze := txBufferSizes[localDBName]
						newBuffer, lastID, newSze, err := processReplicationPacket(st, buffer, sze, count, data)
						if err != nil {
							logger.Error("Failed to process WAL batch", "db", localDBName, "err", err)
							return err
						}
						txBuffers[localDBName] = newBuffer
						txBufferSizes[localDBName] = newSze

						if lastID > 0 {
							ackBuf := new(bytes.Buffer)
							binary.Write(ackBuf, binary.BigEndian, uint32(len(remoteDBName)))
							ackBuf.WriteString(remoteDBName)
							binary.Write(ackBuf, binary.BigEndian, lastID)

							h := make([]byte, 5)
							h[0] = protocol.OpCodeReplAck
							binary.BigEndian.PutUint32(h[1:], uint32(ackBuf.Len()))

							if _, err := conn.Write(h); err != nil {
								return err
							}
							if _, err := conn.Write(ackBuf.Bytes()); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
}

func processReplicationPacket(store *store.Store, buffer []protocol.LogEntry, currentSize uint64, count uint32, data []byte) ([]protocol.LogEntry, uint64, uint64, error) {
	cursor := 0
	var lastCommittedID uint64
	var pendingBatches [][]protocol.LogEntry

	for i := 0; i < int(count); i++ {
		// Entry: [LogID(8)][TxID(8)][Op(1)][KLen(4)][Key][VLen(4)][Val]
		if cursor+17 > len(data) {
			return buffer, 0, currentSize, fmt.Errorf("malformed batch entry header")
		}
		lid := binary.BigEndian.Uint64(data[cursor : cursor+8])
		op := data[cursor+16]
		cursor += 17

		kLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if cursor+kLen > len(data) {
			return buffer, 0, currentSize, fmt.Errorf("malformed batch entry key")
		}
		key := data[cursor : cursor+kLen]
		cursor += kLen

		vLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if cursor+vLen > len(data) {
			return buffer, 0, currentSize, fmt.Errorf("malformed batch entry val")
		}
		val := data[cursor : cursor+vLen]
		cursor += vLen

		if op == protocol.OpJournalCommit {
			if len(buffer) > 0 {
				pendingBatches = append(pendingBatches, buffer)
				lastCommittedID = lid
				buffer = nil
				currentSize = 0
			} else {
				lastCommittedID = lid
			}
		} else {
			entrySize := uint64(len(key) + len(val) + 20)
			if currentSize+entrySize > MaxTransactionBufferSize {
				return buffer, 0, currentSize, fmt.Errorf("transaction too large: %d > %d", currentSize+entrySize, MaxTransactionBufferSize)
			}
			currentSize += entrySize

			buffer = append(buffer, protocol.LogEntry{
				LogSeq: lid,
				OpCode: op,
				Key:    key,
				Value:  val,
			})
		}
	}

	if len(pendingBatches) > 0 {
		if err := store.ReplicateBatches(pendingBatches); err != nil {
			return buffer, lastCommittedID, currentSize, err
		}
	}

	return buffer, lastCommittedID, currentSize, nil
}
