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
	LocalDB  string
	RemoteDB string
}

type ReplicationManager struct {
	mu         sync.Mutex
	serverID   string
	peers      map[string][]ReplicaSource
	cancelFunc map[string]context.CancelFunc
	stores     map[string]*store.Store
	tlsConf    *tls.Config
	logger     *slog.Logger
}

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

func (rm *ReplicationManager) AddReplica(dbName, sourceAddr, sourceDB string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
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
		if cancel, exists := rm.cancelFunc[sourceAddr]; exists {
			cancel()
		}
		rm.spawnConnection(sourceAddr)
		rm.logger.Info("Added replica source", "db", dbName, "source", sourceAddr, "remote_db", sourceDB)
	}
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
	// Create a logger with context for this peer connection
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
			peerLogger.Error("Replication sync failed, retrying in 3s", "err", err)
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
		stats := rm.stores[cfg.LocalDB].Stats()
		logID := uint64(stats.Offset)
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

		// --- Handle Safe Point Propagation ---
		// Payload: [DBNameLen][DBName][Reserved(4)][SafeSeq(8)]
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
							// Debug log only for safe points to avoid noise
							// logger.Debug("Received safe point", "db", localDB, "seq", safeSeq)
							st.SetLeaderSafeSeq(safeSeq)
						}
					}
				}
			}
			continue
		}

		// --- Handle Full Snapshot ---
		// Payload: [DBNameLen][DBName][Count][Data...]
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
						// Apply directly
						st.ReplicateBatch(entries)
					}
				}
			}
			continue
		}

		// --- Handle Snapshot Done Signal ---
		// Payload: [DBNameLen][DBName][0][ResumeSeq(8)]
		if opCode == protocol.OpCodeReplSnapshotDone {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			cursor += 4 // Skip Count(0)

			if cursor+8 <= len(payload) {
				resumeSeq := binary.BigEndian.Uint64(payload[cursor:])
				logger.Info("Snapshot finished, switching to WAL streaming", "remote_db", remoteDBName, "resume_seq", resumeSeq)
			}
			// Mark snapshot as complete for this DB so future snapshots trigger reset again
			snapshotStarted[remoteDBName] = false
			continue
		}

		// --- Handle Standard WAL Batch ---
		// Payload: [DBNameLen][DBName][Count][Data...]
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

						// Send ACK if successful
						if lastID > 0 {
							ackBuf := new(bytes.Buffer)
							binary.Write(ackBuf, binary.BigEndian, uint32(len(remoteDBName)))
							ackBuf.WriteString(remoteDBName)
							binary.Write(ackBuf, binary.BigEndian, lastID)

							h := make([]byte, 5)
							h[0] = protocol.OpCodeReplAck
							binary.BigEndian.PutUint32(h[1:], uint32(ackBuf.Len()))

							// No Deadline for ACK to prevent self-disconnection on slow networks
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

// processReplicationPacket parses raw bytes into LogEntries, buffers them, and applies them.
// It handles fragmentation by accumulating entries in 'buffer' until a Commit marker is found.
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

		// Handle Commit Marker
		if op == protocol.OpJournalCommit {
			if len(buffer) > 0 {
				// We have a complete transaction in buffer
				pendingBatches = append(pendingBatches, buffer)
				lastCommittedID = lid

				// Reset buffer for next transaction (CRITICAL: ensure new slice allocation)
				buffer = nil
				currentSize = 0
			} else {
				// Empty commit (rare but valid)
				lastCommittedID = lid
			}
		} else {
			// Check Size Limit
			entrySize := uint64(len(key) + len(val) + 20) // overhead estimation
			if currentSize+entrySize > MaxTransactionBufferSize {
				return buffer, 0, currentSize, fmt.Errorf("transaction too large: %d > %d", currentSize+entrySize, MaxTransactionBufferSize)
			}
			currentSize += entrySize

			// Buffer the op until we see a commit
			buffer = append(buffer, protocol.LogEntry{
				LogSeq: lid,
				OpCode: op,
				Key:    key,
				Value:  val,
			})
		}
	}

	// Apply all fully accumulated transactions
	if len(pendingBatches) > 0 {
		if err := store.ReplicateBatches(pendingBatches); err != nil {
			return buffer, lastCommittedID, currentSize, err
		}
	}

	// Return remaining buffer (incomplete transaction) and last commit ID
	return buffer, lastCommittedID, currentSize, nil
}
