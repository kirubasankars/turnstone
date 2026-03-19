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

type ReplicaSource struct {
	LocalDB  string
	RemoteDB string
}

// ReplicationManager handles shared connections to upstream servers.
type ReplicationManager struct {
	mu         sync.Mutex
	serverID   string
	peers      map[string][]ReplicaSource // Address -> []ReplicaSource
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
			rm.logger.Info("Stopped replication", "db", dbName, "source", addr)

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
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	rm.logger.Info("Connected to Leader for replication", "addr", addr, "count", len(dbs))

	remoteToLocal := make(map[string][]string)
	txBuffers := make(map[string][]protocol.LogEntry)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, uint32(1)) 
	_ = binary.Write(buf, binary.BigEndian, uint32(len(rm.serverID)))
	buf.WriteString(rm.serverID)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(dbs)))

	for _, cfg := range dbs {
		stats := rm.stores[cfg.LocalDB].Stats()
		logID := uint64(stats.Offset)
		_ = binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		_ = binary.Write(buf, binary.BigEndian, logID)
		rm.logger.Debug("Sending Hello for DB", "remote_db", cfg.RemoteDB, "local_db", cfg.LocalDB, "startLogID", logID)

		remoteToLocal[cfg.RemoteDB] = append(remoteToLocal[cfg.RemoteDB], cfg.LocalDB)
		txBuffers[cfg.LocalDB] = make([]protocol.LogEntry, 0)
	}

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil { return err }
	if _, err := conn.Write(buf.Bytes()); err != nil { return err }

	respHead := make([]byte, 5)
	for {
		if ctx.Err() != nil { return ctx.Err() }
		if _, err := io.ReadFull(conn, respHead); err != nil { return err }
		
		opCode := respHead[0]
		length := binary.BigEndian.Uint32(respHead[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil { return err }

		// --- HANDLE SNAPSHOT PACKETS ---
		if opCode == protocol.OpCodeReplSnapshot {
			// Parse: [DBNameLen][DBName][Count][Data...]
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			remoteDBName := string(payload[cursor : cursor+nLen])
			cursor += nLen
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]

			localDBNames, ok := remoteToLocal[remoteDBName]
			if !ok { continue }

			for _, localDBName := range localDBNames {
				if st, ok := rm.stores[localDBName]; ok {
					// Apply Snapshot Batch Directly
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
					// Write to VLog and Index immediately
					if err := st.ReplicateBatch(entries); err != nil {
						rm.logger.Error("Failed to apply snapshot batch", "db", localDBName, "err", err)
					}
				}
			}
			continue
		}

		// --- HANDLE SNAPSHOT DONE ---
		if opCode == protocol.OpCodeReplSnapshotDone {
			// Parse: [DBNameLen][DBName][0][LogID(8)]
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			remoteDBName := string(payload[cursor : cursor+nLen]) 
			cursor += nLen
			cursor += 4 // Skip Count(0)
			
			if cursor + 8 <= len(payload) {
				resumeSeq := binary.BigEndian.Uint64(payload[cursor:])
				rm.logger.Info("Snapshot finished, resuming WAL", "remote", remoteDBName, "resume_seq", resumeSeq)
			}
			continue
		}

		// --- HANDLE STANDARD WAL BATCH ---
		if opCode == protocol.OpCodeReplBatch {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
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
					buffer := txBuffers[localDBName]
					newBuffer, lastID, err := processReplicationPacket(st, buffer, count, data)
					txBuffers[localDBName] = newBuffer
					
					if err == nil && lastID > 0 {
						// Send ACK
						ackBuf := new(bytes.Buffer)
						binary.Write(ackBuf, binary.BigEndian, uint32(len(remoteDBName)))
						ackBuf.WriteString(remoteDBName)
						binary.Write(ackBuf, binary.BigEndian, lastID)
						
						h := make([]byte, 5)
						h[0] = protocol.OpCodeReplAck
						binary.BigEndian.PutUint32(h[1:], uint32(ackBuf.Len()))
						conn.Write(h)
						conn.Write(ackBuf.Bytes())
					}
				}
			}
		}
	}
}

// processReplicationPacket parses raw bytes into LogEntries, buffers them, and applies upon Commit.
func processReplicationPacket(store *store.Store, buffer []protocol.LogEntry, count uint32, data []byte) ([]protocol.LogEntry, uint64, error) {
	cursor := 0
	var lastCommittedID uint64

	for i := 0; i < int(count); i++ {
		if cursor+17 > len(data) {
			return buffer, 0, fmt.Errorf("malformed batch entry header")
		}
		lid := binary.BigEndian.Uint64(data[cursor : cursor+8])
		op := data[cursor+16]
		cursor += 17

		kLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if cursor+kLen > len(data) {
			return buffer, 0, fmt.Errorf("malformed batch entry key")
		}
		key := data[cursor : cursor+kLen]
		cursor += kLen

		vLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if cursor+vLen > len(data) {
			return buffer, 0, fmt.Errorf("malformed batch entry val")
		}
		val := data[cursor : cursor+vLen]
		cursor += vLen

		// Handle Commit Marker
		if op == protocol.OpJournalCommit {
			if len(buffer) > 0 {
				if err := store.ReplicateBatch(buffer); err != nil {
					return buffer, lastCommittedID, err
				}
				lastCommittedID = lid
				buffer = buffer[:0]
			} else {
				lastCommittedID = lid
			}
		} else {
			buffer = append(buffer, protocol.LogEntry{
				LogSeq: lid,
				OpCode: op,
				Key:    key,
				Value:  val,
			})
		}
	}
	return buffer, lastCommittedID, nil
}
