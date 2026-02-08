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
	peers      map[string][]ReplicaSource // Address -> []ReplicaSource
	cancelFunc map[string]context.CancelFunc
	stores     map[string]*store.Store
	tlsConf    *tls.Config
	logger     *slog.Logger
}

func NewReplicationManager(stores map[string]*store.Store, tlsConf *tls.Config, logger *slog.Logger) *ReplicationManager {
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
		logID := uint64(stats.Offset) // Use Offset as LogID
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		binary.Write(buf, binary.BigEndian, logID)
		rm.logger.Debug("Sending Hello for DB", "remote_db", cfg.RemoteDB, "local_db", cfg.LocalDB, "startLogID", logID)

		remoteToLocal[cfg.RemoteDB] = append(remoteToLocal[cfg.RemoteDB], cfg.LocalDB)
	}

	// Send Hello
	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
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
		if respHead[0] == protocol.OpCodeReplBatch {
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
						h[0] = protocol.OpCodeReplAck
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

func applyReplicationBatch(store *store.Store, count uint32, data []byte) (uint64, error) {
	cursor := 0
	var ops []protocol.LogEntry
	var maxID uint64

	for i := 0; i < int(count); i++ {
		if cursor+17 > len(data) {
			return 0, fmt.Errorf("malformed batch entry header")
		}
		lid := binary.BigEndian.Uint64(data[cursor : cursor+8])
		// lsn := binary.BigEndian.Uint64(data[cursor+8 : cursor+16]) // LSN unused on follower side
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
		if op != protocol.OpJournalDelete {
			if cursor+vLen > len(data) {
				return 0, fmt.Errorf("malformed batch entry val")
			}
			val = data[cursor : cursor+vLen]
			cursor += vLen
		}
		ops = append(ops, protocol.LogEntry{LogSeq: lid, OpCode: op, Key: key, Value: val})
		if lid > maxID {
			maxID = lid
		}
	}

	return maxID, store.ReplicateBatch(ops)
}
