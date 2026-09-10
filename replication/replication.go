// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package replication

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"turnstone/protocol"
	"turnstone/stonedb"
	"turnstone/store"
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

// GetReplicationSource returns the upstream address and remote database name for a given local database.
// Returns empty strings if the database is not replicating.
func (rm *ReplicationManager) GetReplicationSource(dbName string) (string, string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for addr, sources := range rm.peers {
		for _, src := range sources {
			if src.LocalDB == dbName {
				return addr, src.RemoteDB
			}
		}
	}
	return "", ""
}

func (rm *ReplicationManager) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for addr := range rm.peers {
		rm.spawnConnection(addr)
	}
}

// removeDBFromOtherPeersLocked removes any peer entry for dbName that is
// NOT under keepAddr. A given local database must never replicate from more
// than one upstream source concurrently: if the caller bypassed the normal
// REPLICAOF/PROMOTE state-machine guard (or a stale entry was otherwise left
// behind), this ensures AddReplica itself stays globally deduped by dbName
// rather than only deduping within the same sourceAddr's entry. Must be
// called with rm.mu held.
func (rm *ReplicationManager) removeDBFromOtherPeersLocked(dbName, keepAddr string) {
	for addr, existingDBs := range rm.peers {
		if addr == keepAddr {
			continue
		}
		for i, db := range existingDBs {
			if db.LocalDB != dbName {
				continue
			}
			rm.logger.Warn("Replacing stale/duplicate replication source for db", "db", dbName, "old_source", addr, "new_source", keepAddr)
			newDBs := make([]ReplicaSource, 0, len(existingDBs)-1)
			newDBs = append(newDBs, existingDBs[:i]...)
			newDBs = append(newDBs, existingDBs[i+1:]...)
			if len(newDBs) > 0 {
				rm.peers[addr] = newDBs
			} else {
				delete(rm.peers, addr)
				if cancel, exists := rm.cancelFunc[addr]; exists {
					cancel()
					delete(rm.cancelFunc, addr)
				}
			}
			break
		}
	}
}

func (rm *ReplicationManager) AddReplica(dbName, sourceAddr, sourceDB string) error {
	rm.mu.Lock()

	// Check existing
	dbs := rm.peers[sourceAddr]
	for _, db := range dbs {
		if db.LocalDB == dbName {
			rm.mu.Unlock()
			return nil // Already added
		}
	}

	// Defensively dedupe globally by dbName before we even attempt the
	// handshake, so a stale entry under a different address can't leak
	// into candidateDBs sent to the (possibly new) upstream.
	rm.removeDBFromOtherPeersLocked(dbName, sourceAddr)
	dbs = rm.peers[sourceAddr]

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
	currentDBs := rm.peers[sourceAddr]
	for _, db := range currentDBs {
		if db.LocalDB == dbName {
			return nil // Someone else added it
		}
	}

	// Re-dedupe: another AddReplica for the same dbName under a different
	// address may have raced in while we were doing the network handshake.
	rm.removeDBFromOtherPeersLocked(dbName, sourceAddr)
	currentDBs = rm.peers[sourceAddr]

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

// StopAll cancels every outbound replication connection.
func (rm *ReplicationManager) StopAll() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for addr, cancel := range rm.cancelFunc {
		cancel()
		delete(rm.cancelFunc, addr)
		delete(rm.peers, addr)
		rm.logger.Debug("Stopped replication connection", "peer_addr", addr)
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

// safeGo runs fn in a new goroutine with panic recovery. A replication
// connection can be driven by data from a remote peer; if a parsing bug (or
// a malicious/corrupted peer) ever triggers a panic despite the bounds
// checks above, this keeps that failure scoped to the single replication
// goroutine instead of taking down the entire server process, which would
// otherwise happen since Go does not recover panics across goroutine
// boundaries.
func (rm *ReplicationManager) safeGo(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				rm.logger.Error("replication goroutine panicked", "goroutine", name, "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

func (rm *ReplicationManager) spawnConnection(addr string) {
	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelFunc[addr] = cancel
	rm.safeGo("maintainConnection:"+addr, func() { rm.maintainConnection(ctx, addr) })
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

// readLenPrefixedString reads a [Len:4][Bytes] string from buf at cursor,
// returning the string, the cursor advanced past it, and whether the read
// was in-bounds. All wire-format parsing below must use helpers like this
// (rather than slicing directly) because the data originates from a network
// peer: a truncated/malformed/malicious packet must never be able to panic
// this goroutine, since an unrecovered panic here would crash the entire
// server process, not just this replication connection.
func readLenPrefixedString(buf []byte, cursor int) (string, int, bool) {
	if cursor+4 > len(buf) {
		return "", cursor, false
	}
	n := int(binary.BigEndian.Uint32(buf[cursor : cursor+4]))
	cursor += 4
	if n < 0 || cursor+n > len(buf) {
		return "", cursor, false
	}
	return string(buf[cursor : cursor+n]), cursor + n, true
}

// readLenPrefixedBytes is the []byte counterpart of readLenPrefixedString.
func readLenPrefixedBytes(buf []byte, cursor int) ([]byte, int, bool) {
	if cursor+4 > len(buf) {
		return nil, cursor, false
	}
	n := int(binary.BigEndian.Uint32(buf[cursor : cursor+4]))
	cursor += 4
	if n < 0 || cursor+n > len(buf) {
		return nil, cursor, false
	}
	return buf[cursor : cursor+n], cursor + n, true
}

// connectAndSync connects to the remote, sends Hello, and checks the response status.
func (rm *ReplicationManager) connectAndSync(ctx context.Context, addr string, dbs []ReplicaSource, logger *slog.Logger) error {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", addr, rm.tlsConf)
	if err != nil {
		return err
	}

	rm.safeGo("connCloser:"+addr, func() {
		<-ctx.Done()
		_ = conn.Close()
	})
	defer conn.Close()

	logger.Info("Connected to Leader", "db_count", len(dbs))

	remoteToLocal := make(map[string][]string)
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

		// --- VERIFY CRC ---
		// We expect CRC for Batch, Snapshot, SafePoint, Timeline
		if opCode == protocol.OpCodeReplBatch || opCode == protocol.OpCodeReplSnapshot ||
			opCode == protocol.OpCodeReplSafePoint || opCode == protocol.OpCodeReplTimeline ||
			opCode == protocol.OpCodeReplSnapshotDone || opCode == protocol.OpCodeReplLogSegment {

			if len(payload) < 4 {
				return fmt.Errorf("packet too short for crc")
			}
			crcReceived := binary.BigEndian.Uint32(payload[:4])
			rawBody := payload[4:]
			if crc32.Checksum(rawBody, protocol.Crc32Table) != crcReceived {
				return fmt.Errorf("crc mismatch on replication stream")
			}
			// Strip CRC for logic processing
			payload = rawBody
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
			remoteDBName, cursor, ok := readLenPrefixedString(payload, 0)
			if !ok || cursor+4 > len(payload) {
				logger.Warn("Malformed snapshot packet, skipping", "len", len(payload))
				continue
			}
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
						tx := st.DB.NewTransaction(true)
						malformed := false
						for i := 0; i < int(count); i++ {
							key, next, ok := readLenPrefixedBytes(data, dCursor)
							if !ok {
								malformed = true
								break
							}
							dCursor = next
							val, next, ok := readLenPrefixedBytes(data, dCursor)
							if !ok {
								malformed = true
								break
							}
							dCursor = next

							if err := tx.Put(key, val); err != nil {
								tx.Discard()
								logger.Error("Failed to apply snapshot entry", "db", localDBName, "err", err)
								return err
							}
						}
						if malformed {
							tx.Discard()
							return fmt.Errorf("malformed snapshot batch for db %s", localDBName)
						}
						if err := tx.Commit(); err != nil {
							logger.Error("Failed to commit snapshot batch", "db", localDBName, "err", err)
							return err
						}
					}
				}
			}
			continue
		}

		// --- Handle Snapshot Done Signal ---
		if opCode == protocol.OpCodeReplSnapshotDone {
			remoteDBName, cursor, ok := readLenPrefixedString(payload, 0)
			if !ok {
				logger.Warn("Malformed snapshot-done packet, skipping", "len", len(payload))
				continue
			}
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

		// --- Handle raw WAL byte segment (physical replication) ---
		if opCode == protocol.OpCodeReplLogSegment {
			remoteDBName, cursor, ok := readLenPrefixedString(payload, 0)
			if !ok || cursor+4 > len(payload) {
				logger.Warn("Malformed log segment packet, skipping", "len", len(payload))
				continue
			}
			cursor += 4 // skip count/reserved
			if cursor+16 > len(payload) {
				logger.Warn("Malformed log segment header, skipping", "len", len(payload))
				continue
			}
			startOff := binary.BigEndian.Uint64(payload[cursor : cursor+8])
			endOff := binary.BigEndian.Uint64(payload[cursor+8 : cursor+16])
			cursor += 16
			segData := payload[cursor:]
			if endOff < startOff || int(endOff-startOff) != len(segData) {
				logger.Warn("Log segment offset mismatch", "start", startOff, "end", endOff, "len", len(segData))
				continue
			}

			if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
				for _, localDBName := range localDBNames {
					if st, ok := rm.stores[localDBName]; ok {
						lastID, err := st.ApplyLogSegment(segData)
						if err != nil {
							logger.Error("Failed to apply log segment", "db", localDBName, "err", err)
							return err
						}
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
			continue
		}

		// --- Handle Standard WAL Batch (CDC logical replication) ---
		if opCode == protocol.OpCodeReplBatch {
			remoteDBName, cursor, ok := readLenPrefixedString(payload, 0)
			if !ok || cursor+4 > len(payload) {
				logger.Warn("Malformed batch packet, skipping", "len", len(payload))
				continue
			}
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]

			if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
				for _, localDBName := range localDBNames {
					if st, ok := rm.stores[localDBName]; ok {
						lastID, err := processReplicationPacket(st, count, data)
						if err != nil {
							logger.Error("Failed to process WAL batch", "db", localDBName, "err", err)
							return err
						}

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

// walRecordTypeForJournalOp maps the wire journal opcode back to the typed
// WAL record kind so followers can apply a physical stream directly through
// the same eager engine path the leader used to produce it.
func walRecordTypeForJournalOp(op uint8) (stonedb.WALRecordType, bool) {
	switch op {
	case protocol.OpJournalBegin:
		return stonedb.WALRecordBegin, true
	case protocol.OpJournalSet:
		return stonedb.WALRecordSet, true
	case protocol.OpJournalDelete:
		return stonedb.WALRecordDelete, true
	case protocol.OpJournalCommit:
		return stonedb.WALRecordCommit, true
	case protocol.OpJournalAbort:
		return stonedb.WALRecordAbort, true
	}
	return 0, false
}

// processReplicationPacket applies each replicated WAL record directly and
// immediately (no client-side buffering): the leader has already resolved
// eager visibility, so followers just mirror it record-for-record.
func processReplicationPacket(st *store.Store, count uint32, data []byte) (uint64, error) {
	cursor := 0
	var lastAppliedID uint64

	for i := 0; i < int(count); i++ {
		// Entry: [LogID(8)][TxID(8)][Op(1)][KLen(4)][Key][VLen(4)][Val]
		if cursor+17 > len(data) {
			return lastAppliedID, fmt.Errorf("malformed batch entry header")
		}
		lid := binary.BigEndian.Uint64(data[cursor : cursor+8])
		xid := binary.BigEndian.Uint64(data[cursor+8 : cursor+16])
		op := data[cursor+16]
		cursor += 17

		kLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if cursor+kLen > len(data) {
			return lastAppliedID, fmt.Errorf("malformed batch entry key")
		}
		key := data[cursor : cursor+kLen]
		cursor += kLen

		vLen := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if cursor+vLen > len(data) {
			return lastAppliedID, fmt.Errorf("malformed batch entry val")
		}
		val := data[cursor : cursor+vLen]
		cursor += vLen

		recType, ok := walRecordTypeForJournalOp(op)
		if !ok {
			return lastAppliedID, fmt.Errorf("unknown journal opcode: %d", op)
		}

		rec := stonedb.WALRecord{Type: recType, XID: xid, OpID: lid, Key: key, Value: val}
		if err := st.ApplyRecord(rec); err != nil {
			return lastAppliedID, fmt.Errorf("apply record (op=%d, opid=%d): %w", op, lid, err)
		}
		lastAppliedID = lid
	}

	return lastAppliedID, nil
}
