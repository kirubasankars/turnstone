// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package repl

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

	"turnstone/database"
	"turnstone/protocol"
)

type Source struct {
	LocalDB  string `json:"local_db"`
	RemoteDB string `json:"remote_db"`
}

type Manager struct {
	mu         sync.Mutex
	serverID   string
	peers      map[string][]Source // Addr -> List of DBs
	cancelFunc map[string]context.CancelFunc
	stores     map[string]*database.Database
	tlsConf    *tls.Config
	logger     *slog.Logger
}

// NewManager creates a manager for outgoing replication connections.
// Persistence is intentionally disabled; replication must be configured at runtime.
func NewManager(serverID string, stores map[string]*database.Database, tlsConf *tls.Config, logger *slog.Logger) *Manager {
	return &Manager{
		serverID:   serverID,
		peers:      make(map[string][]Source),
		cancelFunc: make(map[string]context.CancelFunc),
		stores:     stores,
		tlsConf:    tlsConf,
		logger:     logger,
	}
}

// IsFollowing checks if the specific database is currently configured to replicate
// from an upstream source. Used to prevent cascading repl.
func (rm *Manager) IsFollowing(dbName string) bool {
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

// Source returns the upstream address and remote database name for a given local database.
// Returns empty strings if the database is not replicating.
func (rm *Manager) Source(dbName string) (string, string) {
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

func (rm *Manager) Start() {
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
// behind), this ensures Follow itself stays globally deduped by dbName
// rather than only deduping within the same sourceAddr's entry. Must be
// called with rm.mu held.
func (rm *Manager) removeDBFromOtherPeersLocked(dbName, keepAddr string) {
	for addr, existingDBs := range rm.peers {
		if addr == keepAddr {
			continue
		}
		for i, db := range existingDBs {
			if db.LocalDB != dbName {
				continue
			}
			rm.logger.Warn("Replacing stale/duplicate replication source for db", "db", dbName, "old_source", addr, "new_source", keepAddr)
			newDBs := make([]Source, 0, len(existingDBs)-1)
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

func (rm *Manager) Follow(dbName, sourceAddr, sourceDB string) error {
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
	candidateDBs := make([]Source, len(dbs), len(dbs)+1)
	copy(candidateDBs, dbs)
	candidateDBs = append(candidateDBs, Source{LocalDB: dbName, RemoteDB: sourceDB})

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

	// Re-dedupe: another Follow for the same dbName under a different
	// address may have raced in while we were doing the network handshake.
	rm.removeDBFromOtherPeersLocked(dbName, sourceAddr)
	currentDBs = rm.peers[sourceAddr]

	// Append to the *current* authoritative list
	finalDBs := append(currentDBs, Source{LocalDB: dbName, RemoteDB: sourceDB})
	rm.peers[sourceAddr] = finalDBs

	if cancel, exists := rm.cancelFunc[sourceAddr]; exists {
		cancel()
	}
	rm.spawnConnection(sourceAddr)
	rm.logger.Info("Added replica source", "db", dbName, "source", sourceAddr, "remote_db", sourceDB)
	return nil
}

// verifyHandshake connects to the remote, sends Hello, and checks the response status.
func (rm *Manager) verifyHandshake(addr string, dbs []Source) error {
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
		var offset uint64
		if st, ok := rm.stores[cfg.LocalDB]; ok {
			offset = st.LastLogOffset()
		}
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		binary.Write(buf, binary.BigEndian, offset)
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
func (rm *Manager) StopAll() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for addr, cancel := range rm.cancelFunc {
		cancel()
		delete(rm.cancelFunc, addr)
		delete(rm.peers, addr)
		rm.logger.Debug("Stopped replication connection", "peer_addr", addr)
	}
}

func (rm *Manager) StopFollowing(dbName string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for addr, dbs := range rm.peers {
		newDBs := make([]Source, 0, len(dbs))
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
func (rm *Manager) safeGo(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				rm.logger.Error("replication goroutine panicked", "goroutine", name, "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

func (rm *Manager) spawnConnection(addr string) {
	ctx, cancel := context.WithCancel(context.Background())
	rm.cancelFunc[addr] = cancel
	rm.safeGo("maintainConnection:"+addr, func() { rm.maintainConnection(ctx, addr) })
}

func (rm *Manager) maintainConnection(ctx context.Context, addr string) {
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
		dbs := make([]Source, len(rm.peers[addr]))
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

// connectAndSync connects to the remote, sends Hello, and checks the response status.
func (rm *Manager) connectAndSync(ctx context.Context, addr string, dbs []Source, logger *slog.Logger) error {
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
	expectedOffset := make(map[string]uint64)

	// Handshake
	// Format: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][Offset:8]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(rm.serverID)))
	buf.WriteString(rm.serverID)
	binary.Write(buf, binary.BigEndian, uint32(len(dbs)))

	for _, cfg := range dbs {
		var offset uint64
		if st, ok := rm.stores[cfg.LocalDB]; ok {
			offset = st.LastLogOffset()
		}
		binary.Write(buf, binary.BigEndian, uint32(len(cfg.RemoteDB)))
		buf.WriteString(cfg.RemoteDB)
		binary.Write(buf, binary.BigEndian, offset)

		logger.Debug("Sending Hello for DB", "remote_db", cfg.RemoteDB, "local_db", cfg.LocalDB, "start_log_id", offset)

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
		// We expect CRC for SafePoint and LogSegment
		if opCode == protocol.OpCodeReplSafePoint ||
			opCode == protocol.OpCodeReplLogRange {

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
				safeOffset := binary.BigEndian.Uint64(payload[cursor:])

				if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
					for _, localDB := range localDBNames {
						if st, ok := rm.stores[localDB]; ok {
							st.SetLeaderRetainOffset(safeOffset)
						}
					}
				}
			}
			continue
		}

		// --- Handle raw log byte segment (physical replication) ---
		if opCode == protocol.OpCodeReplLogRange {
			remoteDBName, cursor, ok := readLenPrefixedString(payload, 0)
			if !ok || cursor+4 > len(payload) {
				return fmt.Errorf("malformed log segment packet (len=%d)", len(payload))
			}
			cursor += 4 // skip count/reserved
			if cursor+16 > len(payload) {
				return fmt.Errorf("malformed log segment header (len=%d)", len(payload))
			}
			startOff := binary.BigEndian.Uint64(payload[cursor : cursor+8])
			endOff := binary.BigEndian.Uint64(payload[cursor+8 : cursor+16])
			cursor += 16
			segData := payload[cursor:]
			if endOff < startOff || int(endOff-startOff) != len(segData) {
				return fmt.Errorf("log segment offset mismatch (start=%d end=%d len=%d)", startOff, endOff, len(segData))
			}

			if localDBNames, ok := remoteToLocal[remoteDBName]; ok {
				for _, localDBName := range localDBNames {
					if st, ok := rm.stores[localDBName]; ok {
						exp, ok := expectedOffset[localDBName]
						if !ok {
							exp = st.LastLogOffset()
						}
						if startOff != exp {
							return fmt.Errorf("log range start mismatch for db %s: got %d want %d", localDBName, startOff, exp)
						}
						if _, err := st.ApplyLogRange(segData); err != nil {
							logger.Error("Failed to apply log segment", "db", localDBName, "err", err)
							return err
						}
						expectedOffset[localDBName] = endOff
						if endOff > 0 {
							ackBuf := new(bytes.Buffer)
							binary.Write(ackBuf, binary.BigEndian, uint32(len(remoteDBName)))
							ackBuf.WriteString(remoteDBName)
							binary.Write(ackBuf, binary.BigEndian, endOff)

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
	}
}
