// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/database"
	"turnstone/engine"
	"turnstone/protocol"
)

// recoverAndLog is deferred at the top of every replication goroutine. These
// goroutines parse network input from remote peers and run for the lifetime
// of a replica connection; an unrecovered panic in any of them would take
// down the entire server process (all databases, all client connections),
// not just this one connection, since Go does not recover panics across
// goroutine boundaries.
func recoverAndLog(logger *slog.Logger, name string) {
	if r := recover(); r != nil {
		logger.Error("replication goroutine panicked", "goroutine", name, "panic", r, "stack", string(debug.Stack()))
	}
}

var (
	MaxReplicationBatchSize = 1 * 1024 * 1024 // 1MB
)

var (
	// replicaWriteTimeoutNs ensures we don't block indefinitely on a hung consumer.
	// Stored as nanoseconds so tests can mutate it without data races.
	replicaWriteTimeoutNs atomic.Int64
)

func init() {
	replicaWriteTimeoutNs.Store(int64((10 * time.Minute).Nanoseconds()))
}

func replicaWriteTimeout() time.Duration {
	return time.Duration(replicaWriteTimeoutNs.Load())
}

func setReplicaWriteTimeout(d time.Duration) {
	replicaWriteTimeoutNs.Store(int64(d.Nanoseconds()))
}

const (
	// MaxReplAckSize bounds the ACK-reader's per-frame allocation. A real
	// ACK body is [DBNameLen(4)][DBName][Offset(8)]; 64KB is far more than
	// any legitimate db name could need.
	MaxReplAckSize = 64 * 1024
)

type replPacket struct {
	dbName string
	opCode uint8
	data   []byte
	count  uint32
}

// HandleReplicaConnection handles the handshake for incoming replicas.
// It now uses the context-aware logger from the connection state (*connState).
func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte, st *connState) {
	// We do NOT clear deadlines globally anymore. We set them per-operation.
	// However, for the READER, we might want a long idle timeout (handled in heartbeat logic usually),
	// but for the WRITER, we must be strict.

	// 1. Parse Hello: [Ver:4][IDLen:4][ID][NumDBs:4] ...
	if len(payload) < 8 {
		st.logger.Warn("Replica handshake failed: payload too short")
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Payload too short"))
		return
	}
	cursor := 4

	// Parse ID
	if cursor+4 > len(payload) {
		st.logger.Warn("Replica handshake failed: ID len truncated")
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("ID len truncated"))
		return
	}
	idLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
	cursor += 4
	if cursor+idLen > len(payload) {
		st.logger.Warn("Replica handshake failed: ID truncated")
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("ID truncated"))
		return
	}
	replicaID := string(payload[cursor : cursor+idLen])
	cursor += idLen

	if replicaID == "" || replicaID == "client-unknown" {
		st.logger.Warn("Replica handshake rejected: missing client ID")
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Missing client ID"))
		return
	}

	// Update Logger with specific Replica ID
	st.logger = st.logger.With("replica_id", replicaID)

	// Parse DB Count
	if cursor+4 > len(payload) {
		st.logger.Warn("Replica handshake failed: count truncated")
		_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("Count truncated"))
		return
	}
	count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
	cursor += 4

	type subReq struct {
		name   string
		offset uint64
	}
	var subs []subReq

	// Track the kill switches for all involved databases
	var killChannels []<-chan struct{}

	for i := 0; i < int(count); i++ {
		if cursor+4 > len(payload) {
			st.logger.Warn("Replica handshake failed: db name len truncated")
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("DB Name len truncated"))
			return
		}
		nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
		cursor += 4
		if cursor+nLen+8 > len(payload) {
			st.logger.Warn("Replica handshake failed: db name/offset truncated")
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("DB Name/Offset truncated"))
			return
		}
		name := string(payload[cursor : cursor+nLen])
		cursor += nLen
		offset := binary.BigEndian.Uint64(payload[cursor : cursor+8])
		cursor += 8

		if storePtr, ok := s.stores[name]; ok {
			if !storePtr.IsValidReplicationCursor(offset) {
				st.logger.Warn("Replica handshake rejected: invalid cursor", "db", name, "cursor", offset, "head", storePtr.LastLogOffset())
				_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte(fmt.Sprintf("Invalid replication cursor for DB '%s'", name)))
				return
			}
		}

		// Check if this server is already a replica for this database.
		if s.replManager != nil && s.replManager.IsFollowing(name) {
			st.logger.Warn("Rejected downstream replication request (cascading disabled on replicas)", "db", name)
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte(fmt.Sprintf("Cascading replication disabled for DB '%s'", name)))
			return
		}

		subs = append(subs, subReq{name, offset})

		if storePtr, ok := s.stores[name]; ok {
			// RULE: Replica can't join non promoted/undefined server
			// We only allow replication if we are PRIMARY.
			if storePtr.GetState() != database.StatePrimary {
				st.logger.Warn("Replica handshake rejected: Server is not PRIMARY for this DB", "db", name, "state", storePtr.GetState())
				_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte(fmt.Sprintf("Replica handshake rejected: DB '%s' is %s (must be PRIMARY)", name, storePtr.GetState())))
				return // Disconnect
			}

			// INFO: Replica Connected
			st.logger.Info("Replica subscribed", "db", name, "start_seq", offset)
			storePtr.RegisterReplica(replicaID, offset, st.role)

			// Capture the kill switch for this specific subscription
			if ch := storePtr.GetReplicaSignalChannel(replicaID); ch != nil {
				killChannels = append(killChannels, ch)
			}

			defer func(sp *database.Database, dbName string) {
				// INFO: Replica Disconnected
				st.logger.Info("Replica disconnected", "db", dbName)
				sp.UnregisterReplica(replicaID)
			}(storePtr, name)
		} else {
			st.logger.Warn("Replica requested unknown database", "db", name)
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte(fmt.Sprintf("Unknown database: %s", name)))
			return
		}
	}

	if err := s.writeBinaryResponse(conn, protocol.ResStatusOK, nil); err != nil {
		st.logger.Warn("Failed to send replica handshake OK", "err", err)
		return
	}

	// Create a merged kill channel. If any store triggers a kill, we drop the connection.
	mergedKill := make(chan struct{})
	killWg := sync.WaitGroup{}
	killOnce := sync.Once{}

	for _, ch := range killChannels {
		killWg.Add(1)
		go func(c <-chan struct{}) {
			defer killWg.Done()
			defer recoverAndLog(st.logger, "killChannelWatcher")
			select {
			case <-c:
				killOnce.Do(func() { close(mergedKill) })
			case <-mergedKill: // Stop waiting if already closed
			}
		}(ch)
	}

	// Ensure we don't leak merged kill routines
	defer func() {
		// Just in case we exit normally without kill
		killOnce.Do(func() { close(mergedKill) })
		killWg.Wait()
	}()

	outCh := make(chan replPacket, 10)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done) // Closing done signals streamDB producers to stop

	var wg sync.WaitGroup

	for _, req := range subs {
		if storePtr, ok := s.stores[req.name]; ok {
			wg.Add(1)
			go func(name string, sp *database.Database, startOffset uint64) {
				defer wg.Done()
				defer recoverAndLog(st.logger, "streamDB:"+name)
				if err := s.streamDB(name, sp, startOffset, outCh, done, st.logger); err != nil {
					st.logger.Error("Replica stream failed", "db", name, "err", err)
					select {
					case errCh <- err:
					default:
					}
				}
			}(req.name, storePtr, req.offset)
		}
	}

	// ACK Reader (KeepAlive & Progress)
	go func() {
		defer recoverAndLog(st.logger, "ackReader")
		h := make([]byte, 5)
		for {
			// Allow a generous read deadline for heartbeats, but not infinite
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			if _, err := io.ReadFull(r, h); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			ln := binary.BigEndian.Uint32(h[1:])
			// Cap the allocation: this length is fully peer-controlled, and
			// a real ACK body ([DBNameLen][DBName][Offset]) never needs more
			// than a few hundred bytes. Without a cap, a malicious/broken
			// peer can force an arbitrarily large (up to 4GB) allocation
			// per frame here.
			if ln > MaxReplAckSize {
				select {
				case errCh <- fmt.Errorf("ack frame too large: %d bytes", ln):
				default:
				}
				return
			}
			b := make([]byte, ln)
			if _, err := io.ReadFull(r, b); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			// Always consume exactly `ln` bytes above regardless of opcode
			// so framing stays in sync on this connection: previously, a
			// non-ACK opcode here skipped reading its body entirely,
			// permanently desyncing every subsequent header read on this
			// connection.
			if h[0] != protocol.OpCodeReplAck {
				continue
			}

			// Parse ACK: [DBNameLen][DBName][Offset]
			if len(b) > 4 {
				nL := binary.BigEndian.Uint32(b[:4])
				if len(b) >= 4+int(nL)+8 {
					offset := binary.BigEndian.Uint64(b[4+nL:])
					if storePtr, ok := s.stores[string(b[4:4+nL])]; ok {
						storePtr.UpdateReplicaOffset(replicaID, offset)
					}
				}
			}
		}
	}()

	// Central Writer
	// This loop multiplexes packets from all DB streams into the single net.Conn.
	for {
		select {
		case <-mergedKill:
			st.logger.Info("Replica disconnected by store (kill switch)")
			return
		case err := <-errCh:
			st.logger.Warn("Dropping replica connection due to error", "err", err)
			return
		case p := <-outCh:
			// Frame: [OpCode][TotalLen] [CRC32(4)][DBNameLen][DBName][Count/Reserved][Data...]
			// We calculate CRC on the payload body.
			// Body = [DBNameLen][DBName][Count/Reserved][Data...]

			bodyBuf := new(bytes.Buffer)
			binary.Write(bodyBuf, binary.BigEndian, uint32(len(p.dbName)))
			bodyBuf.WriteString(p.dbName)
			binary.Write(bodyBuf, binary.BigEndian, p.count)
			if len(p.data) > 0 {
				// Copy: streamDB may reuse backing arrays (e.g. tlBuf) after enqueue.
				bodyBuf.Write(append([]byte(nil), p.data...))
			}

			rawBody := bodyBuf.Bytes()
			crc := crc32.Checksum(rawBody, protocol.Crc32Table)

			// Packet structure: [CRC(4)][RawBody]
			finalPayloadLen := 4 + len(rawBody)

			header := make([]byte, 5)
			header[0] = p.opCode
			binary.BigEndian.PutUint32(header[1:], uint32(finalPayloadLen))

			// Enforce strict Write Deadline to prevent blocking streamDB
			// If the client is slow/hung, we drop them.
			if err := conn.SetWriteDeadline(time.Now().Add(replicaWriteTimeout())); err != nil {
				st.logger.Warn("Failed to set write deadline", "err", err)
				return
			}

			// Write Header
			if _, err := conn.Write(header); err != nil {
				st.logger.Warn("Replica write failed (header)", "err", err)
				return
			}

			// Write CRC
			crcBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(crcBuf, crc)
			if _, err := conn.Write(crcBuf); err != nil {
				st.logger.Warn("Replica write failed (crc)", "err", err)
				return
			}

			// Write Body
			if _, err := conn.Write(rawBody); err != nil {
				st.logger.Warn("Replica write failed (body)", "err", err)
				return
			}
		}
	}
}

func (s *Server) streamDB(name string, st *database.Database, startOffset uint64, outCh chan<- replPacket, done <-chan struct{}, logger *slog.Logger) error {
	return s.runLogStreamLoop(name, st, startOffset, outCh, done, logger)
}

func (s *Server) runLogStreamLoop(name string, st *database.Database, startOffset uint64, outCh chan<- replPacket, done <-chan struct{}, logger *slog.Logger) error {
	currentByteOffset := int64(startOffset)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	safePointTicker := time.NewTicker(1 * time.Second)
	defer safePointTicker.Stop()

	for {
		select {
		case <-done:
			return nil

		case <-safePointTicker.C:
			minOffset := st.MinReplicaOffset()
			if minOffset > 0 {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, minOffset)
				select {
				case outCh <- replPacket{
					dbName: name,
					opCode: protocol.OpCodeReplSafePoint,
					data:   buf,
					count:  0,
				}:
				case <-done:
					return nil
				}
			}

		case <-st.SafePointSignal():
			minOffset := st.MinReplicaOffset()
			if minOffset > 0 && minOffset != math.MaxUint64 {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, minOffset)
				select {
				case outCh <- replPacket{
					dbName: name,
					opCode: protocol.OpCodeReplSafePoint,
					data:   buf,
					count:  0,
				}:
				case <-done:
					return nil
				}
			}

		case <-ticker.C:
			head := st.LastLogOffset()
			if uint64(currentByteOffset) >= head {
				continue
			}
			segData, nextOff, err := st.ReadLogRange(currentByteOffset, int64(MaxReplicationBatchSize))
			if err != nil {
				if errors.Is(err, engine.ErrLogUnavailable) {
					return fmt.Errorf("log unavailable at offset %d: %w", currentByteOffset, err)
				}
				logger.Error("Log segment read error", "db", name, "err", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if len(segData) > 0 {
				payload := make([]byte, 16+len(segData))
				binary.BigEndian.PutUint64(payload[0:], uint64(currentByteOffset))
				binary.BigEndian.PutUint64(payload[8:], uint64(nextOff))
				copy(payload[16:], segData)
				select {
				case outCh <- replPacket{
					dbName: name,
					opCode: protocol.OpCodeReplLogRange,
					data:   payload,
					count:  0,
				}:
				case <-done:
					return nil
				}
				currentByteOffset = nextOff
			}
		}
	}
}
