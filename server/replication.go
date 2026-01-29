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
	"sync"
	"time"

	"turnstone/protocol"
	"turnstone/stonedb"
	"turnstone/store"
)

var (
	MaxReplicationBatchSize = 1 * 1024 * 1024 // 1MB
	ErrBatchFull            = errors.New("replication batch full")
)

const (
	// ReplicaWriteTimeout ensures we don't block indefinitely on a hung consumer.
	ReplicaWriteTimeout = 10 * time.Second
	// SnapshotRateLimit caps the full-sync bandwidth to prevent disk/network thrashing (32MB/s).
	SnapshotRateLimit = 32 * 1024 * 1024
)

type replPacket struct {
	dbName string
	opCode uint8
	data   []byte
	count  uint32
}

// HandleReplicaConnection handles the handshake for incoming replicas (Followers or CDC).
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
		name  string
		logID uint64
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
			st.logger.Warn("Replica handshake failed: db name/logID truncated")
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte("DB Name/LogID truncated"))
			return
		}
		name := string(payload[cursor : cursor+nLen])
		cursor += nLen
		logID := binary.BigEndian.Uint64(payload[cursor : cursor+8])
		cursor += 8

		// Check if this server is already a replica for this database.
		if s.replManager != nil && s.replManager.IsReplicating(name) {
			st.logger.Warn("Rejected downstream replication request (cascading/cdc disabled on replicas)", "db", name)
			_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte(fmt.Sprintf("Cascading replication disabled for DB '%s'", name)))
			return
		}

		subs = append(subs, subReq{name, logID})

		if storePtr, ok := s.stores[name]; ok {
			// RULE: Replica can't join non promoted/undefined server
			// We only allow replication if we are PRIMARY.
			if storePtr.GetState() != store.StatePrimary {
				st.logger.Warn("Replica handshake rejected: Server is not PRIMARY for this DB", "db", name, "state", storePtr.GetState())
				_ = s.writeBinaryResponse(conn, protocol.ResStatusErr, []byte(fmt.Sprintf("Replica handshake rejected: DB '%s' is %s (must be PRIMARY)", name, storePtr.GetState())))
				return // Disconnect
			}

			// INFO: Replica Connected
			st.logger.Info("Replica subscribed", "db", name, "start_seq", logID)
			storePtr.RegisterReplica(replicaID, logID, st.role)

			// Capture the kill switch for this specific subscription
			if ch := storePtr.GetReplicaSignalChannel(replicaID); ch != nil {
				killChannels = append(killChannels, ch)
			}

			defer func(sp *store.Store, dbName string) {
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

	// Create a merged kill channel. If any store triggers a kill, we drop the connection.
	mergedKill := make(chan struct{})
	killWg := sync.WaitGroup{}
	killOnce := sync.Once{}

	for _, ch := range killChannels {
		killWg.Add(1)
		go func(c <-chan struct{}) {
			defer killWg.Done()
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
			go func(name string, sp *store.Store, startLogID uint64) {
				defer wg.Done()
				if err := s.streamDB(name, sp, startLogID, outCh, done, st.logger); err != nil {
					st.logger.Error("Replica stream failed", "db", name, "err", err)
					select {
					case errCh <- err:
					default:
					}
				}
			}(req.name, storePtr, req.logID)
		}
	}

	// ACK Reader (KeepAlive & Progress)
	go func() {
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
			if h[0] == protocol.OpCodeReplAck {
				ln := binary.BigEndian.Uint32(h[1:])
				b := make([]byte, ln)
				if _, err := io.ReadFull(r, b); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				// Parse ACK: [DBNameLen][DBName][LogID]
				if len(b) > 4 {
					nL := binary.BigEndian.Uint32(b[:4])
					if len(b) >= 4+int(nL)+8 {
						dbName := string(b[4 : 4+nL])
						logID := binary.BigEndian.Uint64(b[4+nL:])
						if storePtr, ok := s.stores[dbName]; ok {
							storePtr.UpdateReplicaLogSeq(replicaID, logID)
						}
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
			bodyBuf.Write(p.data)

			rawBody := bodyBuf.Bytes()
			crc := crc32.Checksum(rawBody, protocol.Crc32Table)

			// Packet structure: [CRC(4)][RawBody]
			finalPayloadLen := 4 + len(rawBody)

			header := make([]byte, 5)
			header[0] = p.opCode
			binary.BigEndian.PutUint32(header[1:], uint32(finalPayloadLen))

			// Enforce strict Write Deadline to prevent blocking streamDB
			// If the client is slow/hung, we drop them.
			if err := conn.SetWriteDeadline(time.Now().Add(ReplicaWriteTimeout)); err != nil {
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

func (s *Server) streamDB(name string, st *store.Store, minLogID uint64, outCh chan<- replPacket, done <-chan struct{}, logger *slog.Logger) error {
	currentLogID := minLogID

	// 0. Send Initial Timeline
	currentTL := st.DB.CurrentTimeline()
	tlBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(tlBuf, currentTL)
	select {
	case outCh <- replPacket{
		dbName: name,
		opCode: protocol.OpCodeReplTimeline,
		data:   tlBuf,
		count:  0,
	}:
		logger.Debug("Sent initial timeline to replica", "db", name, "timeline", currentTL)
	case <-done:
		return nil
	}

	// Retry loop for WAL Streaming vs Snapshot Fallback
	for {
		// 1. Snapshot Check / WAL Availability
		// If WAL is unavailable, we MUST snapshot.
		// If WAL is available, we proceed to stream it.
		// We use a check + loop structure to handle race conditions where WAL disappears.

		// Check WAL Availability first
		err := st.ScanWAL(currentLogID+1, func([]stonedb.ValueLogEntry) error { return nil })

		if err == stonedb.ErrLogUnavailable && currentLogID < st.LastOpID() {
			// INFO: Snapshot required
			logger.Info("Replica lag exceeds WAL retention. Starting Full Snapshot.", "db", name, "req_seq", currentLogID, "head", st.LastOpID())

			// Rate Limiting State
			var bytesSent int64
			startTime := time.Now()

			snapTxID, snapOpID, snapErr := st.StreamSnapshot(func(batch []stonedb.SnapshotEntry) error {
				var buf bytes.Buffer
				for _, e := range batch {
					binary.Write(&buf, binary.BigEndian, uint32(len(e.Key)))
					buf.Write(e.Key)
					binary.Write(&buf, binary.BigEndian, uint32(len(e.Value)))
					buf.Write(e.Value)
				}

				// Rate Limiting Logic:
				// Calculate expected duration for bytes sent so far.
				// If actual duration is less, sleep the difference.
				payloadSize := int64(buf.Len())
				bytesSent += payloadSize
				expectedDuration := time.Duration((float64(bytesSent) / float64(SnapshotRateLimit)) * float64(time.Second))
				if elapsed := time.Since(startTime); elapsed < expectedDuration {
					// Throttle
					time.Sleep(expectedDuration - elapsed)
				}

				select {
				case outCh <- replPacket{
					dbName: name,
					opCode: protocol.OpCodeReplSnapshot,
					data:   buf.Bytes(),
					count:  uint32(len(batch)),
				}:
				case <-done:
					return io.EOF
				}
				return nil
			})

			if snapErr != nil {
				return fmt.Errorf("snapshot failed: %w", snapErr)
			}

			doneBuf := make([]byte, 16)
			binary.BigEndian.PutUint64(doneBuf[0:], snapTxID)
			binary.BigEndian.PutUint64(doneBuf[8:], snapOpID)

			select {
			case outCh <- replPacket{
				dbName: name,
				opCode: protocol.OpCodeReplSnapshotDone,
				data:   doneBuf,
				count:  0,
			}:
			case <-done:
				return nil
			}
			// INFO: Snapshot done
			logger.Info("Snapshot complete. Resuming WAL stream.", "db", name, "resume_seq", snapOpID)
			currentLogID = snapOpID

			// Resend Timeline after Snapshot
			currentTL = st.DB.CurrentTimeline()
			binary.BigEndian.PutUint64(tlBuf, currentTL)
			select {
			case outCh <- replPacket{
				dbName: name,
				opCode: protocol.OpCodeReplTimeline,
				data:   tlBuf,
				count:  0,
			}:
			case <-done:
				return nil
			}
		}

		// 2. WAL Streaming Loop
		if err := s.runWALStreamLoop(name, st, currentLogID, outCh, done, logger); err != nil {
			// If error is ErrLogUnavailable, break inner loop and retry outer loop (which will trigger snapshot)
			// This handles the race condition where WAL was purged between check and stream.
			if err.Error() == "OUT_OF_SYNC" || errors.Is(err, stonedb.ErrLogUnavailable) {
				logger.Warn("WAL unavailable during stream, falling back to snapshot", "db", name)
				continue
			}
			return err
		}
		return nil
	}
}

func (s *Server) runWALStreamLoop(name string, st *store.Store, startLogID uint64, outCh chan<- replPacket, done <-chan struct{}, logger *slog.Logger) error {
	currentLogID := startLogID
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	safePointTicker := time.NewTicker(1 * time.Second)
	defer safePointTicker.Stop()

	for {
		select {
		case <-done:
			return nil

		case <-st.TimelineSignal():
			logger.Debug("Broadcasting new timeline", "db", name)
			currentTL := st.DB.CurrentTimeline()
			tlBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(tlBuf, currentTL)
			select {
			case outCh <- replPacket{
				dbName: name,
				opCode: protocol.OpCodeReplTimeline,
				data:   tlBuf,
				count:  0,
			}:
			case <-done:
				return nil
			}

		case <-safePointTicker.C:
			minSeq := st.GetMinSlotLogSeq()
			if minSeq > 0 {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, minSeq)
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
			minSeq := st.GetMinSlotLogSeq()
			if minSeq > 0 && minSeq != math.MaxUint64 {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, minSeq)
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
			var batchBuf bytes.Buffer
			var count uint32

			err := st.ScanWAL(currentLogID+1, func(entries []stonedb.ValueLogEntry) error {
				if len(entries) == 0 {
					return nil
				}
				lastEntry := entries[len(entries)-1]

				for _, e := range entries {
					if err := binary.Write(&batchBuf, binary.BigEndian, e.OperationID); err != nil {
						return err
					}
					if err := binary.Write(&batchBuf, binary.BigEndian, e.TransactionID); err != nil {
						return err
					}

					op := protocol.OpJournalSet
					if e.IsDelete {
						op = protocol.OpJournalDelete
					}
					batchBuf.WriteByte(op)

					if err := binary.Write(&batchBuf, binary.BigEndian, uint32(len(e.Key))); err != nil {
						return err
					}
					batchBuf.Write(e.Key)

					if err := binary.Write(&batchBuf, binary.BigEndian, uint32(len(e.Value))); err != nil {
						return err
					}
					batchBuf.Write(e.Value)

					count++
					currentLogID = e.OperationID

					if batchBuf.Len() > MaxReplicationBatchSize {
						return ErrBatchFull
					}
				}

				if err := binary.Write(&batchBuf, binary.BigEndian, lastEntry.OperationID); err != nil {
					return err
				}
				if err := binary.Write(&batchBuf, binary.BigEndian, lastEntry.TransactionID); err != nil {
					return err
				}
				batchBuf.WriteByte(protocol.OpJournalCommit)
				binary.Write(&batchBuf, binary.BigEndian, uint32(0))
				binary.Write(&batchBuf, binary.BigEndian, uint32(0))
				count++

				return nil
			})

			if count > 0 {
				select {
				case outCh <- replPacket{
					dbName: name,
					opCode: protocol.OpCodeReplBatch,
					data:   batchBuf.Bytes(),
					count:  count,
				}:
				case <-done:
					return nil
				}
			}

			if err != nil {
				if err == ErrBatchFull {
					continue
				} else if err == stonedb.ErrLogUnavailable {
					if currentLogID < st.LastOpID() {
						return errors.New("OUT_OF_SYNC")
					}
				} else {
					logger.Error("WAL Scan error", "db", name, "err", err)
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}
