package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"turnstone/protocol"
	"turnstone/stonedb"
	"turnstone/store"
)

var (
	// MaxReplicationBatchSize limits the payload size of a single replication batch
	MaxReplicationBatchSize = 1 * 1024 * 1024 // 1MB
	ErrBatchFull            = errors.New("replication batch full")
)

type replPacket struct {
	dbName string
	opCode uint8 // Added to support Snapshot opcodes
	data   []byte
	count  uint32
}

// HandleReplicaConnection multiplexes streams from multiple databases onto one connection.
// It uses the provided role to distinguish between full Replicas (quorum members) and CDC clients (observers).
func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte, role string, certClientID string) {
	// 1. Parse Hello: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	if len(payload) < 8 {
		s.logger.Error("HandleReplicaConnection: payload too short for header")
		return
	}
	// ver := binary.BigEndian.Uint32(payload[0:4])
	cursor := 4

	// Parse ID
	if cursor+4 > len(payload) {
		s.logger.Error("HandleReplicaConnection: payload truncated reading ID len")
		return
	}
	idLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
	cursor += 4
	if cursor+idLen > len(payload) {
		s.logger.Error("HandleReplicaConnection: payload truncated reading ID")
		return
	}
	replicaID := string(payload[cursor : cursor+idLen])
	cursor += idLen

	// ID is required field
	if replicaID == "" || replicaID == "client-unknown" {
		s.logger.Error("HandleReplicaConnection: replica connected without explicit ID", "role", role)
		return
	}

	// Parse DB Count
	if cursor+4 > len(payload) {
		s.logger.Error("HandleReplicaConnection: payload truncated reading count")
		return
	}
	count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
	cursor += 4

	type subReq struct {
		name  string
		logID uint64
	}
	var subs []subReq

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
			s.logger.Info("Replica subscribed",
				"replica_id", replicaID,
				"auth_id", certClientID,
				"db", name,
				"startLogID", logID,
				"role", role,
			)
			st.RegisterReplica(replicaID, logID, role)

			defer func(storePtr *store.Store, dbName string) {
				s.logger.Info("Replica unsubscribed (connection closed)", "replica", replicaID, "db", dbName)
				storePtr.UnregisterReplica(replicaID)
			}(st, name)
		} else {
			s.logger.Warn("Replica requested unknown database", "replica", replicaID, "db", name)
		}
	}

	// 2. Start Multiplexer
	outCh := make(chan replPacket, 10)
	errCh := make(chan error, 1) // Signal channel for slow/failed streams
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup

	// Start reader for each requested Database
	for _, req := range subs {
		storeInstance, ok := s.stores[req.name]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(name string, st *store.Store, startLogID uint64) {
			defer wg.Done()
			if err := s.streamDB(name, st, startLogID, outCh, done); err != nil {
				s.logger.Error("StreamDB failed", "db", name, "err", err)
				select {
				case errCh <- err:
				default:
				}
			}
		}(req.name, storeInstance, req.logID)
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
					s.logger.Error("Replica ACK body read failed", "replica", replicaID, "err", err)
					select {
					case errCh <- err:
					default:
					}
					return
				}

				if len(b) > 4 {
					nL := binary.BigEndian.Uint32(b[:4])
					if len(b) >= 4+int(nL)+8 {
						dbName := string(b[4 : 4+nL])
						logID := binary.BigEndian.Uint64(b[4+nL:])
						if st, ok := s.stores[dbName]; ok {
							st.UpdateReplicaLogSeq(replicaID, logID)
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
			if err == io.EOF {
				return
			}
			s.logger.Warn("Dropping failed replica connection", "addr", replicaID, "err", err)

			// Attempt to send error to client before closing
			errMsg := err.Error()
			errHeader := make([]byte, 5)
			errHeader[0] = protocol.ResStatusErr
			binary.BigEndian.PutUint32(errHeader[1:], uint32(len(errMsg)))

			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			conn.Write(errHeader)
			conn.Write([]byte(errMsg))

			return

		case p := <-outCh:
			// Frame: [OpCodeReplBatch][TotalLen] [DBNameLen][DBName][Count][BatchData...]
			totalLen := 4 + len(p.dbName) + 4 + len(p.data)
			header := make([]byte, 5)
			
			// Use packet OpCode or default to Batch
			op := p.opCode
			if op == 0 { op = protocol.OpCodeReplBatch }
			header[0] = op
			
			binary.BigEndian.PutUint32(header[1:], uint32(totalLen))

			conn.SetWriteDeadline(time.Time{})
			if _, err := conn.Write(header); err != nil {
				s.logger.Error("Failed to write batch header", "replica", replicaID, "err", err)
				return
			}

			nameH := make([]byte, 4)
			binary.BigEndian.PutUint32(nameH, uint32(len(p.dbName)))
			if _, err := conn.Write(nameH); err != nil {
				s.logger.Error("Failed to write batch db name len", "replica", replicaID, "err", err)
				return
			}
			if _, err := conn.Write([]byte(p.dbName)); err != nil {
				s.logger.Error("Failed to write batch db name", "replica", replicaID, "err", err)
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

func (s *Server) streamDB(name string, st *store.Store, minLogID uint64, outCh chan<- replPacket, done <-chan struct{}) error {
	currentLogID := minLogID

	// 1. Check if WAL is available. If not, trigger Full Sync (Snapshot).
	// StoneDB's ScanWAL returns ErrLogUnavailable if the log is purged.
	// We perform a dummy check first.
	err := st.ScanWAL(currentLogID+1, func(entries []stonedb.ValueLogEntry) error { return nil })
	
	if err == stonedb.ErrLogUnavailable && currentLogID < st.LastOpID() {
		s.logger.Info("Replica too far behind (WAL purged). Starting Full Sync (Snapshot).", "db", name, "req_seq", currentLogID, "head", st.LastOpID())
		
		// --- SNAPSHOT PHASE ---
		// Updated to receive TxID + OpID
		snapTxID, snapOpID, snapErr := st.StreamSnapshot(func(batch []stonedb.SnapshotEntry) error {
			var buf bytes.Buffer
			
			// Serialize Snapshot Batch
			// Format matches WAL Batch roughly, but without TxIDs/OpIDs per entry
			// [KLen][Key][VLen][Val]...
			for _, e := range batch {
				binary.Write(&buf, binary.BigEndian, uint32(len(e.Key)))
				buf.Write(e.Key)
				binary.Write(&buf, binary.BigEndian, uint32(len(e.Value)))
				buf.Write(e.Value)
			}

			// Send Packet
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

		// Send DONE Signal with 16 bytes: [TxID(8)][OpID(8)]
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

		s.logger.Info("Full Sync complete. Switching to WAL streaming.", "db", name, "resume_seq", snapOpID)
		currentLogID = snapOpID
	}

	// 2. WAL Streaming Phase
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// Loop indefinitely.
	for {
		shouldWait := true

		var batchBuf bytes.Buffer
		var count uint32

		// Scan from next ID
		err := st.ScanWAL(currentLogID+1, func(entries []stonedb.ValueLogEntry) error {
			if len(entries) == 0 {
				return nil
			}

			lastEntry := entries[len(entries)-1]

			for _, e := range entries {
				// Wire Format: [LogID(8)][TxID(8)][Op(1)][KLen(4)][Key][VLen(4)][Val]
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

				vLen := uint32(len(e.Value))
				if err := binary.Write(&batchBuf, binary.BigEndian, vLen); err != nil {
					return err
				}
				batchBuf.Write(e.Value)

				count++
				if e.OperationID > currentLogID {
					currentLogID = e.OperationID
				}

				// Check batch size limit
				if batchBuf.Len() > MaxReplicationBatchSize {
					return ErrBatchFull
				}
			}

			// Append Commit Marker
			if err := binary.Write(&batchBuf, binary.BigEndian, lastEntry.OperationID); err != nil {
				return err
			}
			if err := binary.Write(&batchBuf, binary.BigEndian, lastEntry.TransactionID); err != nil {
				return err
			}
			batchBuf.WriteByte(protocol.OpJournalCommit)
			binary.Write(&batchBuf, binary.BigEndian, uint32(0)) // KeyLen
			binary.Write(&batchBuf, binary.BigEndian, uint32(0)) // ValLen
			count++

			return nil
		})

		// Send if we have data
		if count > 0 {
			select {
			case outCh <- replPacket{dbName: name, opCode: protocol.OpCodeReplBatch, data: batchBuf.Bytes(), count: count}:
			case <-done:
				return nil
			}
		}

		// Error Handling
		if err != nil {
			if err == ErrBatchFull {
				// We hit the limit, imply more data available. Do not wait.
				shouldWait = false
			} else if err == stonedb.ErrLogUnavailable {
				// Check if it's truly purged or just not written yet
				if currentLogID < st.LastOpID() {
					return fmt.Errorf("OUT_OF_SYNC: requested log %d is purged. Snapshot required.", currentLogID+1)
				}
				// If currentLogID >= LastOpID, it means we are at the head, so just wait.
			} else {
				s.logger.Error("ScanWAL error", "db", name, "err", err)
				time.Sleep(100 * time.Millisecond)
			}
		}

		if shouldWait {
			select {
			case <-done:
				return nil
			case <-ticker.C:
			}
		} else {
			select {
			case <-done:
				return nil
			default:
			}
		}
	}
}
