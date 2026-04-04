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
	MaxReplicationBatchSize = 1 * 1024 * 1024 // 1MB
	ErrBatchFull            = errors.New("replication batch full")
)

type replPacket struct {
	dbName string
	opCode uint8 
	data   []byte
	count  uint32
}

func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte, role string, certClientID string) {
	// CRITICAL FIX: Clear the Read/Write deadlines inherited from the main server loop.
	// Replication is a long-lived stream; we must not timeout idle or slow connections.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		s.logger.Error("Failed to clear deadlines", "err", err)
		return
	}

	// 1. Parse Hello: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	if len(payload) < 8 {
		s.logger.Error("HandleReplicaConnection: payload too short for header")
		return
	}
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

	outCh := make(chan replPacket, 10)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup

	for _, req := range subs {
		if st, ok := s.stores[req.name]; ok {
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
			}(req.name, st, req.logID)
		}
	}

	// ACK Reader
	go func() {
		h := make([]byte, 5)
		for {
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
			s.logger.Warn("Dropping failed replica connection", "addr", replicaID, "err", err)
			return
		case p := <-outCh:
			// Frame: [OpCode][TotalLen] [DBNameLen][DBName][Count/Reserved][Data...]
			totalLen := 4 + len(p.dbName) + 4 + len(p.data)
			header := make([]byte, 5)
			header[0] = p.opCode
			binary.BigEndian.PutUint32(header[1:], uint32(totalLen))
			
			// Write strictly without Deadlines to allow slow consumers
			if _, err := conn.Write(header); err != nil {
				return
			}
			
			nameH := make([]byte, 4)
			binary.BigEndian.PutUint32(nameH, uint32(len(p.dbName)))
			if _, err := conn.Write(nameH); err != nil {
				return
			}
			if _, err := conn.Write([]byte(p.dbName)); err != nil {
				return
			}
			
			cntH := make([]byte, 4)
			binary.BigEndian.PutUint32(cntH, p.count)
			if _, err := conn.Write(cntH); err != nil {
				return
			}
			
			if _, err := conn.Write(p.data); err != nil {
				return
			}
		}
	}
}

func (s *Server) streamDB(name string, st *store.Store, minLogID uint64, outCh chan<- replPacket, done <-chan struct{}) error {
	currentLogID := minLogID

	// 1. Snapshot Check
	err := st.ScanWAL(currentLogID+1, func([]stonedb.ValueLogEntry) error { return nil })
	
	if err == stonedb.ErrLogUnavailable && currentLogID < st.LastOpID() {
		s.logger.Info("Replica too far behind (WAL purged). Starting Full Sync (Snapshot).", "db", name, "req_seq", currentLogID, "head", st.LastOpID())
		
		snapTxID, snapOpID, snapErr := st.StreamSnapshot(func(batch []stonedb.SnapshotEntry) error {
			var buf bytes.Buffer
			for _, e := range batch {
				binary.Write(&buf, binary.BigEndian, uint32(len(e.Key)))
				buf.Write(e.Key)
				binary.Write(&buf, binary.BigEndian, uint32(len(e.Value)))
				buf.Write(e.Value)
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
		s.logger.Info("Full Sync complete. Switching to WAL streaming.", "db", name, "resume_seq", snapOpID)
		currentLogID = snapOpID
	}

	// 2. WAL Streaming & SafePoint Broadcast
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	
	safePointTicker := time.NewTicker(1 * time.Second)
	defer safePointTicker.Stop()

	for {
		select {
		case <-done:
			return nil
		
		case <-safePointTicker.C:
			// Broadcast Safe Point
			minSeq := st.GetMinSlotLogSeq()
			if minSeq > 0 {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, minSeq)
				
				select {
				case outCh <- replPacket{
					dbName: name,
					opCode: protocol.OpCodeReplSafePoint,
					data:   buf,
					count:  0, // Ignored
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
						return fmt.Errorf("OUT_OF_SYNC: requested log %d is purged", currentLogID+1)
					}
				} else {
					s.logger.Error("ScanWAL error", "db", name, "err", err)
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}
