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
	ReplicaSendTimeout = 5 * time.Second
	// MaxReplicationBatchSize limits the payload size of a single replication batch
	// to prevent memory spikes and ensure keep-alives/ACKs flow regularly.
	MaxReplicationBatchSize = 1 * 1024 * 1024 // 1MB
	ErrBatchFull            = errors.New("replication batch full")
)

type replPacket struct {
	partitionName string
	data          []byte
	count         uint32
}

// HandleReplicaConnection multiplexes streams from multiple partitions onto one connection.
func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte) {
	// 1. Parse Hello: [Ver:4][NumPartitions:4] ... [NameLen:4][Name][LogID:8]
	if len(payload) < 8 {
		s.logger.Error("HandleReplicaConnection: payload too short for header")
		return
	}
	// ver := binary.BigEndian.Uint32(payload[0:4])
	count := binary.BigEndian.Uint32(payload[4:8])

	cursor := 8
	type subReq struct {
		name  string
		logID uint64
	}
	var subs []subReq
	replicaID := conn.RemoteAddr().String()

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
			s.logger.Info("Replica subscribed", "replica", replicaID, "partition", name, "startLogID", logID)
			st.RegisterReplica(replicaID, logID)

			// Clean up when connection closes.
			// We capture 'st' and 'name' for the specific partition.
			defer func(storePtr *store.Store, pName string) {
				s.logger.Info("Replica unsubscribed (connection closed)", "replica", replicaID, "partition", pName)
				storePtr.UnregisterReplica(replicaID)
			}(st, name)
		} else {
			s.logger.Warn("Replica requested unknown partition", "replica", replicaID, "partition", name)
		}
	}

	// 2. Start Multiplexer
	outCh := make(chan replPacket, 10)
	errCh := make(chan error, 1) // Signal channel for slow/failed streams
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup

	// Start reader for each requested Partition
	for _, req := range subs {
		storeInstance, ok := s.stores[req.name]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(name string, st *store.Store, startLogID uint64) {
			defer wg.Done()
			if err := s.streamPartition(name, st, startLogID, outCh, done); err != nil {
				s.logger.Error("StreamPartition failed", "partition", name, "err", err)
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
		// Format: [OpCode][Len][PartitionNameLen][PartitionName][LogID]
		h := make([]byte, 5)
		for {
			if _, err := io.ReadFull(r, h); err != nil {
				if err != io.EOF {
					s.logger.Error("Replica ACK reader failed", "replica", replicaID, "err", err)
				}
				// CRITICAL: Signal main loop to exit if reader fails or disconnects
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
					// Signal main loop
					select {
					case errCh <- err:
					default:
					}
					return
				}

				if len(b) > 4 {
					nL := binary.BigEndian.Uint32(b[:4])
					if len(b) >= 4+int(nL)+8 {
						partitionName := string(b[4 : 4+nL])
						logID := binary.BigEndian.Uint64(b[4+nL:])
						if st, ok := s.stores[partitionName]; ok {
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
				// Client disconnected gracefully
				return
			}
			s.logger.Warn("Dropping slow/failed replica", "addr", replicaID, "err", err)

			// Attempt to send error to client before closing
			// This helps the client distinguish between network failure and fatal replication errors (e.g. purged logs)
			errMsg := err.Error()
			errHeader := make([]byte, 5)
			errHeader[0] = protocol.ResStatusErr
			binary.BigEndian.PutUint32(errHeader[1:], uint32(len(errMsg)))

			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			conn.Write(errHeader)
			conn.Write([]byte(errMsg))

			return

		case p := <-outCh:
			// Frame: [OpCodeReplBatch][TotalLen] [PartitionNameLen][PartitionName][Count][BatchData...]
			totalLen := 4 + len(p.partitionName) + 4 + len(p.data)
			header := make([]byte, 5)
			header[0] = protocol.OpCodeReplBatch
			binary.BigEndian.PutUint32(header[1:], uint32(totalLen))

			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(header); err != nil {
				s.logger.Error("Failed to write batch header", "replica", replicaID, "err", err)
				return
			}

			nameH := make([]byte, 4)
			binary.BigEndian.PutUint32(nameH, uint32(len(p.partitionName)))
			if _, err := conn.Write(nameH); err != nil {
				s.logger.Error("Failed to write batch partition name len", "replica", replicaID, "err", err)
				return
			}
			if _, err := conn.Write([]byte(p.partitionName)); err != nil {
				s.logger.Error("Failed to write batch partition name", "replica", replicaID, "err", err)
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

func (s *Server) streamPartition(name string, st *store.Store, minLogID uint64, outCh chan<- replPacket, done <-chan struct{}) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	currentLogID := minLogID

	// Loop indefinitely. We use 'shouldWait' to determine if we block on the ticker or loop immediately.
	for {
		shouldWait := true

		var batchBuf bytes.Buffer
		var count uint32

		// Scan from next ID
		err := st.ScanWAL(currentLogID+1, func(entries []stonedb.ValueLogEntry) error {
			for _, e := range entries {
				// Wire Format: [LogID(8)][LSN(8)][Op(1)][KLen(4)][Key][VLen(4)][Val]
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
			return nil
		})

		// Send if we have data
		if count > 0 {
			select {
			case outCh <- replPacket{partitionName: name, data: batchBuf.Bytes(), count: count}:
			case <-done:
				return nil
			case <-time.After(ReplicaSendTimeout):
				return fmt.Errorf("replication send timeout for partition %s", name)
			}
		}

		// Error Handling
		if err != nil {
			if err == ErrBatchFull {
				// We hit the limit, implying more data might be available.
				// Do not wait for ticker; immediate loop.
				shouldWait = false
			} else if err == stonedb.ErrLogUnavailable {
				// If the requested log ID is older than the last committed ID on the server,
				// and the log is unavailable, it means the data has been purged.
				// We must error out to prevent the client from waiting indefinitely.
				if currentLogID < st.LastOpID() {
					return fmt.Errorf("OUT_OF_SYNC: requested log %d is purged (server head: %d). Snapshot required.", currentLogID+1, st.LastOpID())
				}
				// Otherwise, we are at the head (future data), so we just wait for new data.
			} else {
				s.logger.Error("ScanWAL error", "partition", name, "err", err)
				// Backoff slightly on error
				time.Sleep(100 * time.Millisecond)
			}
		}

		// Wait logic
		if shouldWait {
			select {
			case <-done:
				return nil
			case <-ticker.C:
				// Ready to scan again
			}
		} else {
			// Check done non-blocking to ensure we can exit even during high load
			select {
			case <-done:
				return nil
			default:
				// Loop immediately
			}
		}
	}
}
