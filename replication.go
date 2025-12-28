package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"time"
)

// LogEntry represents a logical operation decoded from the WAL.
// This is the wire format unit for replication.
type LogEntry struct {
	LSN    uint64 // Logical Sequence Number.
	OpType uint8  // OpJournalSet or OpJournalDelete.
	Key    []byte
	Value  []byte
}

// --- REPLICATION LEADER LOGIC ---

// HandleReplicaConnection manages the lifecycle of a connected follower.
// It performs the handshake to determine the follower's position, then
// streams log entries.
func (s *Server) HandleReplicaConnection(conn net.Conn, payload []byte) {
	if len(payload) < 16 {
		s.logger.Error("Invalid REPL_HELLO payload size")
		return
	}

	// Extract the follower's last received LSN
	followerLSN := binary.BigEndian.Uint64(payload[8:16])

	replicaID := conn.RemoteAddr().String()
	s.store.RegisterReplica(replicaID, followerLSN)
	defer s.store.UnregisterReplica(replicaID)

	// Locate the approximate byte offset in the WAL for the requested LSN
	startOffset := s.store.FindOffsetForLSN(followerLSN)

	s.logger.Info("Replica connected", "id", replicaID, "follower_lsn", followerLSN, "leader_scan_start", startOffset)

	// Separate goroutine to consume ACKs from the follower so writes don't block
	go s.ReadReplicaAcks(conn, replicaID)

	// Main loop: stream data to follower
	s.StreamReplication(conn, replicaID, startOffset, followerLSN)
}

// ReadReplicaAcks continuously reads ACKs from the follower to update its progress.
// This is critical for synchronous replication (Quorum waits).
func (s *Server) ReadReplicaAcks(conn net.Conn, replicaID string) {
	headerBuf := make([]byte, 5)
	for {
		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			return
		}
		opCode := headerBuf[0]
		length := binary.BigEndian.Uint32(headerBuf[1:])

		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}

		if opCode == OpCodeReplAck && len(payload) >= 8 {
			ackLSN := binary.BigEndian.Uint64(payload[0:8])
			s.store.UpdateReplicaLSN(replicaID, ackLSN)
		}
	}
}

// StreamReplication reads the WAL from disk and pushes logical batches to the follower.
// It effectively acts as a "tail -f" on the WAL file.
func (s *Server) StreamReplication(conn net.Conn, replicaID string, startOffset int64, minLSN uint64) {
	currentOffset := startOffset
	buffer := make([]byte, 1024*1024)

	var pendingBatch []LogEntry
	var currentTxLSN uint64
	firstEntry := true

	for {
		walSize := s.store.wal.Size()
		if currentOffset >= walSize {
			// End of file reached. Flush any pending batch and wait for new data.
			if len(pendingBatch) > 0 {
				if err := s.sendBatch(conn, pendingBatch); err != nil {
					s.logger.Error("Failed to flush trailing batch", "err", err)
					return
				}
				pendingBatch = pendingBatch[:0]
				firstEntry = true
			}
			// Block until the WAL grows
			s.store.wal.Wait(currentOffset)
			walSize = s.store.wal.Size()
		}

		readSize := int64(len(buffer))
		if currentOffset+readSize > walSize {
			readSize = walSize - currentOffset
		}

		n, err := s.store.wal.ReadAt(buffer[:readSize], currentOffset)
		if err != nil && err != io.EOF {
			s.logger.Error("WAL read failed during replication", "err", err)
			return
		}
		if n == 0 {
			continue
		}

		parseOff := 0

		// Parse the WAL block
		for parseOff < n {
			if parseOff+4 > n {
				break
			}
			// Skip zero-padding (holes punched by compaction or pre-allocation)
			if buffer[parseOff] == 0 && buffer[parseOff+1] == 0 && buffer[parseOff+2] == 0 && buffer[parseOff+3] == 0 {
				aligned := (parseOff/4096 + 1) * 4096
				if aligned > n {
					break
				}
				parseOff = aligned
				continue
			}

			if parseOff+HeaderSize > n {
				break
			}

			packed := binary.BigEndian.Uint32(buffer[parseOff : parseOff+4])
			keyLen, valLen, isDel := UnpackMeta(packed)
			lsn := binary.BigEndian.Uint64(buffer[parseOff+4 : parseOff+12])
			storedCrc := binary.BigEndian.Uint32(buffer[parseOff+12 : parseOff+16])

			var payloadLen int
			if isDel {
				payloadLen = int(keyLen)
			} else {
				payloadLen = int(keyLen) + int(valLen)
			}

			if parseOff+HeaderSize+payloadLen > n {
				break
			}

			digest := crc32.New(crc32Table)
			digest.Write(buffer[parseOff : parseOff+12])
			digest.Write(buffer[parseOff+HeaderSize : parseOff+HeaderSize+payloadLen])
			if digest.Sum32() != storedCrc {
				s.logger.Error("CRC mismatch in replication stream", "offset", currentOffset+int64(parseOff))
				return
			}

			// Group entries by LSN (Transactions)
			if !firstEntry && lsn != currentTxLSN {
				if len(pendingBatch) > 0 {
					if err := s.sendBatch(conn, pendingBatch); err != nil {
						s.logger.Error("Failed to send batch", "err", err)
						return
					}
					pendingBatch = pendingBatch[:0]
				}
			}

			if firstEntry {
				currentTxLSN = lsn
				firstEntry = false
			}
			currentTxLSN = lsn

			// Only send entries newer than what the follower has
			if lsn > minLSN {
				kStart := parseOff + HeaderSize
				kEnd := kStart + int(keyLen)
				kCopy := make([]byte, int(keyLen))
				copy(kCopy, buffer[kStart:kEnd])

				var vCopy []byte
				if !isDel {
					vStart := kEnd
					vEnd := vStart + int(valLen)
					vCopy = make([]byte, int(valLen))
					copy(vCopy, buffer[vStart:vEnd])
				}

				pendingBatch = append(pendingBatch, LogEntry{
					LSN: lsn,
					OpType: func() uint8 {
						if isDel {
							return OpJournalDelete
						}
						return OpJournalSet
					}(),
					Key:   kCopy,
					Value: vCopy,
				})
			}

			parseOff += HeaderSize + payloadLen
		}

		currentOffset += int64(parseOff)
	}
}

// sendBatch serializes and writes a batch of log entries to the wire.
func (s *Server) sendBatch(conn net.Conn, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	var batchBuf bytes.Buffer
	for _, entry := range entries {
		binary.Write(&batchBuf, binary.BigEndian, entry.LSN)
		batchBuf.WriteByte(entry.OpType)

		kLen := uint32(len(entry.Key))
		binary.Write(&batchBuf, binary.BigEndian, kLen)
		batchBuf.Write(entry.Key)

		vLen := uint32(len(entry.Value))
		binary.Write(&batchBuf, binary.BigEndian, vLen)
		batchBuf.Write(entry.Value)
	}

	payload := make([]byte, 4+batchBuf.Len())
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(entries)))
	copy(payload[4:], batchBuf.Bytes())

	return s.writeBinaryResponse(conn, OpCodeReplBatch, payload)
}

// --- REPLICATION FOLLOWER LOGIC ---

// ReplicaClient handles the connection from a follower to a leader.
type ReplicaClient struct {
	addr     string
	store    *Store
	logger   *slog.Logger
	certFile string
	keyFile  string
	caFile   string
}

// StartReplicationClient initializes and runs the follower replication loop.
func StartReplicationClient(addr, cert, key, ca string, store *Store, logger *slog.Logger) {
	rc := &ReplicaClient{
		addr:     addr,
		store:    store,
		logger:   logger,
		certFile: cert,
		keyFile:  key,
		caFile:   ca,
	}
	go rc.run()
}

func (rc *ReplicaClient) run() {
	for {
		if err := rc.connectAndSync(); err != nil {
			rc.logger.Error("Replication sync failed", "err", err)
			time.Sleep(3 * time.Second) // Retry backoff
		}
	}
}

// connectAndSync establishes the mTLS connection and enters the protocol loop.
func (rc *ReplicaClient) connectAndSync() error {
	cert, err := timeoutLoadX509KeyPair(rc.certFile, rc.keyFile)
	if err != nil {
		return err
	}
	caCert, _ := os.ReadFile(rc.caFile)
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
	}

	conn, err := tls.Dial("tcp", rc.addr, tlsConfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	rc.logger.Info("Connected to Leader", "addr", rc.addr)

	// Send Hello Handshake
	stats := rc.store.Stats()
	helloPayload := make([]byte, 16)

	binary.BigEndian.PutUint64(helloPayload[0:8], 0) // Protocol Version (Reserved)

	lastLSN := uint64(0)
	if stats.NextLSN > 1 {
		lastLSN = stats.NextLSN - 1
	}
	binary.BigEndian.PutUint64(helloPayload[8:16], lastLSN)

	reqHeader := make([]byte, 5)
	reqHeader[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(reqHeader[1:], 16)

	conn.Write(reqHeader)
	conn.Write(helloPayload)

	// Enter Read Loop
	headerBuf := make([]byte, 5)
	for {
		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			return err
		}
		opCode := headerBuf[0]
		length := binary.BigEndian.Uint32(headerBuf[1:])

		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}

		if opCode == OpCodeReplBatch {
			if err := rc.applyBatch(conn, payload); err != nil {
				return err
			}
		} else if opCode == OpCodeReplAck {
			// Used for heartbeats/keepalives if implemented
		} else {
			rc.logger.Warn("Unknown replication opcode", "op", opCode)
		}
	}
}

// applyBatch deserializes the batch and applies it to the local store.
func (rc *ReplicaClient) applyBatch(conn net.Conn, payload []byte) error {
	count := binary.BigEndian.Uint32(payload[0:4])
	data := payload[4:]
	ptr := 0

	var ops []LogEntry
	var maxLSN uint64

	for i := 0; i < int(count); i++ {
		if ptr+13 > len(data) {
			return fmt.Errorf("short batch")
		}

		lsn := binary.BigEndian.Uint64(data[ptr : ptr+8])
		opType := data[ptr+8]
		kLen := binary.BigEndian.Uint32(data[ptr+9 : ptr+13])
		ptr += 13

		if ptr+int(kLen) > len(data) {
			return fmt.Errorf("short key")
		}
		key := data[ptr : ptr+int(kLen)]
		ptr += int(kLen)

		vLen := binary.BigEndian.Uint32(data[ptr : ptr+4])
		ptr += 4

		var val []byte
		if opType != OpJournalDelete {
			if ptr+int(vLen) > len(data) {
				return fmt.Errorf("short val")
			}
			val = data[ptr : ptr+int(vLen)]
			ptr += int(vLen)
		}

		kCopy := make([]byte, len(key))
		copy(kCopy, key)
		vCopy := make([]byte, len(val))
		copy(vCopy, val)

		ops = append(ops, LogEntry{LSN: lsn, OpType: opType, Key: kCopy, Value: vCopy})
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}

	// Apply batch to local store
	if err := rc.store.ReplicateBatch(ops); err != nil {
		return err
	}

	// Send ACK back to leader
	ackPayload := make([]byte, 8)
	binary.BigEndian.PutUint64(ackPayload, maxLSN)

	ackHeader := make([]byte, 5)
	ackHeader[0] = OpCodeReplAck
	binary.BigEndian.PutUint32(ackHeader[1:], 8)

	if _, err := conn.Write(ackHeader); err != nil {
		return err
	}
	if _, err := conn.Write(ackPayload); err != nil {
		return err
	}

	return nil
}

func timeoutLoadX509KeyPair(certFile, keyFile string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certFile, keyFile)
}
