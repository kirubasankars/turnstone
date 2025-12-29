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
// IMPORTANT: It takes an io.Reader (the bufio.Reader) to ensure we consume
// any buffered ACKs from the previous handshake phase.
func (s *Server) HandleReplicaConnection(conn net.Conn, r io.Reader, payload []byte) {
	if len(payload) < 16 {
		s.logger.Error("Invalid REPL_HELLO payload size")
		return
	}

	// Extract the follower's last received LSN
	followerLSN := binary.BigEndian.Uint64(payload[8:16])

	replicaID := conn.RemoteAddr().String()
	s.store.RegisterReplica(replicaID, followerLSN)
	defer s.store.UnregisterReplica(replicaID)

	// Clear the read deadline to prevent timeouts during long streaming sessions.
	// The ACKs act as heartbeats.
	conn.SetReadDeadline(time.Time{})
	// Clear the write deadline to prevent the initial handshake deadline from
	// killing the long-lived replication stream.
	conn.SetWriteDeadline(time.Time{})

	// Locate the approximate byte offset in the WAL for the requested LSN
	startOffset := s.store.FindOffsetForLSN(followerLSN)

	// Zero cost: Removed the info log to avoid overhead during connections
	// s.logger.Info("Replica connected", "id", replicaID, "follower_lsn", followerLSN, "leader_scan_start", startOffset)

	// Separate goroutine to consume ACKs from the follower so writes don't block.
	// We MUST use the bufio.Reader `r` passed from handleConnection.
	go s.ReadReplicaAcks(r, replicaID)

	// Main loop: stream data to follower (writes to conn)
	s.StreamReplication(conn, replicaID, startOffset, followerLSN)
}

// ReadReplicaAcks continuously reads ACKs from the follower to update its progress.
// This is critical for synchronous replication (Quorum waits).
func (s *Server) ReadReplicaAcks(r io.Reader, replicaID string) {
	headerBuf := make([]byte, 5)
	for {
		if _, err := io.ReadFull(r, headerBuf); err != nil {
			return
		}
		opCode := headerBuf[0]
		length := binary.BigEndian.Uint32(headerBuf[1:])

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
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
	readBuffer := make([]byte, 1024*1024)

	// Optimization: Reuse a write buffer to avoid per-entry allocations.
	// This drastically reduces GC pressure on the leader.
	batchBuf := new(bytes.Buffer)
	batchBuf.Grow(512 * 1024) // Pre-allocate 512KB
	var batchCount uint32

	// Scratch buffer for encoding integers
	scratch := make([]byte, 8)

	const MaxBatchSize = 512 * 1024 // 512KB batch limit

	for {
		walSize := s.store.wal.Size()
		if currentOffset >= walSize {
			// End of file reached. Flush any pending batch and wait for new data.
			if batchCount > 0 {
				if err := s.sendRawBatch(conn, batchBuf, batchCount); err != nil {
					s.logger.Error("Failed to flush trailing batch", "err", err)
					return
				}
				batchBuf.Reset()
				batchCount = 0
			}
			// Block until the WAL grows
			s.store.wal.Wait(currentOffset)
			walSize = s.store.wal.Size()
		}

		readSize := int64(len(readBuffer))
		if currentOffset+readSize > walSize {
			readSize = walSize - currentOffset
		}

		n, err := s.store.wal.ReadAt(readBuffer[:readSize], currentOffset)
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
			if readBuffer[parseOff] == 0 && readBuffer[parseOff+1] == 0 && readBuffer[parseOff+2] == 0 && readBuffer[parseOff+3] == 0 {
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

			packed := binary.BigEndian.Uint32(readBuffer[parseOff : parseOff+4])
			keyLen, valLen, isDel := UnpackMeta(packed)
			lsn := binary.BigEndian.Uint64(readBuffer[parseOff+4 : parseOff+12])
			storedCrc := binary.BigEndian.Uint32(readBuffer[parseOff+12 : parseOff+16])

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
			digest.Write(readBuffer[parseOff : parseOff+12])
			digest.Write(readBuffer[parseOff+HeaderSize : parseOff+HeaderSize+payloadLen])
			if digest.Sum32() != storedCrc {
				s.logger.Error("CRC mismatch in replication stream", "offset", currentOffset+int64(parseOff))
				return
			}

			// Only send entries newer than what the follower has
			if lsn > minLSN {
				kStart := parseOff + HeaderSize
				kEnd := kStart + int(keyLen)

				// --- Serialize directly to batchBuf ---

				// 1. LSN (8 bytes)
				binary.BigEndian.PutUint64(scratch, lsn)
				batchBuf.Write(scratch)

				// 2. OpType (1 byte)
				if isDel {
					batchBuf.WriteByte(OpJournalDelete)
				} else {
					batchBuf.WriteByte(OpJournalSet)
				}

				// 3. Key Len (4 bytes)
				binary.BigEndian.PutUint32(scratch[:4], uint32(keyLen))
				batchBuf.Write(scratch[:4])

				// 4. Key (keyLen bytes)
				batchBuf.Write(readBuffer[kStart:kEnd])

				// 5. Value Len (4 bytes)
				binary.BigEndian.PutUint32(scratch[:4], uint32(valLen))
				batchBuf.Write(scratch[:4])

				// 6. Value (valLen bytes)
				if !isDel {
					vStart := kEnd
					vEnd := vStart + int(valLen)
					batchBuf.Write(readBuffer[vStart:vEnd])
				}

				batchCount++

				// Flush if batch is large enough
				if batchBuf.Len() >= MaxBatchSize {
					if err := s.sendRawBatch(conn, batchBuf, batchCount); err != nil {
						s.logger.Error("Failed to send batch", "err", err)
						return
					}
					batchBuf.Reset()
					batchCount = 0
				}
			}

			parseOff += HeaderSize + payloadLen
		}

		currentOffset += int64(parseOff)
	}
}

// sendRawBatch writes the pre-serialized batch buffer to the connection.
func (s *Server) sendRawBatch(conn net.Conn, buf *bytes.Buffer, count uint32) error {
	if count == 0 {
		return nil
	}

	// Update the write deadline for this specific batch.
	if err := conn.SetWriteDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return err
	}

	// Calculate total payload length:
	// 4 bytes (Batch Count) + Batch Data Length
	payloadLen := 4 + buf.Len()

	// 1. Write Header: [OpCodeReplBatch][PayloadLen]
	header := make([]byte, 5)
	header[0] = OpCodeReplBatch
	binary.BigEndian.PutUint32(header[1:], uint32(payloadLen))

	if _, err := conn.Write(header); err != nil {
		return err
	}

	// 2. Write Batch Count
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, count)
	if _, err := conn.Write(countBuf); err != nil {
		return err
	}

	// 3. Write Batch Data
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
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
