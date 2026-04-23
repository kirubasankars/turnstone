package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"turnstone/protocol"
)

// --- Configuration ---

var (
	host       = flag.String("host", "localhost:6379", "Server address")
	dbName     = flag.String("db", "1", "Database name to backup")
	home       = flag.String("home", ".", "Path to home directory containing certs")
	outDir     = flag.String("out", "backup_data", "Output directory for backup artifacts")
	backupFile = flag.String("file", "basebackup.bin", "Backup filename (will append .gz if compressed)")
	compress   = flag.Bool("compress", true, "Enable GZIP compression")
	wait       = flag.Duration("wait", 2*time.Second, "Idle time to wait for new WAL entries before finishing")
)

// --- Data Structures ---

type BackupMeta struct {
	Timestamp       time.Time `json:"timestamp"`
	Database        string    `json:"database"`
	CurrentTimeline uint64    `json:"current_timeline"`
	SnapshotTxID    uint64    `json:"snapshot_tx_id"`
	SnapshotOpID    uint64    `json:"snapshot_op_id"`
	Compressed      bool      `json:"compressed"`
	SHA256          string    `json:"sha256"`
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Setup context for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runBackup(ctx); err != nil {
		// If the error is context canceled, it was a manual stop, don't exit with 1
		if ctx.Err() != nil {
			log.Println("Backup stopped by user.")
			return
		}
		log.Fatalf("Backup failed: %v", err)
	}
}

func runBackup(ctx context.Context) error {
	log.Printf("Starting Backup from %s [%s]...", *host, *dbName)

	// 1. Setup TLS
	tlsConf, err := loadTLS()
	if err != nil {
		return fmt.Errorf("load TLS: %w", err)
	}

	// 2. Connect
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(&dialer, "tcp", *host, tlsConf)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	// Manage connection lifecycle
	done := make(chan struct{})
	defer close(done)

	// CLEAN SHUTDOWN HANDLER
	go func() {
		select {
		case <-ctx.Done():
			// Attempt to send QUIT packet to server
			conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
			sendQuit(conn)
			time.Sleep(10 * time.Millisecond)
			conn.Close()
		case <-done:
			// Normal exit
		}
	}()
	defer conn.Close()

	// 3. Prepare Output
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	finalFilename := *backupFile
	if *compress && filepath.Ext(finalFilename) != ".gz" {
		finalFilename += ".gz"
	}
	fPath := filepath.Join(*outDir, finalFilename)

	f, err := os.OpenFile(fPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create backup file: %w", err)
	}
	defer f.Close()

	// Clean up partial file on error/cancel
	defer func() {
		if ctx.Err() != nil {
			f.Close()
			os.Remove(fPath)
			log.Println("Backup cancelled, removed partial file.")
		}
	}()

	// 4. Send Hello
	if err := sendHello(conn); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}

	// 5. Setup Writer Chain
	var outputWriter io.Writer
	fileHasher := sha256.New()
	diskWriter := io.MultiWriter(f, fileHasher)

	var gzipW *gzip.Writer
	if *compress {
		gzipW = gzip.NewWriter(diskWriter)
		outputWriter = gzipW
	} else {
		outputWriter = diskWriter
	}

	// 6. Stream Loop
	reader := bufio.NewReader(conn)
	respHeader := make([]byte, 5)
	meta := BackupMeta{
		Timestamp:  time.Now(),
		Database:   *dbName,
		Compressed: *compress,
	}

	count := 0
	start := time.Now()

	// TIMEOUT STRATEGY:
	// We only extend the deadline when we receive actual DATA (Snapshot or Batch).
	// Metadata (Timeline, SafePoint) does not extend the deadline.
	// This ensures that if the server has nothing to send (empty/idle DB), we timeout and finish.
	lastDataTime := time.Now()

	log.Println("Downloading stream...")

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Always enforce deadline based on last significant data activity
		deadline := lastDataTime.Add(*wait)
		conn.SetReadDeadline(deadline)

		if _, err := io.ReadFull(reader, respHeader); err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				log.Printf("Stream idle for %v (Data caught up), finishing backup.", *wait)
				break
			}
			return fmt.Errorf("read header: %w", err)
		}

		opCode := respHeader[0]
		length := binary.BigEndian.Uint32(respHeader[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return fmt.Errorf("read payload: %w", err)
		}

		// Verify CRC for data-heavy packets
		if opCode == protocol.OpCodeReplSnapshot || opCode == protocol.OpCodeReplSnapshotDone || 
		   opCode == protocol.OpCodeReplTimeline || opCode == protocol.OpCodeReplBatch || 
		   opCode == protocol.OpCodeReplSafePoint {
			if len(payload) < 4 {
				return fmt.Errorf("payload too short for CRC")
			}
			crcReceived := binary.BigEndian.Uint32(payload[:4])
			rawBody := payload[4:]
			if crc32.Checksum(rawBody, protocol.Crc32Table) != crcReceived {
				return fmt.Errorf("CRC Mismatch on stream")
			}
			payload = rawBody
		}

		// --- Process Packets ---

		if opCode == protocol.OpCodeReplTimeline {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen + 4
			if cursor+8 <= len(payload) {
				meta.CurrentTimeline = binary.BigEndian.Uint64(payload[cursor:])
				log.Printf("Detected Timeline: %d", meta.CurrentTimeline)
			}
			// Timeline is metadata, DO NOT update lastDataTime (allow timeout if empty DB)

		} else if opCode == protocol.OpCodeReplSafePoint {
			// Heartbeat from server.
			// DO NOT update lastDataTime. If we only get heartbeats, we are caught up.

		} else if opCode == protocol.OpCodeReplSnapshot {
			lastDataTime = time.Now() // Valid Data -> Reset Timeout
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen + 4
			
			snapshotData := bytes.NewReader(payload[cursor:])
			for snapshotData.Len() > 0 {
				if err := writeEntry(snapshotData, outputWriter, 0); err != nil {
					return err
				}
				count++
			}
			if count%1000 == 0 {
				fmt.Printf("\rProcessed items: %d", count)
			}

		} else if opCode == protocol.OpCodeReplSnapshotDone {
			lastDataTime = time.Now() // Valid Data -> Reset Timeout
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen + 4
			if cursor+16 <= len(payload) {
				meta.SnapshotTxID = binary.BigEndian.Uint64(payload[cursor:])
				meta.SnapshotOpID = binary.BigEndian.Uint64(payload[cursor+8:])
			}
			log.Printf("\nSnapshot Complete. TxID: %d, OpID: %d", meta.SnapshotTxID, meta.SnapshotOpID)

		} else if opCode == protocol.OpCodeReplBatch {
			lastDataTime = time.Now() // Valid Data -> Reset Timeout
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen
			
			if cursor+4 > len(payload) { return fmt.Errorf("bad batch count") }
			entryCount := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			
			for i := 0; i < int(entryCount); i++ {
				if cursor+17 > len(payload) { return fmt.Errorf("bad entry header") }
				opID := binary.BigEndian.Uint64(payload[cursor:])
				txID := binary.BigEndian.Uint64(payload[cursor+8:])
				opType := payload[cursor+16]
				cursor += 17
				
				meta.SnapshotTxID = txID 
				meta.SnapshotOpID = opID 

				// Key
				if cursor+4 > len(payload) { return fmt.Errorf("bad entry klen") }
				kLen := binary.BigEndian.Uint32(payload[cursor:])
				cursor += 4
				if cursor+int(kLen) > len(payload) { return fmt.Errorf("bad entry key") }
				key := payload[cursor:cursor+int(kLen)]
				cursor += int(kLen)
				
				// Value
				if cursor+4 > len(payload) { return fmt.Errorf("bad entry vlen") }
				vLen := binary.BigEndian.Uint32(payload[cursor:])
				cursor += 4
				if cursor+int(vLen) > len(payload) { return fmt.Errorf("bad entry val") }
				val := payload[cursor:cursor+int(vLen)]
				cursor += int(vLen)
				
				if opType == protocol.OpJournalCommit {
					continue
				}

				storageType := byte(0)
				if opType == protocol.OpJournalDelete {
					storageType = 1
				}
				
				kLenBuf := make([]byte, 4)
				binary.BigEndian.PutUint32(kLenBuf, kLen)
				vLenBuf := make([]byte, 4)
				binary.BigEndian.PutUint32(vLenBuf, vLen)
				
				outputWriter.Write(kLenBuf)
				outputWriter.Write(key)
				outputWriter.Write(vLenBuf)
				outputWriter.Write(val)
				outputWriter.Write([]byte{storageType})
				
				count++
			}
			if count%1000 == 0 {
				fmt.Printf("\rProcessed items: %d", count)
			}

		} else if opCode == protocol.ResStatusErr {
			return fmt.Errorf("server error: %s", string(payload))
		}
	}

	if *compress && gzipW != nil {
		if err := gzipW.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
	}
	
	meta.SHA256 = hex.EncodeToString(fileHasher.Sum(nil))

	metaBytes, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(*outDir, "backup.meta"), metaBytes, 0644); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	log.Printf("\nBackup successful in %v!", time.Since(start))
	log.Printf("Location: %s", fPath)
	log.Printf("Checksum: %s", meta.SHA256)
	
	sendQuit(conn)
	return nil
}

// Helper to read fields from snapshot stream and write to disk
func writeEntry(r io.Reader, w io.Writer, typeByte byte) error {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil { return err }
	kLen := binary.BigEndian.Uint32(lenBuf)
	
	key := make([]byte, kLen)
	if _, err := io.ReadFull(r, key); err != nil { return err }

	if _, err := io.ReadFull(r, lenBuf); err != nil { return err }
	vLen := binary.BigEndian.Uint32(lenBuf)

	val := make([]byte, vLen)
	if _, err := io.ReadFull(r, val); err != nil { return err }

	w.Write(makeKLen(kLen))
	w.Write(key)
	w.Write(makeVLen(vLen))
	w.Write(val)
	w.Write([]byte{typeByte})
	return nil
}

func makeKLen(l uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, l)
	return b
}
func makeVLen(l uint32) []byte {
	return makeKLen(l)
}

func sendHello(conn net.Conn) error {
	id := "turnstone-backup-tool"
	startSeq := uint64(0)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(id)))
	buf.WriteString(id)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(*dbName)))
	buf.WriteString(*dbName)
	binary.Write(buf, binary.BigEndian, startSeq)

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(buf.Bytes())
	return err
}

func sendQuit(conn net.Conn) {
	header := make([]byte, 5)
	header[0] = protocol.OpCodeQuit
	binary.BigEndian.PutUint32(header[1:], 0)
	conn.Write(header)
}

func loadTLS() (*tls.Config, error) {
	cFile := filepath.Join(*home, "certs", "admin.crt")
	kFile := filepath.Join(*home, "certs", "admin.key")
	caFileResolved := filepath.Join(*home, "certs", "ca.crt")

	cert, err := tls.LoadX509KeyPair(cFile, kFile)
	if err != nil {
		return nil, fmt.Errorf("cert load failed (%s): %w", cFile, err)
	}
	caCert, err := os.ReadFile(caFileResolved)
	if err != nil {
		return nil, fmt.Errorf("ca load failed: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}
