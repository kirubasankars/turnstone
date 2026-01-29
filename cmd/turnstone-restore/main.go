package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"turnstone/stonedb"
)

var (
	inDir      = flag.String("in", "backup_data", "Input directory containing backup artifacts")
	outDir     = flag.String("out", "restored_data", "Target directory for restored database")
	backupFile = flag.String("file", "basebackup.bin", "Backup filename (will auto-detect .gz)")
	verify     = flag.Bool("verify", true, "Verify SHA256 checksum of backup before restoring")
)

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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := runRestore(ctx); err != nil {
		log.Fatalf("Restore failed: %v", err)
	}
}

func runRestore(ctx context.Context) error {
	// 1. Resolve Files
	bkPath := filepath.Join(*inDir, *backupFile)
	if _, err := os.Stat(bkPath); os.IsNotExist(err) {
		if _, err := os.Stat(bkPath + ".gz"); err == nil {
			bkPath += ".gz"
		} else {
			return fmt.Errorf("backup file not found: %s", bkPath)
		}
	}

	metaPath := filepath.Join(*inDir, "backup.meta")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return fmt.Errorf("meta file not found: %s", metaPath)
	}

	// 2. Load Metadata
	metaBytes, _ := os.ReadFile(metaPath)
	var meta BackupMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return fmt.Errorf("invalid meta JSON: %w", err)
	}

	// 3. Prepare Target
	if _, err := os.Stat(*outDir); err == nil {
		return fmt.Errorf("target directory %s already exists", *outDir)
	}
	fmt.Printf("Restoring from: %s\n", bkPath)
	fmt.Printf("Restoring to:   %s\n", *outDir)

	dbPath := filepath.Join(*outDir, "data", meta.Database)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create DB structure: %w", err)
	}

	// 4. Initialize StoneDB State
	tlMeta := stonedb.TimelineMeta{
		CurrentTimeline: meta.CurrentTimeline,
		History:         []stonedb.TimelineHistoryItem{}, // Fresh history
	}
	tlBytes, _ := json.MarshalIndent(tlMeta, "", "  ")
	if err := os.WriteFile(filepath.Join(dbPath, "timeline.meta"), tlBytes, 0644); err != nil {
		return fmt.Errorf("write timeline.meta: %w", err)
	}

	opts := stonedb.Options{
		MaxVLogSize:          200 * 1024 * 1024,
		CompactionMinGarbage: 10 * 1024 * 1024,
		BlockCacheSize:       64 * 1024 * 1024,
	}

	log.Println("Initializing storage engine...")
	db, err := stonedb.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to open StoneDB: %w", err)
	}
	defer db.Close()

	// 5. Setup Read Pipeline
	f, err := os.Open(bkPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var inputReader io.Reader = f
	var hasher hash.Hash

	if *verify {
		hasher = sha256.New()
		inputReader = io.TeeReader(f, hasher)
	}

	// Check Compression (Peek Magic Bytes)
	bufReader := bufio.NewReader(inputReader)
	peek, _ := bufReader.Peek(2)
	isGzip := len(peek) == 2 && peek[0] == 0x1f && peek[1] == 0x8b

	if isGzip {
		log.Println("Detected GZIP compression.")
		gzR, err := gzip.NewReader(bufReader)
		if err != nil {
			return fmt.Errorf("gzip reader failed: %w", err)
		}
		defer gzR.Close()
		inputReader = gzR
	} else {
		inputReader = bufReader
	}

	// 6. Ingest Loop
	log.Println("Ingesting data...")
	var batch []stonedb.ValueLogEntry
	count := 0
	restoreSeq := meta.SnapshotOpID
	restoreTx := meta.SnapshotTxID
	batchSize := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(inputReader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read error: %w", err)
		}
		kLen := int(binary.BigEndian.Uint32(lenBuf))

		key := make([]byte, kLen)
		if _, err := io.ReadFull(inputReader, key); err != nil {
			return fmt.Errorf("read key error: %w", err)
		}

		if _, err := io.ReadFull(inputReader, lenBuf); err != nil {
			return fmt.Errorf("read vlen error: %w", err)
		}
		vLen := int(binary.BigEndian.Uint32(lenBuf))

		val := make([]byte, vLen)
		if _, err := io.ReadFull(inputReader, val); err != nil {
			return fmt.Errorf("read val error: %w", err)
		}

		typeBuf := make([]byte, 1)
		if _, err := io.ReadFull(inputReader, typeBuf); err != nil {
			return fmt.Errorf("read type error: %w", err)
		}
		isDelete := typeBuf[0] == 1

		batch = append(batch, stonedb.ValueLogEntry{
			Key:           key,
			Value:         val,
			TransactionID: restoreTx,
			OperationID:   restoreSeq,
			IsDelete:      isDelete,
		})
		batchSize += kLen + vLen
		count++

		if batchSize > 4*1024*1024 {
			if err := db.ApplyBatch(batch); err != nil {
				return fmt.Errorf("batch apply: %w", err)
			}
			fmt.Printf("\rRestored items: %d", count)
			batch = batch[:0]
			batchSize = 0
		}
	}

	if len(batch) > 0 {
		if err := db.ApplyBatch(batch); err != nil {
			return fmt.Errorf("final apply: %w", err)
		}
	}

	// 7. Verify Checksum
	if *verify {
		log.Println("\nVerifying checksum...")
		calculated := hex.EncodeToString(hasher.Sum(nil))
		if calculated != meta.SHA256 {
			return fmt.Errorf("CHECKSUM MISMATCH!\nBackup file is corrupt.\nExpected: %s\nGot:      %s", meta.SHA256, calculated)
		}
		log.Println("Checksum OK.")
	}

	// 8. Finalize
	db.ForceSetClocks(restoreTx, restoreSeq)
	log.Println("Finalizing checkpoint...")
	if err := db.Checkpoint(); err != nil {
		return err
	}

	fmt.Printf("\nRestore Complete.\nDatabase at: %s\nLast OpID: %d\n", dbPath, restoreSeq)
	return nil
}
