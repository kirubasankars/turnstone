// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

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

	// 3. Verify Checksum BEFORE touching the target directory or committing
	// anything. Previously this tool ingested every batch straight into the
	// live target DB while streaming the backup file, and only checked the
	// SHA256 *after* everything had already been committed -- so a
	// checksum mismatch was reported as a fatal error only after the
	// "restored" database had already been fully (or partially) written
	// with corrupt data, directly contradicting --verify's documented
	// "before restoring" contract. Hashing the raw backup file is cheap
	// (single sequential read, no decompression/parsing needed since the
	// checksum covers the on-disk bytes exactly as turnstone-backup wrote
	// them), so do it as a standalone pass first.
	if *verify {
		log.Println("Verifying backup checksum before restoring...")
		if err := verifyBackupChecksum(bkPath, meta.SHA256); err != nil {
			return err
		}
		log.Println("Checksum OK.")
	}

	// 4. Prepare Target
	if _, err := os.Stat(*outDir); err == nil {
		return fmt.Errorf("target directory %s already exists", *outDir)
	}
	fmt.Printf("Restoring from: %s\n", bkPath)
	fmt.Printf("Restoring to:   %s\n", *outDir)

	dbPath := filepath.Join(*outDir, "data", meta.Database)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create DB structure: %w", err)
	}

	// 5. Initialize StoneDB State
	tlMeta := stonedb.TimelineMeta{
		CurrentTimeline: meta.CurrentTimeline,
		History:         []stonedb.TimelineHistoryItem{}, // Fresh history
	}
	tlBytes, _ := json.MarshalIndent(tlMeta, "", "  ")
	if err := os.WriteFile(filepath.Join(dbPath, "timeline.meta"), tlBytes, 0644); err != nil {
		return fmt.Errorf("write timeline.meta: %w", err)
	}

	opts := stonedb.Options{
		CompactionMinGarbage: 10 * 1024 * 1024,
	}

	log.Println("Initializing storage engine...")
	db, err := stonedb.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to open StoneDB: %w", err)
	}
	defer db.Close()

	// 6. Setup Read Pipeline
	f, err := os.Open(bkPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var inputReader io.Reader = f

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

	// 7. Ingest Loop
	log.Println("Ingesting data...")
	type restoreEntry struct {
		Key      []byte
		Value    []byte
		IsDelete bool
	}
	var batch []restoreEntry
	count := 0
	restoreSeq := meta.SnapshotOpID
	restoreTx := meta.SnapshotTxID
	batchSize := 0

	applyBatch := func(entries []restoreEntry) error {
		if len(entries) == 0 {
			return nil
		}
		tx := db.NewTransaction(true)
		for _, e := range entries {
			var err error
			if e.IsDelete {
				err = tx.Delete(e.Key)
			} else {
				err = tx.Put(e.Key, e.Value)
			}
			if err != nil {
				tx.Discard()
				return err
			}
		}
		return tx.Commit()
	}

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

		batch = append(batch, restoreEntry{
			Key:      key,
			Value:    val,
			IsDelete: isDelete,
		})
		batchSize += kLen + vLen
		count++

		if batchSize > 4*1024*1024 {
			if err := applyBatch(batch); err != nil {
				return fmt.Errorf("batch apply: %w", err)
			}
			fmt.Printf("\rRestored items: %d", count)
			batch = batch[:0]
			batchSize = 0
		}
	}

	if len(batch) > 0 {
		if err := applyBatch(batch); err != nil {
			return fmt.Errorf("final apply: %w", err)
		}
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

// verifyBackupChecksum computes the SHA256 of the raw backup file (exactly
// as turnstone-backup wrote it to disk, i.e. before any gzip decompression)
// and compares it against the expected checksum from backup.meta, returning
// an error on mismatch. This is a standalone read-only pass over the file
// and never touches the restore target, so it's safe to run before any
// target-directory/DB state is created.
func verifyBackupChecksum(path, expectedSHA256 string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open backup file for verification: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return fmt.Errorf("read backup file for verification: %w", err)
	}

	calculated := hex.EncodeToString(hasher.Sum(nil))
	if calculated != expectedSHA256 {
		return fmt.Errorf("CHECKSUM MISMATCH!\nBackup file is corrupt.\nExpected: %s\nGot:      %s", expectedSHA256, calculated)
	}
	return nil
}
