package engine

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"turnstone/protocol"
)

// --- 3. Persistence & Recovery ---

func TestRestartRecovery(t *testing.T) {
	// Phase 1: Write data
	db1, dir := setupDB(t)
	tx := db1.BeginTx()
	tx.Put("persistence", []byte("works"))
	tx.Commit()

	// Ensure flushed to disk
	if err := db1.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	db1.Close()

	// Phase 2: Open fresh instance
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// Verify Data
	// Read with a high TxID to ensure we see the latest recovered data
	val, err := db2.Get("persistence", tx.ID+100)
	if err != nil {
		t.Fatalf("Get failed after recovery: %v", err)
	}
	if string(val) != "works" {
		t.Errorf("Data corruption. Expected 'works', got %s", val)
	}

	// Verify Clock
	if db2.CurrentTxID() < tx.ID {
		t.Errorf("Clock did not recover. Got %d, expected >= %d", db2.CurrentTxID(), tx.ID)
	}
}

func TestActiveVLogRecovery(t *testing.T) {
	db1, dir := setupDB(t)
	// Write but DO NOT FLUSH manually. This simulates data lingering in active vlog file.
	tx1 := db1.BeginTx()
	tx1.Put("crash", []byte("survived"))
	tx1.Commit()

	tx2 := db1.BeginTx()
	tx2.Put("crash2", []byte("survived2"))
	tx2.Commit()

	// Close flushes the active buffer to disk.
	db1.Close()

	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	val, err := db2.Get("crash", tx2.ID)
	if err != nil {
		t.Fatalf("Failed to read key from active vlog replay: %v", err)
	}
	if string(val) != "survived" {
		t.Errorf("Active vlog recovery mismatch. Got %s", val)
	}
}

func TestTxIDRecoveryFromVLog(t *testing.T) {
	// 1. Initialize DB to establish structure
	db1, dir := setupDB(t)
	tx1 := db1.BeginTx()
	tx1.Put("k1", []byte("v1"))
	tx1.Commit()
	db1.Close() // Flushes 0.vlog

	// 2. Manually append a higher TxID (200) to 0.vlog
	vlogPath := filepath.Join(dir, "0.vlog")
	f, err := os.OpenFile(vlogPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("Failed to open vlog: %v", err)
	}

	highTxID := uint64(200)
	key := "ghost_key"
	val := []byte("ghost_val")
	keyLen := uint32(len(key))
	valLen := uint32(len(val))

	// Construct the entry manually
	var buf bytes.Buffer
	// Header placeholder
	buf.Write(make([]byte, 20))
	buf.WriteString(key)
	buf.Write(val)

	data := buf.Bytes()
	// Fill Header
	binary.BigEndian.PutUint64(data[4:12], highTxID)

	// PACK META: KeyLen(16) | ValLen(32) | Type(1)
	var meta uint64
	meta |= (uint64(keyLen) & 0xFFFF) << 32
	meta |= (uint64(valLen) & 0xFFFFFFFF)
	// IsDelete = false, so bit 63 is 0

	binary.BigEndian.PutUint64(data[12:20], meta)

	// CRC - Use Castagnoli to match engine
	crc := crc32.New(protocol.Crc32Table)
	crc.Write(data[4:]) // TxID + Meta + Key + Val
	sum := crc.Sum32()
	binary.BigEndian.PutUint32(data[0:4], sum)

	if _, err := f.Write(data); err != nil {
		t.Fatalf("Failed to append to vlog: %v", err)
	}
	f.Close()

	// 3. Reopen DB
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// 4. Verify Clock
	curr := db2.CurrentTxID()
	if curr < highTxID {
		t.Errorf("TxID recovery failed. Expected >= %d, got %d", highTxID, curr)
	}

	// 5. Verify the ghost key is actually readable (Recovery should index it)
	valRead, err := db2.Get(key, highTxID)
	if err != nil {
		t.Fatalf("Failed to read ghost key recovered from vlog: %v", err)
	}
	if string(valRead) != string(val) {
		t.Errorf("Ghost key value mismatch. Got %s", valRead)
	}
}

func TestRecoveryCorruptedVLog(t *testing.T) {
	// 1. Create valid data
	db1, dir := setupDB(t)
	tx := db1.BeginTx()
	tx.Put("valid", []byte("data"))
	tx.Commit()
	db1.Close()

	// 2. Append corrupt entry (Bad CRC)
	vlogPath := filepath.Join(dir, "0.vlog")
	f, err := os.OpenFile(vlogPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	// Construct entry with bad CRC
	var buf bytes.Buffer
	buf.Write(make([]byte, 20))
	buf.WriteString("corrupt")
	buf.Write([]byte("bad"))
	data := buf.Bytes()

	keyLen := uint32(len("corrupt"))
	valLen := uint32(len("bad"))

	// Header details
	binary.BigEndian.PutUint64(data[4:12], 2) // TxID 2

	// Meta Packing
	var meta uint64
	meta |= (uint64(keyLen) & 0xFFFF) << 32
	meta |= (uint64(valLen) & 0xFFFFFFFF)

	binary.BigEndian.PutUint64(data[12:20], meta)

	// Write BAD CRC (0xDEADBEEF)
	binary.BigEndian.PutUint32(data[0:4], 0xDEADBEEF)

	f.Write(data)
	f.Close()

	// 3. Open DB - Should survive but ignore corrupt entry
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open failed on corrupt vlog: %v", err)
	}
	defer db2.Close()

	// 4. Verify 'valid' exists
	val, err := db2.Get("valid", 10)
	if err != nil || string(val) != "data" {
		t.Errorf("Failed to recover valid data before corruption")
	}

	// 5. Verify 'corrupt' does not exist
	_, err = db2.Get("corrupt", 10)
	if err != protocol.ErrKeyNotFound {
		t.Errorf("Corrupt data should not be indexed")
	}
}

func TestRecoveryPartialEntry(t *testing.T) {
	db1, dir := setupDB(t)
	tx := db1.BeginTx()
	tx.Put("valid", []byte("data"))
	tx.Commit()
	db1.Close()

	// Append partial bytes (less than header)
	vlogPath := filepath.Join(dir, "0.vlog")
	f, err := os.OpenFile(vlogPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x00, 0x01, 0x02}) // Just 3 bytes
	f.Close()

	// Open should survive and truncate partial entry
	_, err = Open(dir, 0)
	if err != nil {
		t.Fatalf("Expected nil error on partial vlog (auto-truncate), got: %v", err)
	}
}

func TestRecoveryTombstone(t *testing.T) {
	// 1. Write Key A
	db1, dir := setupDB(t)
	tx := db1.BeginTx()
	tx.Put("A", []byte("val"))
	tx.Commit()
	db1.Close()

	// 2. Manually append Tombstone for A to vlog
	vlogPath := filepath.Join(dir, "0.vlog")
	f, err := os.OpenFile(vlogPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	// Manual Tombstone Entry construction
	txID := uint64(2)
	key := "A"
	// Tombstones now have EMPTY value (len 0), validation relies on Type bit
	keyLen := uint32(len(key))
	valLen := uint32(0)

	var buf bytes.Buffer
	buf.Write(make([]byte, 20))
	buf.WriteString(key)
	// No value bytes written
	data := buf.Bytes()

	binary.BigEndian.PutUint64(data[4:12], txID)

	// PACK META: Bit 63 = 1 (Tombstone)
	var meta uint64
	meta |= (1 << 63)
	meta |= (uint64(keyLen) & 0xFFFF) << 32
	meta |= (uint64(valLen) & 0xFFFFFFFF)

	binary.BigEndian.PutUint64(data[12:20], meta)

	// CRC - Use Castagnoli
	crc := crc32.New(protocol.Crc32Table)
	crc.Write(data[4:])
	binary.BigEndian.PutUint32(data[0:4], crc.Sum32())

	f.Write(data)
	f.Close()

	// 3. Open DB
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// 4. Verify A is deleted (ErrKeyNotFound)
	_, err = db2.Get("A", 10)
	if err != protocol.ErrKeyNotFound {
		t.Errorf("Expected tombstone to be recovered as delete, got err: %v", err)
	}
}

func TestIndexRebuild(t *testing.T) {
	// 1. Create DB and write data
	db1, dir := setupDB(t)

	// Write spanning multiple files
	tx1 := db1.BeginTx()
	tx1.Put("k1", []byte("v1"))
	tx1.Commit()
	db1.Flush() // k1 in 0.vlog, active becomes 1.vlog

	tx2 := db1.BeginTx()
	tx2.Put("k2", []byte("v2"))
	tx2.Commit()
	db1.Flush() // k2 in 1.vlog, active becomes 2.vlog

	db1.Close()

	// 2. Destroy Index
	indexPath := filepath.Join(dir, "index")
	if err := os.RemoveAll(indexPath); err != nil {
		t.Fatal(err)
	}

	// 3. Reopen DB
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// 4. Verify data exists
	v1, err := db2.Get("k1", 10)
	if err != nil || string(v1) != "v1" {
		t.Errorf("Failed to recover k1: %v", err)
	}
	v2, err := db2.Get("k2", 10)
	if err != nil || string(v2) != "v2" {
		t.Errorf("Failed to recover k2: %v", err)
	}

	// Verify TxID
	if db2.CurrentTxID() < 2 {
		t.Errorf("TxID not recovered. Got %d", db2.CurrentTxID())
	}
}

func TestIndexCorruptionRecovery(t *testing.T) {
	// 1. Initialize DB and write data
	db1, dir := setupDB(t)
	tx := db1.BeginTx()
	tx.Put("vital_data", []byte("persisted"))
	tx.Commit()
	db1.Close()

	// 2. Corrupt LevelDB manually
	// Deleting the CURRENT file or MANIFEST usually triggers corruption errors in LevelDB
	indexDir := filepath.Join(dir, "index")
	files, _ := os.ReadDir(indexDir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "MANIFEST") || strings.HasPrefix(f.Name(), "CURRENT") {
			path := filepath.Join(indexDir, f.Name())
			// Truncate/Corrupt file content
			if err := os.WriteFile(path, []byte("GARBAGE_DATA"), 0o644); err != nil {
				t.Fatalf("Failed to corrupt LevelDB file: %v", err)
			}
		}
	}

	// 3. Reopen DB
	// Should detect error, wipe index, and rebuild from VLog
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open failed during corruption recovery: %v", err)
	}
	defer db2.Close()

	// 4. Verify data was recovered from VLog
	val, err := db2.Get("vital_data", 10)
	if err != nil {
		t.Fatalf("Failed to read key after index corruption recovery: %v", err)
	}
	if string(val) != "persisted" {
		t.Errorf("Data mismatch after recovery. Got %s", val)
	}
}

func TestChecksumValidation(t *testing.T) {
	db, dir := setupDB(t)

	// File 0: To be corrupted (Inactive)
	key1 := "corrupt_me"
	val1 := []byte("original_value")

	tx1 := db.BeginTx()
	tx1.Put(key1, val1)
	tx1.Commit()
	db.Flush() // Writes 0.vlog, increments active to 1

	// File 1: Active file (Safe from corruption in this test)
	key2 := "safe"
	val2 := []byte("safe_value")

	tx2 := db.BeginTx()
	tx2.Put(key2, val2)
	tx2.Commit()
	db.Flush() // Writes 1.vlog, increments active to 2

	db.Close()

	// Corrupt file 0 (The older, immutable file)
	vlogPath := filepath.Join(dir, "0.vlog")
	f, err := os.OpenFile(vlogPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("Failed to open vlog for corruption: %v", err)
	}

	// Header is 20 bytes. Data starts after that.
	// Flip a bit in the value part.
	offset := int64(20 + len(key1) + 1) // +1 into value
	b := make([]byte, 1)
	f.ReadAt(b, offset)
	b[0] ^= 0xFF // Flip bits
	f.WriteAt(b, offset)
	f.Close()

	// Reopen
	db2, err := Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Attempt read on corrupted older file
	// Should fail checksum check because Open() didn't truncate it (it wasn't the active file)
	_, err = db2.Get(key1, 100)
	if err != ErrChecksumMismatch {
		t.Errorf("Expected ErrChecksumMismatch, got %v", err)
	}
}

func TestStartupPermissions(t *testing.T) {
	// Skip on root because root ignores file permissions on many systems
	if os.Getuid() == 0 {
		t.Skip("Skipping permission test: running as root")
	}

	dir := t.TempDir()

	// Restore permissions after test to allow auto-cleanup
	// Must restore execute on directories for Walk/RemoveAll to work
	t.Cleanup(func() {
		os.Chmod(dir, 0o755)
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if info.IsDir() {
					os.Chmod(path, 0o755)
				} else {
					os.Chmod(path, 0o666)
				}
			}
			return nil
		})
	})

	// Case 1: Directory Read-Only
	// Make directory read-only (prevents creating new files)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}

	_, err := Open(dir, 0)
	if err == nil {
		t.Fatal("Expected error opening read-only directory, got nil")
	} else if !strings.Contains(err.Error(), "permission denied") && !strings.Contains(err.Error(), "permission check failed") {
		// Loosened check to verify just that it failed with some permission issue
		t.Fatalf("Expected permission error, got: %v", err)
	}

	// Restore permissions so we can write for Case 2
	os.Chmod(dir, 0o755)

	// Case 2: Existing vlog File Read-Only
	vlogPath := filepath.Join(dir, "0.vlog")
	if err := os.WriteFile(vlogPath, []byte("dummy data"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Make vlog file read-only
	if err := os.Chmod(vlogPath, 0o400); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, 0)
	if err == nil {
		t.Fatal("Expected error opening read-only vlog, got nil")
	} else if !strings.Contains(err.Error(), "permission denied") && !strings.Contains(err.Error(), "permission check failed") {
		t.Fatalf("Expected permission error, got: %v", err)
	}

	// Clean up permissions is handled by t.Cleanup
}
