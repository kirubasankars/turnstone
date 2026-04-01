package stonedb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_CrashConsistency(t *testing.T) {
	dir := t.TempDir()

	// 1. Open and write data
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.NewTransaction(true)
	tx.Put([]byte("persist"), []byte("true"))
	tx.Commit()

	// 2. "Crash" (Close without Checkpoint/Rotation implies WAL replay needed)
	db.Close()

	// 3. Re-open
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// 4. Verify data exists (recovered from WAL)
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("persist"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "true" {
		t.Errorf("Data lost after restart")
	}
}

func TestRecovery_IndexRebuild(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.NewTransaction(true)
	tx.Put([]byte("a"), []byte("1"))
	tx.Commit()
	db.Close()

	// Manually corrupt/delete the index directory
	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}

	// Re-open
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Verify data is still accessible
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get failed after index rebuild: %v", err)
	}
	if string(val) != "1" {
		t.Errorf("Index rebuild failed")
	}
}

func TestDB_Open_RebuildIndex_Corruption(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Write data
	tx := db.NewTransaction(true)
	tx.Put([]byte("a"), []byte("b"))
	tx.Commit()
	db.Close()

	// Corrupt Index: Delete MANIFEST or CURRENT
	os.Remove(filepath.Join(dir, "index", "CURRENT"))

	// Reopen - should detect LDB failure and rebuild
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Failed to open with corrupt index: %v", err)
	}
	defer db2.Close()

	// Verify data
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("a"))
	if err != nil || string(val) != "b" {
		t.Error("Data not found after index rebuild")
	}
}

func TestDB_Open_InconsistentIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Write data
	tx := db.NewTransaction(true)
	tx.Put([]byte("a"), []byte("b"))
	tx.Commit()

	// Manually mess up the TransactionID in LevelDB to be older than memory
	// This simulates a crash where Index wasn't flushed but WAL was?
	badID := make([]byte, 8)
	binary.BigEndian.PutUint64(badID, 0) // Reset to 0
	db.ldb.Put(sysTransactionIDKey, badID, nil)

	db.Close()

	// Reopen - should detect inconsistency (LDB ID < WAL ID) and rebuild
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db2.Close()

	// If rebuild happened, we should find "a"
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("a"))
	if err != nil || string(val) != "b" {
		t.Error("Index check failed to trigger rebuild or rebuild failed")
	}
}

func TestDB_LoadGarbageStats(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate in-memory garbage stats accumulation
	db.mu.Lock()
	db.deletedBytesByFile[99] = 1024
	db.mu.Unlock()

	// Close() triggers Checkpoint(), which persists deletedBytesByFile to LevelDB
	db.Close()

	// Reopen
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Check loaded stats
	if stat, ok := db2.deletedBytesByFile[99]; !ok || stat != 1024 {
		t.Errorf("Failed to load garbage stats. Got %d", stat)
	}
}

func TestDB_LDB_CorruptFile(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	db.Close()

	// Corrupt the CURRENT file to be garbage (not missing)
	os.WriteFile(filepath.Join(dir, "index", "CURRENT"), []byte("GARBAGE"), 0o644)

	// Open should fail or rebuild
	db2, err := Open(dir, Options{})
	if err != nil {
		// Failure acceptable, but if success, ensure close
	} else {
		db2.Close()
	}
}

func TestRecovery_FullRebuildFromWAL(t *testing.T) {
	dir := t.TempDir()

	// 1. Create DB and write data
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Increase count to 1500 to trigger batch flushing in RebuildIndexFromVLog (batch size 1000)
	const count = 1500
	for i := 0; i < count; i++ {
		tx := db.NewTransaction(true)
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := tx.Put(key, val); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	// 2. Destructive step: Remove VLog and Index folders completely
	// This simulates a total loss of derived data, leaving only the log of truth (WAL).
	if err := os.RemoveAll(filepath.Join(dir, "vlog")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}

	// 3. Reopen
	// This should:
	// a. Create a new empty VLog directory.
	// b. Scan the WAL and replay all transactions into the new VLog.
	// c. Detect that the Index is missing.
	// d. Scan the new VLog to rebuild the Index.
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// 4. Verify Data integrity
	for i := 0; i < count; i++ {
		tx := db2.NewTransaction(false)
		key := []byte(fmt.Sprintf("key-%d", i))
		val, err := tx.Get(key)
		if err != nil {
			t.Fatalf("Key %d missing after full recovery: %v", i, err)
		}
		expected := fmt.Sprintf("val-%d", i)
		if string(val) != expected {
			t.Errorf("Value mismatch for %d. Got %s, want %s", i, val, expected)
		}
		tx.Discard()
	}

	// 5. Verify total key count
	keyCount, err := db2.KeyCount()
	if err != nil {
		t.Fatal(err)
	}
	if keyCount != int64(count) {
		t.Errorf("Expected %d keys, got %d", count, keyCount)
	}
}

func TestRecovery_PartialRebuildFromWAL(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024} // Ensure WAL is large enough

	// 1. Create DB and write enough data to span multiple VLog files
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write Batch 1
	for i := 0; i < 50; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("val1"))
		tx.Commit()
	}
	db.Checkpoint() // Rotate VLog (File 0 -> 1)

	// Write Batch 2
	for i := 50; i < 100; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("val2"))
		tx.Commit()
	}
	db.Close()

	// 2. Simulate corruption/loss: Delete the LATEST VLog file.
	// The WAL should still have the data for Batch 2.
	vlogDir := filepath.Join(dir, "vlog")
	matches, _ := filepath.Glob(filepath.Join(vlogDir, "*.vlog"))
	if len(matches) < 2 {
		t.Fatalf("Expected at least 2 VLog files, got %d", len(matches))
	}
	// Delete the last one (contains Batch 2)
	lastVLog := matches[len(matches)-1]
	if err := os.Remove(lastVLog); err != nil {
		t.Fatal(err)
	}

	// Delete Index to force rebuild (since old pointers in index are now invalid)
	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}

	// 3. Reopen
	// - Recover() finds maxTx from Batch 1 (in File 0).
	// - ReplaySinceTx() replays Batch 2 from WAL (appends to new file).
	// - RebuildIndex() scans VLogs to restore index.
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	// 4. Verify Batch 1 (Persisted VLog)
	for i := 0; i < 50; i++ {
		tx := db2.NewTransaction(false)
		val, err := tx.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Errorf("Batch 1 Key k%d missing: %v", i, err)
		} else if string(val) != "val1" {
			t.Errorf("Batch 1 Key k%d mismatch: got %s", i, val)
		}
		tx.Discard()
	}

	// 5. Verify Batch 2 (Recovered from WAL)
	for i := 50; i < 100; i++ {
		tx := db2.NewTransaction(false)
		val, err := tx.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Errorf("Batch 2 Key k%d missing (WAL recovery failed): %v", i, err)
		} else if string(val) != "val2" {
			t.Errorf("Batch 2 Key k%d mismatch: got %s", i, val)
		}
		tx.Discard()
	}
}

func TestRecovery_WALTruncation_Atomicity(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptWAL: true, MaxWALSize: 1024 * 1024}

	// 1. Setup: Write Tx1 and Tx2
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Tx 1: "safe"
	tx1 := db.NewTransaction(true)
	tx1.Put([]byte("safe"), []byte("kept"))
	tx1.Commit()

	// Tx 2: "victim" - will be corrupted/truncated
	tx2 := db.NewTransaction(true)
	tx2.Put([]byte("victim"), []byte("lost"))
	tx2.Commit()

	// Ensure WAL is flushed
	db.Close()

	// 2. Corrupt the WAL by truncating the last few bytes
	// This simulates a power loss during the write of Tx 2
	walDir := filepath.Join(dir, "wal")
	matches, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if len(matches) == 0 {
		t.Fatal("No WAL files found")
	}
	lastWAL := matches[len(matches)-1]

	info, _ := os.Stat(lastWAL)
	// Truncate 1 byte off the end. This invalidates the checksum/length of the last frame (Tx2).
	if err := os.Truncate(lastWAL, info.Size()-1); err != nil {
		t.Fatal(err)
	}

	// 3. Delete VLog and Index to simulate that VLog write didn't happen for Tx2
	// because it crashed during WAL write. This forces a rebuild from the (now truncated) WAL.
	if err := os.RemoveAll(filepath.Join(dir, "vlog")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}

	// 4. Reopen
	// Should detect corruption at EOF and truncate the *entire* last batch (Tx 2).
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed during truncation recovery: %v", err)
	}
	defer db2.Close()

	// 5. Verify Tx 1 is present
	txCheck := db2.NewTransaction(false)
	val, err := txCheck.Get([]byte("safe"))
	if err != nil {
		t.Errorf("Tx 1 data missing: %v", err)
	}
	if string(val) != "kept" {
		t.Errorf("Tx 1 data mismatch: %s", val)
	}

	// 6. Verify Tx 2 is gone (Atomicity: partial write = no write)
	_, err = txCheck.Get([]byte("victim"))
	if err != ErrKeyNotFound {
		t.Errorf("Tx 2 should be missing, got error: %v", err)
	}
}

func TestRecovery_WALCorruption_Strict(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptWAL: false} // Explicitly require failure on corruption

	// 1. Write data
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	tx.Put([]byte("key"), []byte("val"))
	tx.Commit()
	db.Close()

	// 2. Corrupt the WAL by appending garbage
	walDir := filepath.Join(dir, "wal")
	matches, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if len(matches) == 0 {
		t.Fatal("No WAL files found")
	}
	lastWAL := matches[len(matches)-1]

	f, err := os.OpenFile(lastWAL, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// 3. Reopen - Expect Failure
	if _, err := Open(dir, opts); err == nil {
		t.Fatal("Expected Open to fail due to WAL corruption")
	}
}

func TestRecovery_LastVLogCorruption_PartialWrite(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		MaxWALSize:         1024 * 1024,
		TruncateCorruptWAL: true,
	}

	// 1. Write data
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("val"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	// 2. Sabotage the last VLog file (Partial truncate)
	vlogDir := filepath.Join(dir, "vlog")
	matches, _ := filepath.Glob(filepath.Join(vlogDir, "*.vlog"))
	if len(matches) == 0 {
		t.Fatal("No vlog files found")
	}

	// db.Close() triggers a checkpoint which might create a new empty log file.
	// We want to corrupt the last file that actually contains data.
	var targetVLog string
	var targetSize int64
	var targetIndex int = -1

	// Iterate backwards to find the last non-empty file
	for i := len(matches) - 1; i >= 0; i-- {
		info, err := os.Stat(matches[i])
		if err != nil {
			continue
		}
		if info.Size() > 5 { // Ensure it's large enough to truncate
			targetVLog = matches[i]
			targetSize = info.Size()
			targetIndex = i
			break
		}
	}

	if targetVLog == "" {
		t.Fatal("No suitable non-empty vlog files found to corrupt")
	}

	// Delete any newer (empty) VLog files created by the clean shutdown rotation.
	// This ensures Recover() treats targetVLog as the last file, allowing truncation recovery.
	for i := targetIndex + 1; i < len(matches); i++ {
		if err := os.Remove(matches[i]); err != nil {
			t.Logf("Failed to remove empty vlog %s: %v", matches[i], err)
		}
	}

	// Truncate by 5 bytes to break the last entry
	if err := os.Truncate(targetVLog, targetSize-5); err != nil {
		t.Fatal(err)
	}

	// 3. Reopen
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed during recovery: %v", err)
	}
	defer db2.Close()

	// 4. Verify Data (Should be fully recovered from WAL)
	for i := 0; i < 10; i++ {
		tx := db2.NewTransaction(false)
		val, err := tx.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Errorf("Key k%d missing after recovery: %v", i, err)
		} else if string(val) != "val" {
			t.Errorf("Key k%d mismatch: %s", i, val)
		}
		tx.Discard()
	}
}

func TestRecovery_LastVLogCorruption_GarbageAppend(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024}

	// 1. Write data
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	tx.Put([]byte("key"), []byte("val"))
	tx.Commit()
	db.Close()

	// 2. Append garbage to VLog
	vlogDir := filepath.Join(dir, "vlog")
	matches, _ := filepath.Glob(filepath.Join(vlogDir, "*.vlog"))
	lastVLog := matches[len(matches)-1]

	f, err := os.OpenFile(lastVLog, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("GARBAGE"))
	f.Close()

	// 3. Reopen
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db2.Close()

	// 4. Verify Key exists
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("key"))
	if err != nil || string(val) != "val" {
		t.Errorf("Data lost or corrupt")
	}
}

func TestRecovery_VLogCorruption_Middle_Strict(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024}

	// 1. Create DB and generate 3 VLog files
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// File 0
	tx := db.NewTransaction(true)
	tx.Put([]byte("k0"), []byte("val0"))
	tx.Commit()
	db.Checkpoint() // Rotate 0 -> 1

	// File 1 (Middle)
	tx = db.NewTransaction(true)
	tx.Put([]byte("k1"), []byte("val1"))
	tx.Commit()
	db.Checkpoint() // Rotate 1 -> 2

	// File 2 (Last)
	tx = db.NewTransaction(true)
	tx.Put([]byte("k2"), []byte("val2"))
	tx.Commit()

	db.Close()

	// 2. Corrupt File 1
	vlogDir := filepath.Join(dir, "vlog")
	matches, _ := filepath.Glob(filepath.Join(vlogDir, "*.vlog"))
	if len(matches) < 3 {
		t.Fatalf("Expected at least 3 VLog files, got %d", len(matches))
	}
	middleVLog := matches[1]

	f, err := os.OpenFile(middleVLog, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Overwrite a byte in the header or payload to cause checksum failure
	// Header size is 29. Payload follows.
	// Let's overwrite byte 30.
	if _, err := f.WriteAt([]byte{0xFF}, 30); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// 3. Reopen - Expect Failure
	// VLog recovery scans all files to find maxTx/maxOp.
	// It checks checksums. If a middle file is corrupt, it cannot simply truncate it because it would lose committed data that might be referenced by the index (or future replay).
	// Currently `Recover` in `vlog.go` returns error if `!isLastFile`.
	if _, err := Open(dir, opts); err == nil {
		t.Fatal("Expected Open to fail due to corruption in middle VLog file")
	}
}

func TestRecovery_TimelineMeta_TempFileIgnored(t *testing.T) {
	dir := t.TempDir()

	// 1. Create a valid timeline.meta
	metaPath := filepath.Join(dir, "timeline.meta")
	validJSON := `{"current_timeline": 2, "history": []}`
	if err := os.WriteFile(metaPath, []byte(validJSON), 0644); err != nil {
		t.Fatal(err)
	}

	// 2. Create a garbage/partial timeline.meta.tmp
	tmpPath := filepath.Join(dir, "timeline.meta.tmp")
	if err := os.WriteFile(tmpPath, []byte(`{"current_timel`), 0644); err != nil {
		t.Fatal(err)
	}

	// 3. Open DB
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open failed when .tmp meta file exists: %v", err)
	}
	defer db.Close()

	// 4. Verify we loaded the valid timeline 2
	if db.timelineMeta.CurrentTimeline != 2 {
		t.Errorf("Expected timeline 2, got %d", db.timelineMeta.CurrentTimeline)
	}
}
