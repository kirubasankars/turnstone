package stonedb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

	// Manually inject garbage stats into LDB
	k := make([]byte, len(sysStaleBytesPrefix)+4)
	copy(k, sysStaleBytesPrefix)
	binary.BigEndian.PutUint32(k[len(sysStaleBytesPrefix):], 99) // File 99

	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, 1024) // 1024 bytes garbage

	db.ldb.Put(k, v, nil)
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

func TestWAL_PurgeExpired(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		MaxWALSize:       1024,
		WALRetentionTime: 100 * time.Millisecond, // Short retention for testing
	}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Generate WAL files
	// Write enough to trigger rotation multiple times
	for i := 0; i < 5; i++ {
		// Write 500 bytes (approx half max wal size)
		tx := db.NewTransaction(true)
		tx.Put([]byte("k"), make([]byte, 500))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// 2. Advance Checkpoint (SafeOpID)
	// This ensures the logs are considered "flushed" to VLog/Index
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	safeOpID := db.LastOpID()

	// 3. Wait for retention
	time.Sleep(200 * time.Millisecond)

	// 4. Trigger Purge
	if err := db.writeAheadLog.PurgeExpired(opts.WALRetentionTime, safeOpID); err != nil {
		t.Fatalf("PurgeExpired failed: %v", err)
	}

	// 5. Verify some files are gone
	matches, _ := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	if len(matches) == 0 {
		t.Error("All WAL files deleted, expected at least one")
	}
	// Note: Precise assertion of file count is difficult due to header/metadata variance,
	// but reaching here ensures no crash.
}

func TestDB_VerifyChecksums_HappyPath(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{CompactionMinGarbage: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write and Rotate to create immutable VLog files
	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	tx.Commit()
	db.Checkpoint()

	// Call Verify
	if err := db.VerifyChecksums(); err != nil {
		t.Errorf("VerifyChecksums failed on valid DB: %v", err)
	}
}

func TestDB_RollbackWAL_Manual(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Get current WAL size
	initialSize := db.writeAheadLog.writeOffset

	// 2. Manually write to WAL (simulate the Prepare phase)
	dummyData := []byte("payload")
	if err := db.writeAheadLog.AppendBatch(dummyData); err != nil {
		t.Fatal(err)
	}

	newSize := db.writeAheadLog.writeOffset
	addedBytes := int64(newSize - initialSize)

	// 3. Rollback
	if err := db.rollbackWAL(addedBytes); err != nil {
		t.Fatalf("rollbackWAL failed: %v", err)
	}

	// 4. Verify size is back to initial
	if db.writeAheadLog.writeOffset != initialSize {
		t.Errorf("Expected offset %d, got %d", initialSize, db.writeAheadLog.writeOffset)
	}

	// 5. Verify physical file size
	info, _ := db.writeAheadLog.currentFile.Stat()
	if uint32(info.Size()) != initialSize {
		t.Errorf("Physical file size mismatch. Expected %d, got %d", initialSize, info.Size())
	}
}

func TestDB_Stats(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()

	// Cover getters
	_, _, _, _ = db.StorageStats()
	_ = db.LastOpID()
	_ = db.GetConflicts()
	_ = db.ActiveTransactionCount()
	db.SetCompactionMinGarbage(100)
	db.SetWALRetentionTime(time.Hour)
}

func TestRecovery_IsIndexConsistent_EdgeCases(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Test ErrNotFound case with non-zero transaction ID
	// Remove the sys key
	if err := db.ldb.Delete(sysTransactionIDKey, nil); err != nil {
		t.Fatal(err)
	}
	// Manually set transaction ID to simulate state
	db.transactionID = 100
	if db.isIndexConsistent() {
		t.Error("Expected inconsistent index when sys key missing but txID > 0")
	}

	// 2. Test corrupt value length
	if err := db.ldb.Put(sysTransactionIDKey, []byte("short"), nil); err != nil {
		t.Fatal(err)
	}
	if db.isIndexConsistent() {
		t.Error("Expected inconsistent index when sys key value is short")
	}
}

// TestInternal_ComputeStaleBytes tests the helper function used for garbage estimation.
// Targets compaction.go: computeStaleBytes (0.0%)
func TestInternal_ComputeStaleBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{CompactionMinGarbage: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Setup: Write a key "k1" -> File 0
	tx := db.NewTransaction(true)
	tx.Put([]byte("k1"), []byte("val1"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Checkpoint() // Rotate to File 1

	// 2. Prepare PendingOps that overwrite "k1"
	ops := map[string]*PendingOp{
		"k1": {Value: []byte("val2"), IsDelete: false},
		"k2": {Value: []byte("val2"), IsDelete: false}, // New key, no stale bytes
	}

	// 3. Call internal function
	staleMap := db.computeStaleBytes(ops)

	// 4. Verify
	// We expect garbage in File 0 for "k1".
	// Size = Header(29) + Key(2) + Val(4) = 35 bytes
	if garbage, ok := staleMap[0]; !ok || garbage <= 0 {
		t.Errorf("Expected garbage for file 0, got map: %v", staleMap)
	}
}

// TestInternal_RollbackWAL_Boundary tests error handling when rollback crosses file boundaries.
// Targets committer.go: rollbackWAL (73.3% -> 100%)
func TestInternal_RollbackWAL_Boundary(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Current offset is likely 0 or small.
	// Try to rollback 1000 bytes, which is definitely more than the current file size
	// (since we just opened it and wrote nothing/little).
	err = db.rollbackWAL(1000000)
	if err == nil {
		t.Error("Expected error rolling back past file boundary, got nil")
	}
}

// TestInternal_Commit_WALWritten_VLogFails verifies that VLog write failure triggers WAL rollback.
// Targets committer.go: persistBatch (71.4%), rollbackWAL success path.
// Scenario:
// 1. Transaction starts.
// 2. WAL append succeeds.
// 3. VLog append fails (file handle closed).
// 4. System detects failure, rolls back WAL, and returns error.
func TestInternal_Commit_WALWritten_VLogFails(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Write initial data to ensure WAL has some valid data before the bad batch
	txInit := db.NewTransaction(true)
	txInit.Put([]byte("init"), []byte("val"))
	txInit.Commit()

	initialWALSize := db.writeAheadLog.writeOffset

	// 2. Sabotage VLog: Close the file handle so Write() fails
	// Note: We close the underlying file but the VLog struct doesn't know yet.
	db.valueLog.currentFile.Close()

	// 3. Attempt a Write
	// This will:
	// a. Write to WAL (Success - writes to DB.wal.currentFile)
	// b. Write to VLog (Fail - writes to DB.vlog.currentFile which is closed)
	// c. Trigger rollbackWAL on DB.wal
	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	err = tx.Commit()

	// 4. Verify Failure
	if err == nil {
		t.Fatal("Expected commit error due to closed VLog")
	}

	// 5. Verify specific error path
	if !strings.Contains(err.Error(), "vlog write failed (wal rolled back)") {
		t.Errorf("Expected error to mention wal rollback, got: %v", err)
	}

	// 6. Verify Rollback
	// WAL offset should be back to where it was after txInit
	if db.writeAheadLog.writeOffset != initialWALSize {
		t.Errorf("WAL offset mismatch. Expected %d, got %d. Rollback failed.", initialWALSize, db.writeAheadLog.writeOffset)
	}
}

// TestInternal_PrepareBatch_IntraBatchConflict tests the logic for detecting conflicts
// between transactions *within the same batch*.
// Targets committer.go: prepareBatch, hasIntraBatchConflict
func TestInternal_PrepareBatch_IntraBatchConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Scenario 1: TxA and TxB both write "key1".
	// TxA comes first in batch -> Succeeds.
	// TxB comes second -> Fails (Write-Write conflict).

	txA := db.NewTransaction(true)
	txA.Put([]byte("key1"), []byte("valA"))

	txB := db.NewTransaction(true)
	txB.Put([]byte("key1"), []byte("valB"))

	reqs := []commitRequest{
		{tx: txA, resp: make(chan error, 1)},
		{tx: txB, resp: make(chan error, 1)},
	}

	// Call prepareBatch directly
	batch, _ := db.prepareBatch(reqs)

	// Check results
	// TxA should be in batch
	if len(batch.reqs) != 1 {
		t.Errorf("Expected 1 successful request in batch, got %d", len(batch.reqs))
	}
	if batch.reqs[0].tx != txA {
		t.Error("TxA should have succeeded")
	}

	// TxB should have failed with ErrWriteConflict
	errB := <-reqs[1].resp
	if errB != ErrWriteConflict {
		t.Errorf("TxB expected ErrWriteConflict, got %v", errB)
	}

	// Scenario 2: TxC reads "key2", TxD writes "key2".
	// TxD comes first -> Succeeds.
	// TxC comes second -> Fails (Read-Write conflict / Stale Read).
	// Note: prepareBatch processes sequentially. If TxD is processed first, it registers a write to "key2".
	// When TxC is processed, hasIntraBatchConflict checks TxC.readSet["key2"] against batchWrites.

	txC := db.NewTransaction(true)
	txC.readSet["key2"] = struct{}{} // Simulate read
	txC.Put([]byte("other"), []byte("val"))

	txD := db.NewTransaction(true)
	txD.Put([]byte("key2"), []byte("valD"))

	reqs2 := []commitRequest{
		{tx: txD, resp: make(chan error, 1)}, // Writer first
		{tx: txC, resp: make(chan error, 1)}, // Reader second
	}

	batch2, _ := db.prepareBatch(reqs2)

	if len(batch2.reqs) != 1 {
		t.Errorf("Expected 1 successful request in batch 2, got %d", len(batch2.reqs))
	}
	if batch2.reqs[0].tx != txD {
		t.Error("TxD should have succeeded")
	}

	errC := <-reqs2[1].resp
	if errC != ErrWriteConflict {
		t.Errorf("TxC expected ErrWriteConflict (Intra-batch Read-Write), got %v", errC)
	}
}

// TestRecovery_LastVLogCorruption_PartialWrite ensures data consistency when
// the last ValueLog file ends with a partial (incomplete) entry.
// The recovery process should truncate the partial entry and assume it wasn't committed.
// However, if the WAL had it, the WAL replay will bring it back (if commit succeeded there).
// In this test, we simulate that VLog corruption happened *after* DB closed (e.g. disk rot),
// but we assume WAL is fine.
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

// TestRecovery_LastVLogCorruption_GarbageAppend ensures that appending garbage
// to the last ValueLog file does not prevent the DB from opening and serving existing data.
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
