package stonedb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestCompaction_BasicGarbageCollection(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		MaxWALSize:           1024 * 1024,
		CompactionMinGarbage: 1, // Trigger on any garbage for test
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 1. Fill File 0 with initial data
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		txn := db.NewTransaction(true)
		txn.Put(key, val)
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Force Rotate VLog (File 0 -> File 1).
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 2. Overwrite data to create garbage in File 0
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-updated-%d", i))
		txn := db.NewTransaction(true)
		txn.Put(key, val)
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Rotate VLog again
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 3. Run Compaction
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// 4. Verify Data integrity post-compaction
	txn := db.NewTransaction(false)
	val, err := txn.Get([]byte("key-25"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val-updated-25" {
		t.Errorf("Data corruption post-compaction: expected val-updated-25, got %s", val)
	}
	txn.Discard()

	// 5. Verify File 0 Deletion
	vlog0 := filepath.Join(dir, "vlog", fmt.Sprintf("%04d.vlog", 0))
	if _, err := os.Stat(vlog0); !os.IsNotExist(err) {
		t.Errorf("File 0 should have been deleted by compaction, but it still exists at %s", vlog0)
	}
}

func TestCompaction_SnapshotIsolation_BlocksDeletion(t *testing.T) {
	dir := t.TempDir()
	opts := Options{CompactionMinGarbage: 1}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Setup: Write Key "foo" = "v1" in File 0
	{
		tx := db.NewTransaction(true)
		tx.Put([]byte("foo"), []byte("v1"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.Checkpoint() // Rotate to File 1. File 0 is now sealed.

	// 2. Start a "Long Running" Read Transaction.
	longTx := db.NewTransaction(false)
	val, err := longTx.Get([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Fatal("Snapshot failed to see initial value")
	}

	// 3. Update "foo" = "v2" in a new transaction.
	{
		tx := db.NewTransaction(true)
		tx.Put([]byte("foo"), []byte("v2"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.Checkpoint() // Rotate again

	// 4. Run Compaction - should NOT physically delete File 0 due to longTx
	if err := db.RunCompaction(); err != nil {
		t.Fatal(err)
	}

	// 5. Verify File 0 still exists
	vlog0 := filepath.Join(dir, "vlog", fmt.Sprintf("%04d.vlog", 0))
	if _, err := os.Stat(vlog0); os.IsNotExist(err) {
		t.Error("File 0 was deleted prematurely! longTx is still active and needs it.")
	}

	// 6. Verify longTx still reads "v1"
	val, err = longTx.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("longTx failed to read after compaction: %v", err)
	}
	if string(val) != "v1" {
		t.Errorf("longTx should still see 'v1', got '%s'", val)
	}

	longTx.Discard()

	// 9. Trigger deferred deletion
	db.deleteObsoleteFiles()

	// 10. Verify deletion happens now
	if _, err := os.Stat(vlog0); !os.IsNotExist(err) {
		t.Error("File 0 should be deleted now that longTx is finished")
	}
}

func TestCompaction_RewriteWithSkip(t *testing.T) {
	// This test targets logic where rewriteBatch skips entries
	// that have been superseded by newer transactions.
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024, CompactionMinGarbage: 1}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Write Key A (Tx1) in File 0
	tx1 := db.NewTransaction(true)
	tx1.Put([]byte("A"), []byte("v1"))
	tx1.Commit()

	db.Checkpoint()

	// 2. Update Key A (Tx2) in File 1
	tx2 := db.NewTransaction(true)
	tx2.Put([]byte("A"), []byte("v2"))
	tx2.Commit()

	// 3. Manually trigger compaction on File 0
	// rewriteBatch will see "A" in File 0.
	// It checks LevelDB. LevelDB points to Tx2 (File 1).
	// Tx1 != Tx2, so "A" from File 0 is dropped.
	if err := db.RunCompaction(); err != nil {
		t.Fatal(err)
	}

	// 4. Verify "v2" is preserved
	tx3 := db.NewTransaction(false)
	val, _ := tx3.Get([]byte("A"))
	if string(val) != "v2" {
		t.Errorf("Expected v2, got %s", val)
	}
}

func TestCompaction_Tombstones(t *testing.T) {
	dir := t.TempDir()
	opts := Options{CompactionMinGarbage: 1}
	db, _ := Open(dir, opts)

	// File 0: Write A
	tx := db.NewTransaction(true)
	tx.Put([]byte("A"), []byte("val"))
	tx.Commit()
	db.Checkpoint()

	// File 1: Delete A
	tx = db.NewTransaction(true)
	tx.Delete([]byte("A"))
	tx.Commit()
	db.Checkpoint()

	// Compaction on File 0 should remove A because latest version is tombstone in File 1
	if err := db.RunCompaction(); err != nil {
		t.Fatal(err)
	}

	// Verify A is gone/deleted
	tx = db.NewTransaction(false)
	if _, err := tx.Get([]byte("A")); err != ErrKeyNotFound {
		t.Error("Expected not found")
	}
	db.Close()
}

func TestValueLog_Iterate_MissingFile(t *testing.T) {
	dir := t.TempDir()
	// Default 1MB threshold would prevent compaction here since we inject only 100 bytes
	opts := Options{CompactionMinGarbage: 1}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Inject a fake deleted file entry
	db.mu.Lock()
	db.deletedBytesByFile[999] = 100 // File 999 has garbage
	db.mu.Unlock()

	// Run compaction - it will pick 999, try to iterate, and fail because file 999 doesn't exist
	if err := db.RunCompaction(); err == nil {
		t.Error("Expected error compacting missing file")
	}
}

func TestCompaction_CleansUpStaleIndexEntries(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024, CompactionMinGarbage: 1}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("history")

	// 1. Write v1 (TxID=1) -> File 0
	tx1 := db.NewTransaction(true)
	tx1.Put(key, []byte("v1"))
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	tx1ID := db.transactionID

	db.Checkpoint() // Rotate to File 1

	// 2. Write v2 (TxID=2) -> File 1
	tx2 := db.NewTransaction(true)
	tx2.Put(key, []byte("v2"))
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	tx2ID := db.transactionID

	db.Checkpoint() // Rotate to File 2

	// Verify both exist in Index before compaction
	if _, err := db.ldb.Get(encodeIndexKey(key, tx1ID), nil); err != nil {
		t.Fatalf("Expected v1 index entry to exist before compaction, got %v", err)
	}
	if _, err := db.ldb.Get(encodeIndexKey(key, tx2ID), nil); err != nil {
		t.Fatalf("Expected v2 index entry to exist before compaction, got %v", err)
	}

	// 3. Run Compaction
	// This should target File 0 (contains v1).
	// v1 is stale because v2 exists.
	// rewriteBatch should delete the index entry for v1.
	if err := db.RunCompaction(); err != nil {
		t.Fatal(err)
	}

	// 4. Verify Index Entries
	// v1 entry should be gone
	if _, err := db.ldb.Get(encodeIndexKey(key, tx1ID), nil); err == nil {
		t.Error("Stale index entry (v1) was NOT removed during compaction")
	}

	// v2 entry should remain
	if _, err := db.ldb.Get(encodeIndexKey(key, tx2ID), nil); err != nil {
		t.Error("Active index entry (v2) was wrongly removed or missing")
	}
}

func TestCompaction_PrunesVersionChains(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024, CompactionMinGarbage: 1}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("chain")

	// 1. Create a chain of 5 versions in File 0
	for i := 0; i < 5; i++ {
		tx := db.NewTransaction(true)
		tx.Put(key, []byte(fmt.Sprintf("v%d", i)))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// Latest is v4.

	// Force rotation so File 0 is eligible for compaction
	db.Checkpoint()

	// 2. Verify Index has all 5 versions (Simulating the "Long Chain")
	count := 0
	iter := db.ldb.NewIterator(nil, nil)
	seekKey := encodeIndexKey(key, math.MaxUint64)
	if iter.Seek(seekKey) {
		for iter.Valid() {
			foundKey := iter.Key()
			uKey, _, _ := decodeIndexKey(foundKey)
			if string(uKey) != string(key) {
				break
			}
			count++
			iter.Next()
		}
	}
	iter.Release()
	if count != 5 {
		t.Errorf("Expected 5 versions in chain before compaction, got %d", count)
	}

	// 3. Run Compaction on File 0
	if err := db.RunCompaction(); err != nil {
		t.Fatal(err)
	}

	// 4. Verify Index only has 1 version now (The latest)
	count = 0
	iter = db.ldb.NewIterator(nil, nil)
	if iter.Seek(seekKey) {
		for iter.Valid() {
			foundKey := iter.Key()
			uKey, _, _ := decodeIndexKey(foundKey)
			if string(uKey) != string(key) {
				break
			}
			count++
			iter.Next()
		}
	}
	iter.Release()

	if count != 1 {
		t.Errorf("Expected 1 version in chain after compaction, got %d", count)
	}

	// Verify it is the correct value
	tx := db.NewTransaction(false)
	val, err := tx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v4" {
		t.Errorf("Expected v4, got %s", val)
	}
}

// TestCompaction_BatchFlushLimit triggers the batch flush logic in RunCompaction.
// RunCompaction has a batch limit of 1000 entries (maxBatchCount).
// We write 1500 valid entries, ensure they are kept valid, and run compaction.
// This forces rewriteBatch to be called mid-loop.
func TestCompaction_BatchFlushLimit(t *testing.T) {
	dir := t.TempDir()
	// MinGarbage 1 ensures compaction runs on any file with garbage.
	opts := Options{CompactionMinGarbage: 1}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Write 1500 items into File 0.
	count := 1500
	for i := 0; i < count; i++ {
		tx := db.NewTransaction(true)
		// Use small values so we don't hit the byte limit (2MB) first
		tx.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte("val"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Force rotation to seal File 0
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 2. Make File 0 eligible for compaction by invalidating a few entries.
	// We update the first 50 keys.
	// File 0: 50 garbage, 1450 valid.
	// Compaction loop logic expected:
	// - Iterate 1500 entries
	// - Skip 50 (stale)
	// - Buffer 1000 valid -> Flush (Reset buffer)
	// - Buffer 450 valid -> Flush at end
	for i := 0; i < 50; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte("val-new"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Checkpoint again so updates are in File 1 (or 2) and leave File 0 untouched
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 3. Run Compaction
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// 4. Verify all data is present and correct
	for i := 0; i < count; i++ {
		tx := db.NewTransaction(false)
		key := []byte(fmt.Sprintf("k-%04d", i))
		val, err := tx.Get(key)
		if err != nil {
			t.Fatalf("Key %s missing after compaction: %v", key, err)
		}

		expected := "val"
		if i < 50 {
			expected = "val-new"
		}

		if string(val) != expected {
			t.Errorf("Key %s value mismatch. Got %s, want %s", key, val, expected)
		}
		tx.Discard()
	}
}

func TestCompaction_MixedScenario_CrossCheckpoint(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		CompactionMinGarbage: 1,
		MaxWALSize:           1024 * 1024,
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Setup File 0 (Checkpoint 1)
	// Key A: Active
	// Key B: Will be updated (Deprecated)
	// Key C: Will be deleted (Removed)
	tx := db.NewTransaction(true)
	tx.Put([]byte("A"), []byte("valA_v1"))
	tx.Put([]byte("B"), []byte("valB_v1"))
	tx.Put([]byte("C"), []byte("valC_v1"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Force rotation to File 1. File 0 is now sealed.
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 2. Setup File 1 (Checkpoint 2) - Modifications
	tx = db.NewTransaction(true)
	tx.Put([]byte("B"), []byte("valB_v2")) // Updates B (v1 deprecated)
	tx.Delete([]byte("C"))                 // Deletes C (v1 removed)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Force rotation to File 2. File 1 is now sealed.
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Verify initial state before compaction
	// File 0 should have garbage because B and C were updated/deleted.
	db.mu.RLock()
	if _, ok := db.deletedBytesByFile[0]; !ok {
		t.Error("File 0 should have garbage recorded")
	}
	db.mu.RUnlock()

	// 3. Run Compaction on File 0
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// 4. Verify Data integrity
	tx = db.NewTransaction(false)
	defer tx.Discard()

	// A should exist (v1) - preserved
	val, err := tx.Get([]byte("A"))
	if err != nil {
		t.Fatalf("Key A missing: %v", err)
	}
	if string(val) != "valA_v1" {
		t.Errorf("Key A mismatch: got %s, want valA_v1", val)
	}

	// B should exist (v2) - active version preserved
	val, err = tx.Get([]byte("B"))
	if err != nil {
		t.Fatalf("Key B missing: %v", err)
	}
	if string(val) != "valB_v2" {
		t.Errorf("Key B mismatch: got %s, want valB_v2", val)
	}

	// C should be deleted
	_, err = tx.Get([]byte("C"))
	if err != ErrKeyNotFound {
		t.Errorf("Key C should be not found, got %v", err)
	}

	// 5. Verify File 0 Deletion
	vlog0 := filepath.Join(dir, "vlog", fmt.Sprintf("%04d.vlog", 0))
	if _, err := os.Stat(vlog0); !os.IsNotExist(err) {
		t.Errorf("File 0 should have been deleted, exists at %s", vlog0)
	}
}
