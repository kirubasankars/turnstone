package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestStore_Recover_Basic verifies that the store can recover data from the WAL
// after a clean shutdown using the default MemoryIndex.
func TestStore_Recover_Basic(t *testing.T) {
	dir := t.TempDir()
	// Use discard logger to keep test output clean
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Initialize Store and write data
	s1, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatalf("Failed to create initial store: %v", err)
	}

	keys := []string{"alpha", "beta", "gamma"}
	// We must track the LSN to simulate a client reading the latest state
	currentLSN := atomic.LoadUint64(&s1.nextLSN) - 1

	for _, k := range keys {
		op := bufferedOp{
			opType: OpJournalSet,
			key:    k,
			val:    []byte("val-" + k),
		}
		// Write individually to simulate sequence
		// Use currentLSN to avoid conflicts (though different keys usually don't conflict,
		// maintaining good hygiene is important)
		if err := s1.ApplyBatch([]bufferedOp{op}, currentLSN); err != nil {
			t.Fatalf("ApplyBatch failed for key %s: %v", k, err)
		}
		// Advance LSN view after successful write
		currentLSN = atomic.LoadUint64(&s1.nextLSN) - 1
	}

	// Close s1. This triggers s1.done -> flush() -> persists data to WAL.
	if err := s1.Close(); err != nil {
		t.Fatalf("Failed to close store 1: %v", err)
	}

	// 2. Re-open Store (Trigger Recover)
	s2, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatalf("Failed to create recovered store: %v", err)
	}
	defer s2.Close()

	// 3. Verify Data
	stats := s2.Stats()
	if stats.KeyCount != 3 {
		t.Errorf("Expected 3 keys, got %d", stats.KeyCount)
	}

	for _, k := range keys {
		val, err := s2.Get(k, s2.nextLSN)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", k, err)
		}
		expected := "val-" + k
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", k, expected, val)
		}
	}
}

// TestStore_Recover_Corruption verifies that the store detects corrupted/partial writes
// at the end of the WAL and truncates them if allowTruncate is true.
func TestStore_Recover_Corruption(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Write valid data
	s1, _ := NewStore(dir, logger, true, 0, true, "memory")
	op := bufferedOp{opType: OpJournalSet, key: "valid", val: []byte("data")}
	s1.ApplyBatch([]bufferedOp{op}, 0)
	s1.Close()

	// 2. Corrupt the WAL by appending garbage bytes
	walPath := filepath.Join(dir, "values.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("Failed to open wal for corruption: %v", err)
	}
	// Append 4 bytes of garbage (not enough for a header, or invalid CRC)
	if _, err := f.Write([]byte{0xBA, 0xAD, 0xF0, 0x0D}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// 3. Recover with allowTruncate = true
	s2, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatalf("Store failed to recover from corruption: %v", err)
	}
	defer s2.Close()

	// 4. Verify valid data persists
	val, err := s2.Get("valid", 100)
	if err != nil {
		t.Errorf("Failed to retrieve valid key after corruption recovery: %v", err)
	}
	if string(val) != "data" {
		t.Errorf("Expected 'data', got '%s'", string(val))
	}

	// 5. Verify garbage is gone (implicit: Store opened successfully)
}

// TestStore_Recover_FastRecovery verifies the optimization where the store skips
// scanning the full WAL if persistent state (NextLSN/Offset) is found in the Index.
func TestStore_Recover_FastRecovery(t *testing.T) {
	// This test requires LevelDB to persist the state
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Initialize with LevelDB
	s1, err := NewStore(dir, logger, true, 0, true, "leveldb")
	if err != nil {
		t.Skipf("Skipping LevelDB test (dependency missing or init failed): %v", err)
	}

	// Write enough data to advance offset
	op := bufferedOp{opType: OpJournalSet, key: "fast", val: []byte("recovery")}
	if err := s1.ApplyBatch([]bufferedOp{op}, 0); err != nil {
		t.Fatal(err)
	}

	// Close s1. This calls flushBatch -> PutState(NextLSN, Offset) into LevelDB.
	// We rely on the fix in flushBatch to save this state.
	s1.Close()

	// 2. Re-open. Logic inside recover() should find the state and log "Fast recovery".
	// Since we can't easily capture logs here, we check functional correctness
	// and rely on internal white-box checks if needed.
	start := time.Now()
	s2, err := NewStore(dir, logger, true, 0, true, "leveldb")
	if err != nil {
		t.Fatalf("Failed to re-open store: %v", err)
	}
	defer s2.Close()
	duration := time.Since(start)

	// 3. Verify data
	val, err := s2.Get("fast", 100)
	if err != nil {
		t.Errorf("Failed to get key in fast recovery mode: %v", err)
	}
	if string(val) != "recovery" {
		t.Errorf("Data mismatch")
	}

	// 4. Verify Internal State matches what was persisted
	// We know we wrote 1 key. WAL header (16) + Key (4) + Val (8) = 28 bytes roughly.
	if s2.offset == 0 {
		t.Error("Store offset should not be 0 after recovery")
	}
	if s2.nextLSN <= 1 {
		t.Error("NextLSN should have advanced")
	}

	t.Logf("Recovery took %v", duration)
}

// TestStore_Recover_CrashResilience verifies that if the WAL grows but state isn't saved
// (e.g. crash before flush), standard recovery still finds the data.
func TestStore_Recover_CrashResilience(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s1, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}

	// Write data
	op := bufferedOp{opType: OpJournalSet, key: "crash", val: []byte("test")}
	// ApplyBatch writes to WAL immediately, but index state is in memory only for "memory" type.
	// For "leveldb", PutState happens in flush.
	s1.ApplyBatch([]bufferedOp{op}, 0)

	// Simulate "Crash" by NOT closing s1 properly (WAL is written, but maybe state isn't persisted if we used leveldb)
	// Since we use MemoryIndex here, PutState is no-op anyway, forcing full scan.
	// We just want to ensure WAL scanning works.
	s1.wal.Close() // Hard close file handle
	// s1.index.Close() // Don't close index cleanly

	s2, err := NewStore(dir, logger, true, 0, true, "leveldb")
	if err != nil {
		t.Fatalf("Recovery failed after hard stop: %v", err)
	}
	defer s2.Close()

	val, err := s2.Get("crash", 100)
	if err != nil {
		t.Errorf("Failed to recover data after crash: %v", err)
	}
	if string(val) != "test" {
		t.Errorf("Data mismatch")
	}
}

// TestStore_Recover_MinReadLSN verifies that MinReadLSN is correctly initialized
// relative to NextLSN after recovery.
func TestStore_Recover_MinReadLSN(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Initialize and write data
	s1, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}

	// Write 5 transactions.
	// Each successful ApplyBatch corresponds to one transaction and increments LSN by 1.
	// Initial NextLSN is 1.
	expectedLSN := uint64(1)
	currentReadLSN := uint64(0)

	for i := 0; i < 5; i++ {
		op := bufferedOp{opType: OpJournalSet, key: "k", val: []byte("v")}

		// FIX: Use currentReadLSN to avoid "transaction conflict"
		// because we are writing to the SAME key "k" repeatedly.
		if err := s1.ApplyBatch([]bufferedOp{op}, currentReadLSN); err != nil {
			t.Fatalf("ApplyBatch failed on iter %d: %v", i, err)
		}

		// After write, update our view of the world
		currentReadLSN = atomic.LoadUint64(&s1.nextLSN) - 1
		expectedLSN++
	}

	if s1.nextLSN != expectedLSN {
		t.Errorf("Pre-close NextLSN mismatch. Expected %d, got %d", expectedLSN, s1.nextLSN)
	}

	// Close s1 to flush and stop.
	s1.Close()

	// 2. Re-open
	s2, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// 3. Verify
	// Upon recovery, NextLSN should be restored to expectedLSN.
	if s2.nextLSN != expectedLSN {
		t.Errorf("Post-recovery NextLSN mismatch. Expected %d, got %d", expectedLSN, s2.nextLSN)
	}

	// MinReadLSN should be NextLSN - 1
	expectedMinRead := expectedLSN - 1
	if s2.minReadLSN != expectedMinRead {
		t.Errorf("MinReadLSN mismatch. Expected %d, got %d", expectedMinRead, s2.minReadLSN)
	}
}

// TestStore_CRUD verifies the basic Set, Get, and Delete operations individually.
func TestStore_CRUD(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	// 1. SET (Put)
	key := "mykey"
	val1 := []byte("value1")
	opSet := bufferedOp{opType: OpJournalSet, key: key, val: val1}

	// Initial write can use 0
	if err := s.ApplyBatch([]bufferedOp{opSet}, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get current snapshot LSN for verification
	snapLSN := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. GET
	got, err := s.Get(key, snapLSN)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(val1) {
		t.Errorf("Get mismatch. Want %s, got %s", val1, got)
	}

	// 3. SET (Update/Overwrite)
	// FIX: Must use snapLSN (latest) to avoid conflict
	val2 := []byte("value2")
	opUpdate := bufferedOp{opType: OpJournalSet, key: key, val: val2}
	if err := s.ApplyBatch([]bufferedOp{opUpdate}, snapLSN); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	snapLSN = atomic.LoadUint64(&s.nextLSN) - 1

	got2, err := s.Get(key, snapLSN)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if string(got2) != string(val2) {
		t.Errorf("Get update mismatch. Want %s, got %s", val2, got2)
	}

	// 4. DEL
	// FIX: Use latest snapshot
	opDel := bufferedOp{opType: OpJournalDelete, key: key}
	if err := s.ApplyBatch([]bufferedOp{opDel}, snapLSN); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	snapLSN = atomic.LoadUint64(&s.nextLSN) - 1

	// 5. GET (Verify Deleted)
	_, err = s.Get(key, snapLSN)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after delete, got %v", err)
	}
}

// TestStore_SnapshotIsolation_Get verifies that a reader sees the state of the world
// at the specific LSN they requested, unaffected by subsequent writes.
func TestStore_SnapshotIsolation_Get(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := "iso_key"

	// 1. Initial State: LSN 1
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("v1")}}, 0); err != nil {
		t.Fatal(err)
	}
	snapV1 := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. Update State: LSN 2
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("v2")}}, snapV1); err != nil {
		t.Fatal(err)
	}
	snapV2 := atomic.LoadUint64(&s.nextLSN) - 1

	// 3. Verify Snapshot V1 (Should still see "v1")
	val, err := s.Get(key, snapV1)
	if err != nil {
		t.Fatalf("Failed to read snapshot v1: %v", err)
	}
	if string(val) != "v1" {
		t.Errorf("Snapshot Isolation Violation. SnapV1: Want 'v1', got '%s'", val)
	}

	// 4. Verify Snapshot V2 (Should see "v2")
	val, err = s.Get(key, snapV2)
	if err != nil {
		t.Fatalf("Failed to read snapshot v2: %v", err)
	}
	if string(val) != "v2" {
		t.Errorf("Snapshot Isolation Violation. SnapV2: Want 'v2', got '%s'", val)
	}
}

// TestStore_SnapshotIsolation_Set verifies that a write operation fails if the
// data has changed since the snapshot was taken (Write Skew / Update Conflict).
func TestStore_SnapshotIsolation_Set(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := "conflict_key"

	// 1. Initial Write
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("initial")}}, 0)
	snapOriginal := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. Concurrent Write (The "Interfering" Transaction)
	// This advances the version of 'key'
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("interfering")}}, snapOriginal); err != nil {
		t.Fatal(err)
	}

	// 3. Stale Write (The "Victim" Transaction)
	// Tries to write based on 'snapOriginal', but 'key' is already newer than snapOriginal.
	err = s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("stale")}}, snapOriginal)

	if err != ErrConflict {
		t.Errorf("Expected ErrConflict for stale write, got: %v", err)
	}
}

// TestStore_SnapshotIsolation_Del verifies that a delete operation fails if the
// data has changed since the snapshot was taken.
func TestStore_SnapshotIsolation_Del(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := "del_conflict_key"

	// 1. Initial Write
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("alive")}}, 0)
	snapOriginal := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. Concurrent Update
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: []byte("updated")}}, snapOriginal); err != nil {
		t.Fatal(err)
	}

	// 3. Stale Delete
	// Tries to delete based on 'snapOriginal', but 'key' has been updated since then.
	err = s.ApplyBatch([]bufferedOp{{opType: OpJournalDelete, key: key}}, snapOriginal)

	if err != ErrConflict {
		t.Errorf("Expected ErrConflict for stale delete, got: %v", err)
	}
}

// TestStore_Isolation_StrictReadDependency verifies that two concurrent transactions
// declaring a read dependency on the same key (via OpJournalGet) conflict if processed
// in the same batch, enforcing strict serialization for read-modify-write patterns.
func TestStore_Isolation_StrictReadDependency(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Initial State: Key A exists.
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "A", val: []byte("valA")}}, 0); err != nil {
		t.Fatal(err)
	}
	snap := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. Tx1: Reads A, Writes B.
	tx1Ops := []bufferedOp{
		{opType: OpJournalGet, key: "A"},
		{opType: OpJournalSet, key: "B", val: []byte("valB")},
	}

	// 3. Tx2: Reads A, Writes C.
	tx2Ops := []bufferedOp{
		{opType: OpJournalGet, key: "A"},
		{opType: OpJournalSet, key: "C", val: []byte("valC")},
	}

	// 4. Execution
	// We execute them concurrently to force them into the pending queue (10ms batch window).
	// The store's internal conflict check iterates pending ops.
	// If Tx1 is pending, its accessMap has "A".
	// When Tx2 arrives, it checks pending. It sees "A" in Tx1's accessMap.
	// Since Tx2 also needs "A" (in its accessMap), it returns ErrConflict.

	errCh := make(chan error, 2)
	go func() { errCh <- s.ApplyBatch(tx1Ops, snap) }()
	go func() { errCh <- s.ApplyBatch(tx2Ops, snap) }()

	err1 := <-errCh
	err2 := <-errCh

	// 5. Verification
	success := 0
	conflict := 0
	if err1 == nil {
		success++
	} else if err1 == ErrConflict {
		conflict++
	}
	if err2 == nil {
		success++
	} else if err2 == ErrConflict {
		conflict++
	}

	if success != 1 || conflict != 1 {
		t.Errorf("Expected 1 success and 1 conflict due to common read dependency 'A'. Got Success: %d, Conflict: %d", success, conflict)
	}
}

// TestStore_MinReadLSN_Lifecycle verifies that MinReadLSN correctly tracks
// the oldest needed LSN, pinning it during active snapshots and advancing it otherwise.
func TestStore_MinReadLSN_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Helper to safely get MinReadLSN
	getMinSafe := func() uint64 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.minReadLSN
	}

	// 1. Initial State
	// NextLSN=1 (0 reserved), MinReadLSN=0
	if val := getMinSafe(); val != 0 {
		t.Errorf("Initial MinReadLSN mismatch. Want 0, got %d", val)
	}

	// 2. Perform Write 1
	// NextLSN -> 2, MinReadLSN -> 1 (Previous LSN)
	op1 := bufferedOp{opType: OpJournalSet, key: "k1", val: []byte("v1")}
	if err := s.ApplyBatch([]bufferedOp{op1}, 0); err != nil {
		t.Fatal(err)
	}

	if val := getMinSafe(); val != 1 {
		t.Errorf("After Write 1: MinReadLSN mismatch. Want 1, got %d", val)
	}

	// 3. Pin Snapshot
	// This simulates starting a long-running transaction (READ ONLY or otherwise)
	// Snapshot LSN should be 1 (current state).
	snapLSN := s.AcquireSnapshot()
	if snapLSN != 1 {
		t.Errorf("Acquired snapshot mismatch. Want 1, got %d", snapLSN)
	}

	// 4. Perform Write 2 (Advance Global LSN)
	// NextLSN -> 3. Normally MinReadLSN would go to 2, but it should be pinned at 1.
	op2 := bufferedOp{opType: OpJournalSet, key: "k2", val: []byte("v2")}
	// Pass snapLSN (1) to conflict checker.
	if err := s.ApplyBatch([]bufferedOp{op2}, snapLSN); err != nil {
		t.Fatal(err)
	}

	if val := getMinSafe(); val != 1 {
		t.Errorf("Pinned: MinReadLSN mismatch. Want 1, got %d", val)
	}

	// 5. Perform Write 3
	// NextLSN -> 4. MinReadLSN pinned at 1.
	op3 := bufferedOp{opType: OpJournalSet, key: "k3", val: []byte("v3")}
	if err := s.ApplyBatch([]bufferedOp{op3}, snapLSN); err != nil {
		t.Fatal(err)
	}

	if val := getMinSafe(); val != 1 {
		t.Errorf("Pinned (2): MinReadLSN mismatch. Want 1, got %d", val)
	}

	// 6. Release Snapshot
	// MinReadLSN should now jump to (NextLSN - 1) which is 3.
	s.ReleaseSnapshot(snapLSN)

	// ReleaseSnapshot is synchronous regarding state update.
	expected := atomic.LoadUint64(&s.nextLSN) - 1
	if val := getMinSafe(); val != expected {
		t.Errorf("Released: MinReadLSN mismatch. Want %d, got %d", expected, val)
	}
}

// TestStore_Snapshot_RefCounting verifies that acquiring and releasing snapshots
// correctly manages the reference counts in activeSnapshots.
func TestStore_Snapshot_RefCounting(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Initial State: NextLSN=1. Snapshot LSN should be 0.
	snap1 := s.AcquireSnapshot()
	if snap1 != 0 {
		t.Errorf("Expected snapshot 0, got %d", snap1)
	}

	// Verify internal ref count is 1
	s.mu.RLock()
	count, ok := s.activeSnapshots[snap1]
	s.mu.RUnlock()
	if !ok || count != 1 {
		t.Errorf("Ref count mismatch. Want 1, got %d (exists: %v)", count, ok)
	}

	// Acquire same snapshot again
	snap2 := s.AcquireSnapshot()
	if snap2 != snap1 {
		t.Errorf("Expected snapshot %d, got %d", snap1, snap2)
	}

	// Verify internal ref count is 2
	s.mu.RLock()
	count, ok = s.activeSnapshots[snap1]
	s.mu.RUnlock()
	if !ok || count != 2 {
		t.Errorf("Ref count mismatch. Want 2, got %d", count)
	}

	// Release first reference
	s.ReleaseSnapshot(snap1)
	s.mu.RLock()
	count, ok = s.activeSnapshots[snap1]
	s.mu.RUnlock()
	if !ok || count != 1 {
		t.Errorf("Ref count mismatch after release. Want 1, got %d", count)
	}

	// Release second reference -> Should clear map entry
	s.ReleaseSnapshot(snap2)
	s.mu.RLock()
	_, ok = s.activeSnapshots[snap1]
	s.mu.RUnlock()
	if ok {
		t.Error("Snapshot entry should be removed from activeSnapshots when count reaches 0")
	}

	// Advance state to get a different snapshot
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v")}}, 0)

	snap3 := s.AcquireSnapshot()
	if snap3 == snap1 {
		t.Errorf("Expected new snapshot LSN, got same %d", snap3)
	}

	s.mu.RLock()
	count, ok = s.activeSnapshots[snap3]
	s.mu.RUnlock()
	if !ok || count != 1 {
		t.Errorf("Ref count for new snapshot mismatch. Want 1, got %d", count)
	}
	s.ReleaseSnapshot(snap3)
}

// TestStore_Close verifies that the store correctly shuts down,
// releasing resources and rejecting new operations.
func TestStore_Close(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Use memory index to focus on Store logic (WAL closure etc)
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Write a key to ensure state exists
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "test", val: []byte("val")}}, 0); err != nil {
		t.Fatal(err)
	}

	// Perform Close
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 1. Verify Read fails (WAL is closed)
	// We pass LSN 1 (valid for the key we wrote)
	val, err := s.Get("test", 1)
	if err == nil {
		t.Errorf("Read after Close: expected error, got val '%s'", string(val))
	} else {
		t.Logf("Read after close correctly returned error: %v", err)
	}

	// 2. Verify Write attempts fail
	// Note: Due to select{} race between opsChannel send and done channel receive,
	// this might return ErrClosed OR ErrTransactionTimeout if the channel isn't full.
	// However, properly closed stores should reject writes.
	err = s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "after", val: []byte("val")}}, 0)
	if err == nil {
		t.Error("Write after Close: expected error, got nil")
	} else if err != ErrClosed && err != ErrTransactionTimeout {
		t.Errorf("Write after Close: expected ErrClosed or Timeout, got %v", err)
	}
}

// TestStore_Compaction verifies that the compaction process moves live data
// from the tail of the WAL to the head and advances the compactedOffset.
func TestStore_Compaction(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Use memory index for simplicity
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	// 1. Write a key we want to survive compaction ("Live Data")
	key := "survivor"
	val := []byte("important_data")
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: key, val: val}}, 0); err != nil {
		t.Fatal(err)
	}

	// Capture original location
	s.mu.RLock()
	entryBefore, ok := s.index.Get(key, s.nextLSN)
	s.mu.RUnlock()
	if !ok {
		t.Fatal("Key not found before compaction")
	}

	// 2. Write enough data to push 'survivor' into the compaction zone.
	// Compaction requires: safeEndOffset := s.offset - (10 * 1024 * 1024) > startOffset (0)
	// We need s.offset > 10MB. Let's write ~12MB.
	payload := make([]byte, 1024*1024) // 1MB payload
	payload[0] = 1                     // Non-zero to ensure it's not treated as a hole

	// We need valid LSNs to avoid conflicts
	currentLSN := atomic.LoadUint64(&s.nextLSN) - 1

	for i := 0; i < 12; i++ {
		k := "filler"
		if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: k, val: payload}}, currentLSN); err != nil {
			t.Fatalf("Filler write failed: %v", err)
		}
		currentLSN = atomic.LoadUint64(&s.nextLSN) - 1
	}

	// 3. Trigger Compaction
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact call failed: %v", err)
	}

	// 4. Wait for Compaction to finish
	// We poll `compactedOffset`. It runs asynchronously.
	timeout := time.After(10 * time.Second) // Give it plenty of time for I/O
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	done := false
	for !done {
		select {
		case <-timeout:
			t.Fatal("Timed out waiting for compaction")
		case <-ticker.C:
			s.mu.RLock()
			compacted := s.compactedOffset
			s.mu.RUnlock()

			// We expect compactedOffset to pass the original entry.
			// entryBefore.Offset is likely near 0.
			if compacted > entryBefore.Offset+int64(entryBefore.Length) {
				done = true
			}
		}
	}

	// 5. Verify 'survivor' was moved
	s.mu.RLock()
	entryAfter, ok := s.index.Get(key, s.nextLSN)
	s.mu.RUnlock()
	if !ok {
		t.Fatal("Key lost after compaction")
	}

	if entryAfter.Offset == entryBefore.Offset {
		t.Errorf("Compaction did not move the key. Old Off: %d, New Off: %d", entryBefore.Offset, entryAfter.Offset)
	}
	if entryAfter.Offset < entryBefore.Offset {
		t.Error("New offset is smaller than old offset? Should be appended to end.")
	}

	// 6. Verify Data Integrity
	valRead, err := s.Get(key, atomic.LoadUint64(&s.nextLSN)-1)
	if err != nil {
		t.Errorf("Failed to read key after compaction: %v", err)
	}
	if string(valRead) != string(val) {
		t.Errorf("Data corruption. Want %s, got %s", val, valRead)
	}
}

// TestStore_RunLoop_ConcurrentWrites stresses the runLoop and flushBatch mechanisms
// by launching multiple goroutines writing unique keys simultaneously.
func TestStore_RunLoop_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	concurrency := 10
	opsPerRoutine := 100
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			// Each routine uses its own keys, so no conflicts
			for j := 0; j < opsPerRoutine; j++ {
				key := fmt.Sprintf("k-%d-%d", id, j)
				val := []byte(fmt.Sprintf("v-%d-%d", id, j))

				// Must read latest snapshot to avoid RW conflict with previous committed batches
				snap := atomic.LoadUint64(&s.nextLSN) - 1

				// Retry loop for potential conflicts if snapshot advances quickly
				for {
					op := bufferedOp{opType: OpJournalSet, key: key, val: val}
					err := s.ApplyBatch([]bufferedOp{op}, snap)
					if err == nil {
						break
					}
					if err == ErrConflict {
						// Update snapshot and retry
						snap = atomic.LoadUint64(&s.nextLSN) - 1
						continue
					}
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(i)
	}

	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Concurrent write failed: %v", err)
		}
	}

	// Verify count
	stats := s.Stats()
	expected := concurrency * opsPerRoutine
	if stats.KeyCount != expected {
		t.Errorf("Key count mismatch. Want %d, got %d", expected, stats.KeyCount)
	}
}

// TestStore_RunLoop_PendingConflict verifies that the runLoop detects conflicts
// within the current batch (Write-Write conflict) before flushing.
func TestStore_RunLoop_PendingConflict(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := "hot_key"

	// Start two goroutines trying to write the exact same key at the same time.
	// We use 0 as readLSN for simplicity (initial state).

	resCh := make(chan error, 2)

	// Helper to send write
	doWrite := func() {
		op := bufferedOp{opType: OpJournalSet, key: key, val: []byte("val")}
		// We deliberately use the SAME snapshot LSN (0) to trigger conflict check.
		// Even if LSN check passes, the Pending-Check in runLoop should catch the second one
		// if they arrive within the same BatchDelay window.
		resCh <- s.ApplyBatch([]bufferedOp{op}, 0)
	}

	// Launch writes close together
	go doWrite()
	go doWrite()

	err1 := <-resCh
	err2 := <-resCh

	// One should succeed, one should fail with ErrConflict.
	// It is possible (though unlikely with small BatchDelay) that they fall into different batches.
	// If they fall into different batches, the second one might fail due to stale snapshot (RW conflict)
	// or succeed if we updated the snapshot (but we hardcoded 0).
	// If the first succeeds, it advances LSN to 1. The second (using LSN 0) will fail RW conflict.
	// If they are in the SAME batch, the second fails WW conflict.
	// In ALL cases, exactly one should succeed given they use the same base LSN.

	successCount := 0
	conflictCount := 0

	check := func(err error) {
		if err == nil {
			successCount++
		} else if err == ErrConflict {
			conflictCount++
		} else {
			t.Errorf("Unexpected error: %v", err)
		}
	}
	check(err1)
	check(err2)

	if successCount != 1 {
		t.Errorf("Expected exactly 1 success, got %d", successCount)
	}
	if conflictCount != 1 {
		t.Errorf("Expected exactly 1 conflict, got %d", conflictCount)
	}
}

// TestStore_ApplyBatch_Validation verifies input validation in ApplyBatch.
func TestStore_ApplyBatch_Validation(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Test empty batch (should theoretically be no-op or handled gracefully)
	// ApplyBatch logic iterates ops to build accessMap. Empty ops -> empty map.
	// Then sends to channel. flushBatch checks len(activeReqs).
	// An empty batch might technically pass through but do nothing.
	// The implementation of ApplyBatch doesn't explicitly reject empty ops,
	// but let's see if it causes issues.
	if err := s.ApplyBatch([]bufferedOp{}, 0); err != nil {
		t.Errorf("Empty batch should not error, got %v", err)
	}

	// 2. Test Get op in ApplyBatch (should be allowed but skipped in WAL)
	// This mimics a "Read-Your-Own-Writes" transaction buffer being flushed,
	// where Gets are ignored by the store but Sets/Dels are persisted.
	ops := []bufferedOp{
		{opType: OpJournalSet, key: "k1", val: []byte("v1")},
		{opType: OpJournalGet, key: "k1"}, // Should be ignored by flushBatch
	}
	if err := s.ApplyBatch(ops, 0); err != nil {
		t.Errorf("Batch with GET should succeed, got %v", err)
	}

	// Verify only k1 set is persisted (Get is not persisted)
	// Access index directly or check stats
	val, _ := s.Get("k1", s.nextLSN)
	if string(val) != "v1" {
		t.Error("Set operation in mixed batch failed")
	}
}

// TestStore_Batch_Conflict_Mixed ensures that conflict detection works
// even when the conflicting key is not the most recent addition to the pending batch.
// It verifies that the conflict checker scans the entire pending list.
func TestStore_Batch_Conflict_Mixed(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// We rely on BatchDelay (10ms) to keep the batch open.
	// We execute operations rapidly to ensure they land in the same batch processing window.

	// Scenario:
	// 1. Send Req A1 (Key="A") -> Should Succeed (enters pending)
	// 2. Send Req B  (Key="B") -> Should Succeed (enters pending, distinct key)
	// 3. Send Req A2 (Key="A") -> Should Fail (Conflict with A1 in pending)

	errCh := make(chan error, 3)
	snap := atomic.LoadUint64(&s.nextLSN) - 1

	go func() {
		errCh <- s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "A", val: []byte("1")}}, snap)
	}()
	go func() {
		// Use a tiny sleep to nudge ordering, though runLoop arrival order is what matters.
		time.Sleep(100 * time.Microsecond)
		errCh <- s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "B", val: []byte("2")}}, snap)
	}()
	go func() {
		time.Sleep(500 * time.Microsecond) // Ensure this arrives last
		errCh <- s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "A", val: []byte("3")}}, snap)
	}()

	var errs []error
	for i := 0; i < 3; i++ {
		errs = append(errs, <-errCh)
	}

	successCount := 0
	conflictCount := 0

	for _, err := range errs {
		if err == nil {
			successCount++
		} else if err == ErrConflict {
			conflictCount++
		} else {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// Expectation:
	// A1 succeeds.
	// B succeeds (independent).
	// A2 fails (conflicts with A1).
	// Total: 2 Success, 1 Conflict.
	if successCount != 2 {
		t.Errorf("Expected 2 successes, got %d", successCount)
	}
	if conflictCount != 1 {
		t.Errorf("Expected 1 conflict, got %d", conflictCount)
	}

	// Verify Data: Key A should be "1" (first write), Key B should be "2".
	valA, _ := s.Get("A", atomic.LoadUint64(&s.nextLSN)-1)
	if string(valA) != "1" {
		t.Errorf("Key A mismatch. Want '1', got '%s'", string(valA))
	}
	valB, _ := s.Get("B", atomic.LoadUint64(&s.nextLSN)-1)
	if string(valB) != "2" {
		t.Errorf("Key B mismatch. Want '2', got '%s'", string(valB))
	}
}

// TestStore_SerializeBatch verifies the internal serialization logic for WAL entries.
func TestStore_SerializeBatch(t *testing.T) {
	// We don't need a real store on disk, just the struct methods.
	// But NewStore is easier to get a valid struct.
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, _ := NewStore(dir, logger, true, 0, true, "memory")
	defer s.Close()

	req := &request{
		ops: []bufferedOp{
			{opType: OpJournalSet, key: "key1", val: []byte("val1")},
			{opType: OpJournalGet, key: "key1"}, // Should be skipped
			{opType: OpJournalDelete, key: "key2"},
		},
		opLens: make([]int, 0),
	}

	// Manually prep headers like ApplyBatch does, to make it realistic
	// Header size = 16
	// Set: KeyLen=4, ValLen=4
	// Del: KeyLen=4, ValLen=0

	// Op 0 (Set)
	pack0 := PackMeta(4, 4, false)
	binary.BigEndian.PutUint32(req.ops[0].header[0:4], pack0)

	// Op 2 (Del)
	pack2 := PackMeta(4, 0, true)
	binary.BigEndian.PutUint32(req.ops[2].header[0:4], pack2)

	var buf bytes.Buffer
	s.serializeBatch(req, &buf)

	// Validation
	if len(req.opLens) != 3 {
		t.Errorf("Expected 3 opLens, got %d", len(req.opLens))
	}

	// Check Op 0
	expectedLen0 := HeaderSize + 4 + 4 // 16 + 4 + 4 = 24
	if req.opLens[0] != expectedLen0 {
		t.Errorf("Op 0 length mismatch. Want %d, got %d", expectedLen0, req.opLens[0])
	}

	// Check Op 1 (Get) -> Should be 0
	if req.opLens[1] != 0 {
		t.Errorf("Op 1 (Get) length should be 0, got %d", req.opLens[1])
	}

	// Check Op 2 (Del)
	expectedLen2 := HeaderSize + 4 // 16 + 4 = 20
	if req.opLens[2] != expectedLen2 {
		t.Errorf("Op 2 length mismatch. Want %d, got %d", expectedLen2, req.opLens[2])
	}

	// Check Buffer Content
	// Total bytes = 24 + 20 = 44
	if buf.Len() != 44 {
		t.Errorf("Buffer length mismatch. Want 44, got %d", buf.Len())
	}

	data := buf.Bytes()

	// Verify Op 0 Data
	// Header (first 4 bytes match packed meta)
	if binary.BigEndian.Uint32(data[0:4]) != pack0 {
		t.Error("Op 0 header meta mismatch")
	}
	// Key at offset 16
	if string(data[16:20]) != "key1" {
		t.Error("Op 0 key mismatch")
	}
	// Val at offset 20
	if string(data[20:24]) != "val1" {
		t.Error("Op 0 val mismatch")
	}

	// Verify Op 2 Data (starts at offset 24)
	offset2 := 24
	if binary.BigEndian.Uint32(data[offset2:offset2+4]) != pack2 {
		t.Error("Op 2 header meta mismatch")
	}
	if string(data[offset2+16:offset2+20]) != "key2" {
		t.Error("Op 2 key mismatch")
	}
}

// TestStore_Replication_Quorum verifies that writes block until enough replicas acknowledge.
func TestStore_Replication_Quorum(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Initialize Store with minReplicas = 1
	s, err := NewStore(dir, logger, true, 1, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Register a replica
	replicaID := "node-2"
	s.RegisterReplica(replicaID, 0)

	// 3. Perform a write in a background goroutine (it should block)
	doneCh := make(chan error)
	go func() {
		op := bufferedOp{opType: OpJournalSet, key: "k", val: []byte("v")}
		// Use 0 as readLSN (initial)
		doneCh <- s.ApplyBatch([]bufferedOp{op}, 0)
	}()

	// 4. Verify it blocks (wait a bit)
	select {
	case err := <-doneCh:
		t.Fatalf("Write returned prematurely with err: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected to block
	}

	// 5. Update Replica LSN to acknowledge the write (LSN 1)
	// The write generates LSN 1.
	s.UpdateReplicaLSN(replicaID, 1)

	// 6. Verify write unblocks and succeeds
	select {
	case err := <-doneCh:
		if err != nil {
			t.Errorf("Write failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Write remained blocked after quorum was met")
	}
}

// TestStore_ReplicateBatch_Follower verifies that a follower applies replication batches correctly,
// preserving LSNs from the leader.
func TestStore_ReplicateBatch_Follower(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Follower store (minReplicas usually 0 for followers, but doesn't matter for ReplicateBatch)
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Create a batch of LogEntries (simulating network payload)
	entries := []LogEntry{
		{LSN: 10, OpType: OpJournalSet, Key: []byte("key10"), Value: []byte("val10")},
		{LSN: 11, OpType: OpJournalSet, Key: []byte("key11"), Value: []byte("val11")},
		{LSN: 12, OpType: OpJournalDelete, Key: []byte("key10"), Value: nil},
	}

	// Apply batch
	if err := s.ReplicateBatch(entries); err != nil {
		t.Fatalf("ReplicateBatch failed: %v", err)
	}

	// Verify Data
	// key10 should be deleted
	_, err = s.Get("key10", 12)
	if err != ErrKeyNotFound {
		t.Errorf("key10 should be deleted, got %v", err)
	}

	// key11 should exist
	val, err := s.Get("key11", 12)
	if err != nil {
		t.Errorf("key11 missing: %v", err)
	} else if string(val) != "val11" {
		t.Errorf("key11 mismatch: %s", val)
	}

	// Verify Store State
	// NextLSN should be maxLSN + 1 = 13
	if s.nextLSN != 13 {
		t.Errorf("NextLSN mismatch. Want 13, got %d", s.nextLSN)
	}
}

// TestStore_Replication_MinLSN verifies that slow replicas prevent
// MinReadLSN from advancing, effectively holding back garbage collection.
func TestStore_Replication_MinLSN(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Initial writes to advance LSN
	// LSN 1
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v")}}, 0)

	// Register Replica at LSN 1
	s.RegisterReplica("slow-node", 1)

	// LSN 2
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v2")}}, 1)
	// LSN 3
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v3")}}, 2)

	// Check MinReadLSN
	// Should be min(NextLSN-1=3, Replica=1) = 1
	s.mu.RLock()
	minLSN := s.minReadLSN
	s.mu.RUnlock()

	if minLSN != 1 {
		t.Errorf("MinReadLSN not pinned by replica. Want 1, got %d", minLSN)
	}

	// Update Replica to LSN 3
	s.UpdateReplicaLSN("slow-node", 3)

	// Check MinReadLSN
	// Should be 3
	s.mu.RLock()
	minLSN = s.minReadLSN
	s.mu.RUnlock()

	if minLSN != 3 {
		t.Errorf("MinReadLSN did not advance. Want 3, got %d", minLSN)
	}

	// Unregister
	s.UnregisterReplica("slow-node")
	// No change expected immediately as 3 is max committed, but just ensure no crash
}

// TestStore_MinReadLSN_Complex verifies MinReadLSN behavior with multiple snapshots
// and interactions between replicas and snapshots.
func TestStore_MinReadLSN_Complex(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	getMin := func() uint64 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.minReadLSN
	}

	// 1. Setup: Write to LSN 1
	// NextLSN starts at 1. Write -> NextLSN=2. Committed=1.
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v1")}}, 0); err != nil {
		t.Fatal(err)
	}
	// Min should be 1
	if m := getMin(); m != 1 {
		t.Errorf("Initial: Expected Min 1, got %d", m)
	}

	// 2. Multiple Snapshots
	snap1 := s.AcquireSnapshot() // LSN 1

	// Write LSN 2. Read snapshot is snap1 to avoid conflict.
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v2")}}, snap1); err != nil {
		t.Fatal(err)
	}
	// NextLSN=3. Committed=2.

	snap2 := s.AcquireSnapshot() // LSN 2

	// Write LSN 3. Read snapshot snap2.
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v3")}}, snap2); err != nil {
		t.Fatal(err)
	}
	// NextLSN=4. Committed=3.

	// Min should be min(Snap1=1, Snap2=2, StoreCommitted=3) = 1
	if m := getMin(); m != 1 {
		t.Errorf("Multiple Snaps: Expected Min 1, got %d", m)
	}

	// 3. Release Oldest Snapshot
	s.ReleaseSnapshot(snap1)
	// Min should advance to min(Snap2=2, StoreCommitted=3) = 2
	if m := getMin(); m != 2 {
		t.Errorf("After Release Snap1: Expected Min 2, got %d", m)
	}

	// 4. Replica Interaction
	// Register Replica at LSN 2. Min is min(Snap2=2, Replica=2, StoreCommitted=3) = 2.
	s.RegisterReplica("rep1", 2)
	if m := getMin(); m != 2 {
		t.Errorf("With Replica: Expected Min 2, got %d", m)
	}

	// Advance Store to LSN 4 (Write 4).
	// FIX: Use a different key to avoid conflict with LSN 3 update to "k"
	// while using the old snapshot 'snap2' (LSN 2).
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k_new", val: []byte("v4")}}, snap2); err != nil {
		t.Fatal(err)
	}
	// NextLSN=5. Committed=4. Min depends on Snap2(2) and Replica(2).

	// Release Snap2. Now Min depends on Replica(2).
	s.ReleaseSnapshot(snap2)
	if m := getMin(); m != 2 {
		t.Errorf("After Release Snap2 (Held by Replica): Expected Min 2, got %d", m)
	}

	// Advance Replica to LSN 4.
	// Store Committed is 4.
	// Min should be min(Replica=4, StoreCommitted=4) = 4.
	s.UpdateReplicaLSN("rep1", 4)
	if m := getMin(); m != 4 {
		t.Errorf("After Replica Update: Expected Min 4, got %d", m)
	}

	// Unregister Replica. Min should be StoreCommitted=4.
	s.UnregisterReplica("rep1")
	if m := getMin(); m != 4 {
		t.Errorf("After Unregister: Expected Min 4, got %d", m)
	}
}

// TestStore_Replica_Registry verifies the direct administration of replica slots
// (Register, Update, Unregister) and verifies internal state.
func TestStore_Replica_Registry(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := "replica-node-1"

	// 1. Add Replica
	s.RegisterReplica(id, 50)

	s.mu.RLock()
	val, ok := s.replicationSlots[id]
	total := len(s.replicationSlots)
	s.mu.RUnlock()

	if !ok {
		t.Fatalf("Replica %s not found after registration", id)
	}
	if val.LSN != 50 {
		t.Errorf("Replica LSN mismatch. Want 50, got %d", val.LSN)
	}
	if total != 1 {
		t.Errorf("Expected 1 replica, got %d", total)
	}

	// 2. Update Replica
	s.UpdateReplicaLSN(id, 60)

	s.mu.RLock()
	val = s.replicationSlots[id]
	s.mu.RUnlock()

	if val.LSN != 60 {
		t.Errorf("Replica LSN mismatch after update. Want 60, got %d", val.LSN)
	}

	// 3. Remove Replica
	s.UnregisterReplica(id)

	s.mu.RLock()
	_, ok = s.replicationSlots[id]
	total = len(s.replicationSlots)
	s.mu.RUnlock()

	if ok {
		t.Fatalf("Replica %s found after unregister", id)
	}
	if total != 0 {
		t.Errorf("Expected 0 replicas, got %d", total)
	}
}

// TestStore_FindOffsetForLSN validates the binary search logic for finding WAL offsets
// based on LSN checkpoints, including interactions with the compaction offset.
func TestStore_FindOffsetForLSN(t *testing.T) {
	// Manually construct a Store with checkpoints to test the search logic.
	// We don't need a full initialization since we are testing internal logic.
	s := &Store{
		checkpoints: []Checkpoint{
			{LSN: 10, Offset: 1000},
			{LSN: 20, Offset: 2000},
			{LSN: 30, Offset: 3000},
		},
		compactedOffset: 0,
	}

	tests := []struct {
		reqLSN uint64
		want   int64
	}{
		{0, 0},     // Before everything
		{5, 0},     // Before first checkpoint
		{9, 0},     // Just before first
		{10, 1000}, // Exact match first checkpoint
		{15, 1000}, // Between first and second
		{19, 1000}, // Just before second
		{20, 2000}, // Exact match second
		{25, 2000}, // Between second and third
		{30, 3000}, // Exact match third
		{40, 3000}, // After last checkpoint
	}

	for _, tt := range tests {
		got := s.FindOffsetForLSN(tt.reqLSN)
		if got != tt.want {
			t.Errorf("FindOffsetForLSN(%d) = %d; want %d", tt.reqLSN, got, tt.want)
		}
	}

	// Test interaction with compaction
	// Simulate that the WAL has been compacted/truncated up to offset 1500.
	s.compactedOffset = 1500

	// Now everything calculated as < 1500 should return 1500 (the floor).
	// LSN 10 corresponds to Offset 1000, which is < 1500.
	// LSN 20 corresponds to Offset 2000, which is > 1500.

	compactionTests := []struct {
		reqLSN uint64
		want   int64
	}{
		{5, 1500},  // Was 0, now clamped
		{10, 1500}, // Was 1000, now clamped
		{15, 1500}, // Was 1000, now clamped
		{20, 2000}, // > CompactedOffset, returns actual
		{25, 2000}, // > CompactedOffset, returns actual
	}

	for _, tt := range compactionTests {
		got := s.FindOffsetForLSN(tt.reqLSN)
		if got != tt.want {
			t.Errorf("Compacted FindOffsetForLSN(%d) = %d; want %d", tt.reqLSN, got, tt.want)
		}
	}
}

// TestStore_Isolation_CommonReadDependency verifies that explicitly declaring a read dependency
// on a common key causes a conflict if concurrent transactions try to commit,
// even if they write to different keys. This enforces serializability for the "Shared" key.
func TestStore_Isolation_CommonReadDependency(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Setup: Create the common dependency key "Shared"
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "Shared", val: []byte("initial")}}, 0); err != nil {
		t.Fatal(err)
	}
	snap := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. Define Tx1: Read "Shared", Write "Unique1"
	tx1 := []bufferedOp{
		{opType: OpJournalGet, key: "Shared"},
		{opType: OpJournalSet, key: "Unique1", val: []byte("v1")},
	}

	// 3. Define Tx2: Read "Shared", Write "Unique2"
	tx2 := []bufferedOp{
		{opType: OpJournalGet, key: "Shared"},
		{opType: OpJournalSet, key: "Unique2", val: []byte("v2")},
	}

	// 4. Execute concurrently to force collision in the pending batch
	errCh := make(chan error, 2)
	go func() { errCh <- s.ApplyBatch(tx1, snap) }()
	go func() { errCh <- s.ApplyBatch(tx2, snap) }()

	err1 := <-errCh
	err2 := <-errCh

	// 5. Verification: One must fail with ErrConflict because both "lock" "Shared" in the pending batch.
	success := 0
	conflict := 0

	check := func(err error) {
		if err == nil {
			success++
		} else if err == ErrConflict {
			conflict++
		} else {
			t.Errorf("Unexpected error: %v", err)
		}
	}
	check(err1)
	check(err2)

	if success != 1 || conflict != 1 {
		t.Errorf("Expected 1 success and 1 conflict. Got Success: %d, Conflict: %d", success, conflict)
	}
}
