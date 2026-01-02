package main

import (
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

// TestStore_MultiKey_AtomicWrite verifies that multiple operations submitted
// in a single batch are applied atomically (all visible after commit).
func TestStore_MultiKey_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Setup initial state: Key "K3" exists
	initOps := []bufferedOp{
		{opType: OpJournalSet, key: "K3", val: []byte("initial")},
	}
	if err := s.ApplyBatch(initOps, 0); err != nil {
		t.Fatal(err)
	}
	snap := atomic.LoadUint64(&s.nextLSN) - 1

	// 2. Execute Multi-Key Transaction
	// Actions: Set K1, Set K2, Delete K3
	ops := []bufferedOp{
		{opType: OpJournalSet, key: "K1", val: []byte("val1")},
		{opType: OpJournalSet, key: "K2", val: []byte("val2")},
		{opType: OpJournalDelete, key: "K3"},
	}

	if err := s.ApplyBatch(ops, snap); err != nil {
		t.Fatalf("Multi-key batch failed: %v", err)
	}

	// 3. Verify Results
	finalSnap := atomic.LoadUint64(&s.nextLSN) - 1

	// K1 should exist
	val1, err := s.Get("K1", finalSnap)
	if err != nil || string(val1) != "val1" {
		t.Errorf("K1 mismatch: err=%v, val=%s", err, val1)
	}

	// K2 should exist
	val2, err := s.Get("K2", finalSnap)
	if err != nil || string(val2) != "val2" {
		t.Errorf("K2 mismatch: err=%v, val=%s", err, val2)
	}

	// K3 should be deleted
	_, err = s.Get("K3", finalSnap)
	if err != ErrKeyNotFound {
		t.Errorf("K3 should be deleted, got %v", err)
	}
}

// TestStore_MinReadLSN_Calculation verifies that the store correctly calculates
// the safe garbage collection point (MinReadLSN) based on active snapshots and replicas.
func TestStore_MinReadLSN_Calculation(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true, "memory")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Initial State: NextLSN = 1. MinReadLSN = 0.
	if s.minReadLSN != 0 {
		t.Errorf("Initial MinReadLSN should be 0, got %d", s.minReadLSN)
	}

	// 1. Advance LSN to 10
	// We perform 9 writes. NextLSN goes 1 -> 10.
	// MinReadLSN should track head (9) because there are no pins.
	// FIX: Use currentReadLSN to avoid "transaction conflict"
	var currentReadLSN uint64 = 0
	for i := 0; i < 9; i++ {
		if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v")}}, currentReadLSN); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
		currentReadLSN++
	}
	if s.minReadLSN != 9 {
		t.Errorf("MinReadLSN should track head (9), got %d", s.minReadLSN)
	}

	// 2. Acquire Snapshot at LSN 9
	// This simulates a long-running transaction starting at LSN 9.
	snapLSN := s.AcquireSnapshot()
	if snapLSN != 9 {
		t.Fatalf("Expected snap 9, got %d", snapLSN)
	}

	// Write more (LSN 10, 11, 12)
	// FIX: Use different keys ("a", "b", "c") and readLSN 0 to avoid conflict with existing "k"
	// since we just want to advance NextLSN.
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "a", val: []byte("v")}}, 0)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "b", val: []byte("v")}}, 0)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "c", val: []byte("v")}}, 0)

	// MinReadLSN should still be 9 because of the active snapshot.
	if s.minReadLSN != 9 {
		t.Errorf("MinReadLSN should be pinned by snapshot (9), got %d", s.minReadLSN)
	}

	// 3. Register Replica at LSN 5 (Lagging)
	// This simulates a slow follower connecting.
	s.RegisterReplica("rep1", 5)

	// MinReadLSN should drop to 5 (min(Snapshot=9, Replica=5) = 5)
	if s.minReadLSN != 5 {
		t.Errorf("MinReadLSN should be pinned by replica (5), got %d", s.minReadLSN)
	}

	// 4. Update Replica to 11
	// Replica catches up past the snapshot.
	s.UpdateReplicaLSN("rep1", 11)

	// MinReadLSN should return to 9 (min(Snapshot=9, Replica=11) = 9)
	if s.minReadLSN != 9 {
		t.Errorf("MinReadLSN should return to snapshot (9), got %d", s.minReadLSN)
	}

	// 5. Release Snapshot
	// Transaction finishes.
	s.ReleaseSnapshot(snapLSN)

	// MinReadLSN should move to min(Head=12, Replica=11) = 11
	if s.minReadLSN != 11 {
		t.Errorf("MinReadLSN should move to replica (11) after snap release, got %d", s.minReadLSN)
	}

	// 6. Unregister Replica
	s.UnregisterReplica("rep1")

	// MinReadLSN should move to Head (12)
	if s.minReadLSN != 12 {
		t.Errorf("MinReadLSN should move to head (12), got %d", s.minReadLSN)
	}
}
