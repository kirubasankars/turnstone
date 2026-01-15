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

// TestStore_MinReadTxID verifies snapshot isolation pins MinReadTxID.
func TestStore_MinReadTxID(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Advance TxID to 10
	var currentReadTxID uint64 = 0
	for i := 0; i < 9; i++ {
		s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v")}}, currentReadTxID)
		currentReadTxID++
	}

	// 2. Acquire Snapshot at TxID 9
	snapTxID := s.AcquireSnapshot()
	if snapTxID != 9 {
		t.Fatalf("Expected snap 9, got %d", snapTxID)
	}

	// Write more (TxID 10, 11, 12)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "a", val: []byte("v")}}, 0)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "b", val: []byte("v")}}, 0)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "c", val: []byte("v")}}, 0)

	// MinReadTxID should still be 9 because of the active snapshot.
	if s.minReadTxID != 9 {
		t.Errorf("MinReadTxID should be pinned by snapshot (9), got %d", s.minReadTxID)
	}

	// 4. Release Snapshot
	s.ReleaseSnapshot(snapTxID)

	// MinReadTxID should move to Head (12)
	if s.minReadTxID != 12 {
		t.Errorf("MinReadTxID should move to head (12), got %d", s.minReadTxID)
	}
}

func TestStore_Isolation_ReadRead(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Initial Data
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val0")}}, 0)

	// Two concurrent snapshots
	snap1 := s.AcquireSnapshot()
	snap2 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap1)
	defer s.ReleaseSnapshot(snap2)

	// Both read simultaneously
	val1, err := s.Get("key", snap1)
	if err != nil || string(val1) != "val0" {
		t.Errorf("Tx1 read failed: %v %s", err, val1)
	}
	val2, err := s.Get("key", snap2)
	if err != nil || string(val2) != "val0" {
		t.Errorf("Tx2 read failed: %v %s", err, val2)
	}
}

func TestStore_Isolation_WriteWrite_Conflict(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Initial set
	snapInit := s.AcquireSnapshot()
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val0")}}, snapInit)
	s.ReleaseSnapshot(snapInit)

	// Tx1 and Tx2 start concurrently
	snap1 := s.AcquireSnapshot()
	snap2 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap1)
	defer s.ReleaseSnapshot(snap2)

	// Tx1 Commits first
	err = s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val1")}}, snap1)
	if err != nil {
		t.Fatalf("Tx1 commit failed: %v", err)
	}

	// Tx2 Tries to Commit (Should fail because key changed since snap2 was acquired)
	err = s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val2")}}, snap2)
	if err != ErrConflict {
		t.Errorf("Tx2 expected ErrConflict, got %v", err)
	}
}

func TestStore_Isolation_ReadWrite_Snapshot(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Initial Data
	snapInit := s.AcquireSnapshot()
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val0")}}, snapInit)
	s.ReleaseSnapshot(snapInit)

	// Tx1 starts (Reading view is frozen at snap1)
	snap1 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap1)

	// Tx2 starts, writes, commits
	snap2 := s.AcquireSnapshot()
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val2")}}, snap2)
	s.ReleaseSnapshot(snap2)

	// Tx1 reads. Should see val0 (Snapshot Isolation), not val2
	val, err := s.Get("key", snap1)
	if err != nil {
		t.Fatalf("Tx1 get failed: %v", err)
	}
	if string(val) != "val0" {
		t.Errorf("Snapshot isolation violation. Want 'val0', got '%s'", string(val))
	}

	// Tx3 starts after Tx2. Should see val2
	snap3 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap3)
	val3, _ := s.Get("key", snap3)
	if string(val3) != "val2" {
		t.Errorf("Visibility error. Want 'val2', got '%s'", string(val3))
	}
}

func TestStore_Isolation_WriteRead(t *testing.T) {
	// Verifies that once a Write is committed, a subsequent Read sees it.
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Tx1 Writes
	snap1 := s.AcquireSnapshot()
	if err := s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key", val: []byte("val1")}}, snap1); err != nil {
		t.Fatal(err)
	}
	s.ReleaseSnapshot(snap1)

	// Tx2 Reads
	snap2 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap2)
	val, err := s.Get("key", snap2)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1" {
		t.Errorf("Tx2 expected val1, got %s", val)
	}
}

// TestStore_Isolation_DisjointWrites_Success verifies that two transactions
// can write to different keys concurrently as long as they don't read the modified keys.
func TestStore_Isolation_DisjointWrites_Success(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Init A=val0
	snapInit := s.AcquireSnapshot()
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "a", val: []byte("val0")}}, snapInit)
	s.ReleaseSnapshot(snapInit)

	// Tx1 and Tx2 start
	snap1 := s.AcquireSnapshot()
	snap2 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap1)
	defer s.ReleaseSnapshot(snap2)

	// Tx1: Read A, Write B (Does not modify A)
	ops1 := []bufferedOp{
		{opType: OpJournalGet, key: "a"},
		{opType: OpJournalSet, key: "b", val: []byte("val1")},
	}
	if err := s.ApplyBatch(ops1, snap1); err != nil {
		t.Fatalf("Tx1 commit failed: %v", err)
	}

	// Tx2: Read A, Write C (Does not modify A)
	// This should SUCCEED because 'a' was not modified by Tx1.
	ops2 := []bufferedOp{
		{opType: OpJournalGet, key: "a"},
		{opType: OpJournalSet, key: "c", val: []byte("val2")},
	}
	if err := s.ApplyBatch(ops2, snap2); err != nil {
		t.Errorf("Tx2 commit failed: %v", err)
	}

	// Verify final state contains both writes
	snapFinal := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snapFinal)
	valB, _ := s.Get("b", snapFinal)
	valC, _ := s.Get("c", snapFinal)

	if string(valB) != "val1" {
		t.Errorf("Expected B=val1, got %s", valB)
	}
	if string(valC) != "val2" {
		t.Errorf("Expected C=val2, got %s", valC)
	}
}

// TestStore_Isolation_WriteSkew_Failure verifies that conflicting dependencies are caught.
func TestStore_Isolation_WriteSkew_Failure(t *testing.T) {
	// Scenario: Classic Write Skew
	// T1: Read A, Read B. Write A. (Depends on state of B)
	// T2: Read A, Read B. Write B. (Depends on state of A)
	// Result: Turnstone (OCC) should detect that T2's read set (A) was modified by T1.

	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Init A=0, B=0
	snapInit := s.AcquireSnapshot()
	s.ApplyBatch([]bufferedOp{
		{opType: OpJournalSet, key: "a", val: []byte("0")},
		{opType: OpJournalSet, key: "b", val: []byte("0")},
	}, snapInit)
	s.ReleaseSnapshot(snapInit)

	// Tx1 and Tx2 start
	snap1 := s.AcquireSnapshot()
	snap2 := s.AcquireSnapshot()
	defer s.ReleaseSnapshot(snap1)
	defer s.ReleaseSnapshot(snap2)

	// Tx1: Read A, B. Write A=1
	ops1 := []bufferedOp{
		{opType: OpJournalGet, key: "a"},
		{opType: OpJournalGet, key: "b"},
		{opType: OpJournalSet, key: "a", val: []byte("1")},
	}
	if err := s.ApplyBatch(ops1, snap1); err != nil {
		t.Fatalf("Tx1 commit failed: %v", err)
	}

	// Tx2: Read A, B. Write B=1
	// This should FAIL because T2 read 'a', but T1 committed a new version of 'a'
	// after T2's snapshot was taken.
	ops2 := []bufferedOp{
		{opType: OpJournalGet, key: "a"},
		{opType: OpJournalGet, key: "b"},
		{opType: OpJournalSet, key: "b", val: []byte("1")},
	}
	err = s.ApplyBatch(ops2, snap2)

	if err == nil {
		t.Errorf("Tx2 commit succeeded, expected Write Skew conflict failure")
	} else if err != ErrConflict {
		t.Errorf("Tx2 expected ErrConflict, got %v", err)
	}
}

// TestStore_Recover_Basic verifies that the store can recover data from the WAL.
func TestStore_Recover_Basic(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Initialize Store and write data
	s1, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatalf("Failed to create initial store: %v", err)
	}

	keys := []string{"alpha", "beta", "gamma"}
	currentTxID := atomic.LoadUint64(&s1.nextTxID) - 1

	for _, k := range keys {
		op := bufferedOp{
			opType: OpJournalSet,
			key:    k,
			val:    []byte("val-" + k),
		}
		if err := s1.ApplyBatch([]bufferedOp{op}, currentTxID); err != nil {
			t.Fatalf("ApplyBatch failed for key %s: %v", k, err)
		}
		currentTxID = atomic.LoadUint64(&s1.nextTxID) - 1
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("Failed to close store 1: %v", err)
	}

	// 2. Re-open Store
	s2, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatalf("Failed to create recovered store: %v", err)
	}
	defer s2.Close()

	// 3. Verify Data
	stats := s2.Stats()
	// Fix: Expect 4 keys (3 user keys + 1 system "_id" key)
	if stats.KeyCount != 4 {
		t.Errorf("Expected 4 keys, got %d", stats.KeyCount)
	}

	for _, k := range keys {
		val, err := s2.Get(k, s2.nextTxID)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", k, err)
		}
		expected := "val-" + k
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", k, expected, val)
		}
	}
}

func TestStore_Recover_CRC_Corruption(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	walPath := filepath.Join(dir, "values.log")

	// 1. Create Store and write two entries
	s1, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	// Write Entry 1 (Valid)
	if err := s1.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key1", val: []byte("val1")}}, 0); err != nil {
		t.Fatal(err)
	}
	// Write Entry 2 (To be corrupted)
	if err := s1.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key2", val: []byte("val2")}}, 0); err != nil {
		t.Fatal(err)
	}

	s1.Close()

	// 2. Corrupt the WAL manually
	// Open file, flip the last byte. This modifies the payload of the last entry (key2),
	// causing a CRC mismatch because the header's CRC won't match the modified payload.
	f, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	stat, _ := f.Stat()
	size := stat.Size()

	// Read last byte
	b := make([]byte, 1)
	f.ReadAt(b, size-1)

	// Flip bit
	b[0] ^= 0xFF

	// Write back
	if _, err := f.WriteAt(b, size-1); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// FORCE FULL RECOVERY:
	// The previous Store (s1) persisted its index state on Close().
	// Because the file size hasn't changed (we only flipped a bit), the new Store (s2)
	// would otherwise think the persisted index state is valid and fast-forward, skipping
	// the CRC check on the corrupted entry during startup.
	// To test the WAL recovery/truncation logic specifically, we must invalidate the index
	// so the store is forced to scan the WAL.
	if err := os.RemoveAll(filepath.Join(dir, "index.ldb")); err != nil {
		t.Fatal(err)
	}

	// 3. Re-open Store (Should trigger truncate)
	s2, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatalf("Failed to recover store: %v", err)
	}
	defer s2.Close()

	// 4. Verify Entry 1 exists
	val, err := s2.Get("key1", s2.nextTxID)
	if err != nil {
		t.Errorf("Expected key1 to survive corruption, got error: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("Expected val1, got %s", val)
	}

	// 5. Verify Entry 2 is gone
	_, err = s2.Get("key2", s2.nextTxID)
	if err != ErrKeyNotFound {
		t.Errorf("Expected key2 to be dropped due to CRC failure, got: %v", err)
	}

	// 6. Verify Store Offset is truncated
	f2, _ := os.Open(walPath)
	stat2, _ := f2.Stat()
	f2.Close()

	if stat2.Size() >= size {
		t.Errorf("WAL file should have been truncated. Original: %d, New: %d", size, stat2.Size())
	}
}

func TestStore_Recover_PartialWrite(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	walPath := filepath.Join(dir, "values.log")

	// 1. Create Store and write data
	s1, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	s1.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "key1", val: []byte("val1")}}, 0)
	s1.Close()

	// 2. Append garbage (partial header)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Append 10 bytes (less than HeaderSize 24)
	if _, err := f.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// 3. Re-open
	s2, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatalf("Recovery failed on partial write: %v", err)
	}
	defer s2.Close()

	// 4. Verify Valid Data remains
	if _, err := s2.Get("key1", s2.nextTxID); err != nil {
		t.Error("key1 lost during partial write recovery")
	}
}

func TestStore_Replication_Quorum(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create Store with MinReplicas = 1
	// This ensures that any write operation must wait for at least 1 replica to acknowledge.
	s, err := NewStore(dir, logger, true, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Perform Write (Should Block)
	// We run this in a goroutine because ApplyBatch is synchronous and will block until quorum is met.
	done := make(chan error)
	go func() {
		done <- s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v")}}, 0)
	}()

	// 3. Verify it's blocked
	// We wait a short duration to ensure the write hasn't completed immediately (which would be a bug).
	select {
	case <-done:
		t.Fatal("Write returned before quorum was met")
	case <-time.After(100 * time.Millisecond):
		// Expected behavior: timeout because write is blocked
	}

	// 4. Register Replica and Ack
	// The write above generates a LogSeq.
	// NOTE: Because the store bootstraps an internal _id key, the first user write ("k")
	// will typically have LogSeq = 2 (1 for _id, 2 for k).
	// We acknowledge 2 to ensure the write is unblocked.
	s.RegisterReplica("replica-1", 0)
	s.UpdateReplicaLogSeq("replica-1", 2)

	// 5. Verify Unblock
	// Now that quorum is met, the write should complete successfully.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Write timed out after quorum met")
	}
}

func TestStore_Replication_ApplyBatch(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Replica store (MinReplicas=0 because replicas typically don't require downstream quorum)
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Simulate incoming batch from Primary
	entries := []LogEntry{
		{LogSeq: 100, TxID: 50, OpType: OpJournalSet, Key: []byte("k1"), Value: []byte("v1")},
		{LogSeq: 101, TxID: 51, OpType: OpJournalSet, Key: []byte("k2"), Value: []byte("v2")},
	}

	if err := s.ReplicateBatch(entries); err != nil {
		t.Fatalf("ReplicateBatch failed: %v", err)
	}

	// Verify Data is readable
	// We use a high TxID (60) to ensure we are reading a snapshot that includes the replicated data (TxID 50, 51).
	val, err := s.Get("k1", 60)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Errorf("Want v1, got %s", val)
	}

	val2, err := s.Get("k2", 60)
	if err != nil {
		t.Fatal(err)
	}
	if string(val2) != "v2" {
		t.Errorf("Want v2, got %s", val2)
	}
}

// TestStore_AutoIDGeneration verifies that the store automatically generates
// and persists a unique _id key upon initialization.
func TestStore_AutoIDGeneration(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create new store (First run)
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Retrieve the generated _id
	// We access the store directly.
	val, err := s.Get("_id", s.nextTxID)
	if err != nil {
		t.Fatalf("Failed to retrieve _id: %v", err)
	}
	id1 := string(val)
	if len(id1) < 10 {
		t.Errorf("Generated _id seems too short or invalid: %s", id1)
	}
	t.Logf("Generated Store ID: %s", id1)

	// 3. Close the store
	s.Close()

	// 4. Re-open the store (Second run)
	s2, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// 5. Verify the _id is persisted and unchanged
	val2, err := s2.Get("_id", s2.nextTxID)
	if err != nil {
		t.Fatalf("Failed to retrieve _id after restart: %v", err)
	}
	id2 := string(val2)

	if id1 != id2 {
		t.Errorf("Store ID changed after restart. Original: %s, New: %s", id1, id2)
	}
}
