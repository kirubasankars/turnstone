package main

import (
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
)

// TestStore_MinReadLSN verifies snapshot isolation pins MinReadLSN.
func TestStore_MinReadLSN(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Advance LSN to 10
	var currentReadLSN uint64 = 0
	for i := 0; i < 9; i++ {
		s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "k", val: []byte("v")}}, currentReadLSN)
		currentReadLSN++
	}

	// 2. Acquire Snapshot at LSN 9
	snapLSN := s.AcquireSnapshot()
	if snapLSN != 9 {
		t.Fatalf("Expected snap 9, got %d", snapLSN)
	}

	// Write more (LSN 10, 11, 12)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "a", val: []byte("v")}}, 0)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "b", val: []byte("v")}}, 0)
	s.ApplyBatch([]bufferedOp{{opType: OpJournalSet, key: "c", val: []byte("v")}}, 0)

	// MinReadLSN should still be 9 because of the active snapshot.
	if s.minReadLSN != 9 {
		t.Errorf("MinReadLSN should be pinned by snapshot (9), got %d", s.minReadLSN)
	}

	// 4. Release Snapshot
	s.ReleaseSnapshot(snapLSN)

	// MinReadLSN should move to Head (12)
	if s.minReadLSN != 12 {
		t.Errorf("MinReadLSN should move to head (12), got %d", s.minReadLSN)
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
	currentLSN := atomic.LoadUint64(&s1.nextLSN) - 1

	for _, k := range keys {
		op := bufferedOp{
			opType: OpJournalSet,
			key:    k,
			val:    []byte("val-" + k),
		}
		if err := s1.ApplyBatch([]bufferedOp{op}, currentLSN); err != nil {
			t.Fatalf("ApplyBatch failed for key %s: %v", k, err)
		}
		currentLSN = atomic.LoadUint64(&s1.nextLSN) - 1
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
