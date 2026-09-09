package stonedb

import (
	"errors"
	"sync/atomic"
	"testing"
)

// TestProcessCommitBatch_MultipleValid verifies that several disjoint-key
// transactions in the same group-commit cycle all succeed together with a
// single WAL fsync.
func TestProcessCommitBatch_MultipleValid(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txA := db.NewTransaction(true)
	if err := txA.Put([]byte("k_A"), []byte("val_A")); err != nil {
		t.Fatal(err)
	}
	txD := db.NewTransaction(true)
	if err := txD.Put([]byte("k_D"), []byte("val_D")); err != nil {
		t.Fatal(err)
	}

	reqs := []commitRequest{
		{tx: txA, resp: make(chan error, 1)},
		{tx: txD, resp: make(chan error, 1)},
	}
	db.processCommitBatch(reqs)

	if err := <-reqs[0].resp; err != nil {
		t.Errorf("TxA expected success, got %v", err)
	}
	if err := <-reqs[1].resp; err != nil {
		t.Errorf("TxD expected success, got %v", err)
	}

	checkKey(t, db, "k_A", "val_A")
	checkKey(t, db, "k_D", "val_D")
}

// TestProcessCommitBatch_ReadSetConflict verifies that a transaction whose
// read set is stale relative to its (forced) snapshot fails commit-time SI
// validation, even though under first-writer-wins its own writes never
// conflicted with anyone at Put time.
func TestProcessCommitBatch_ReadSetConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Establish a committed version of "k_conflict".
	tx0 := db.NewTransaction(true)
	if err := tx0.Put([]byte("k_conflict"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := tx0.Commit(); err != nil {
		t.Fatal(err)
	}

	// txB read "k_conflict" under a snapshot older than tx0's commit, then
	// wrote a disjoint key. At commit time, the read set must be revalidated
	// against the clog and found stale.
	txB := db.NewTransaction(true)
	txB.readSet["k_conflict"] = struct{}{}
	txB.snapshot = Snapshot{Xmax: 1, Xip: map[uint64]bool{}}
	if err := txB.Put([]byte("k_B"), []byte("val_B")); err != nil {
		t.Fatal(err)
	}

	reqs := []commitRequest{{tx: txB, resp: make(chan error, 1)}}
	db.processCommitBatch(reqs)

	if err := <-reqs[0].resp; err != ErrWriteConflict {
		t.Errorf("txB expected ErrWriteConflict (stale read), got %v", err)
	}

	// The key locks/xid bookkeeping must be released so a later writer isn't
	// blocked by the aborted transaction.
	checkKeyMissing(t, db, "k_B")
	txRetry := db.NewTransaction(true)
	if err := txRetry.Put([]byte("k_B"), []byte("val_B_retry")); err != nil {
		t.Fatalf("expected k_B to be writable after txB aborted, got %v", err)
	}
	if err := txRetry.Commit(); err != nil {
		t.Fatal(err)
	}
	checkKey(t, db, "k_B", "val_B_retry")
}

// TestProcessCommitBatch_SystemErrorHook verifies that the
// testingProcessCommitBatchErr hook fails every request in the batch before
// any WAL/clog work happens.
func TestProcessCommitBatch_SystemErrorHook(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	forcedErr := errTestSystemFailure
	testingProcessCommitBatchErr = forcedErr
	defer func() { testingProcessCommitBatchErr = nil }()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("key"), []byte("val")); err != nil {
		t.Fatal(err)
	}

	reqs := []commitRequest{{tx: tx, resp: make(chan error, 1)}}
	db.processCommitBatch(reqs)

	if err := <-reqs[0].resp; err != forcedErr {
		t.Errorf("expected forced error %v, got %v", forcedErr, err)
	}
}

// TestProcessCommitBatch_WALFsyncFailure verifies that a failure while
// group-fsyncing COMMIT records marks the database corrupt (there is no
// rollback of an already-eager-written transaction; the WAL append for the
// COMMIT record itself is what failed here, before it was durable).
func TestProcessCommitBatch_WALFsyncFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// NOTE: deliberately not deferring db.Close() here. We sabotage the WAL's
	// underlying file below, and a subsequent Close()/Checkpoint() would force
	// a WAL rotation that calls strictSync directly on the closed file,
	// which panics by design (fail-fast on real storage failure). That crash
	// path is exercised/covered elsewhere; this test only cares about the
	// commit-time error and corruption flag.

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Sabotage the WAL's underlying file so the COMMIT record's fsync fails.
	db.log.file.Close()

	reqs := []commitRequest{{tx: tx, resp: make(chan error, 1)}}
	db.processCommitBatch(reqs)

	if err := <-reqs[0].resp; err == nil {
		t.Fatal("expected error from sabotaged WAL fsync, got nil")
	}

	if atomic.LoadInt32(&db.isCorrupt) != 1 {
		t.Error("expected DB to be marked corrupt after WAL commit-fsync failure")
	}
}

// errTestSystemFailure is a distinct sentinel used by the testing hook tests.
var errTestSystemFailure = errors.New("simulated critical system failure")

// Helpers for test conciseness

func checkKey(t *testing.T, db *DB, key, expected string) {
	t.Helper()
	tx := db.NewTransaction(false)
	defer tx.Discard()
	val, err := tx.Get([]byte(key))
	if err != nil {
		t.Errorf("Get(%s) failed: %v", key, err)
		return
	}
	if string(val) != expected {
		t.Errorf("Get(%s) = %s, want %s", key, val, expected)
	}
}

func checkKeyMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	tx := db.NewTransaction(false)
	defer tx.Discard()
	_, err := tx.Get([]byte(key))
	if err != ErrKeyNotFound {
		t.Errorf("Get(%s) expected ErrKeyNotFound, got %v", key, err)
	}
}
