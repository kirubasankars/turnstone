package stonedb

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestProcessCommitBatch_MixedValidity verifies that the commit pipeline correctly
// separates valid transactions from conflicting ones within the same batch.
// It ensures that a single bad transaction does not fail the entire batch.
func TestProcessCommitBatch_MixedValidity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Setup: Create a key "k_conflict" at version 1 (TxID 1)
	{
		tx := db.NewTransaction(true)
		tx.Put([]byte("k_conflict"), []byte("v1"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// 2. Prepare Transactions for the batch

	// TxA: Valid write to a new key
	txA := db.NewTransaction(true)
	txA.Put([]byte("k_A"), []byte("val_A"))

	// TxB: Invalid (Stale Read on "k_conflict")
	// We force a conflict by manually setting the read snapshot to 0 (older than v1)
	txB := db.NewTransaction(true)
	txB.readTxID = 0
	txB.readSet["k_conflict"] = struct{}{}
	txB.Put([]byte("k_B"), []byte("val_B"))

	// TxC: Invalid (Intra-batch conflict with TxA)
	// TxA writes "k_A". TxC writes "k_A" too. Since TxA comes first in our list,
	// TxC should detect the conflict against the pending batch.
	txC := db.NewTransaction(true)
	txC.Put([]byte("k_A"), []byte("val_C_conflict"))

	// TxD: Valid (Disjoint write)
	txD := db.NewTransaction(true)
	txD.Put([]byte("k_D"), []byte("val_D"))

	// 3. Construct the Batch Request manually
	reqs := []commitRequest{
		{tx: txA, resp: make(chan error, 1)},
		{tx: txB, resp: make(chan error, 1)},
		{tx: txC, resp: make(chan error, 1)},
		{tx: txD, resp: make(chan error, 1)},
	}

	// 4. Execute internal pipeline directly
	// Note: We bypass the commitCh and call the processor directly to ensure deterministic execution.
	db.processCommitBatch(reqs)

	// 5. Verify Responses

	// TxA should succeed
	if err := <-reqs[0].resp; err != nil {
		t.Errorf("TxA expected success, got %v", err)
	}

	// TxB should fail with conflict
	if err := <-reqs[1].resp; err != ErrWriteConflict {
		t.Errorf("TxB expected ErrWriteConflict (Stale Read), got %v", err)
	}

	// TxC should fail with conflict (Intra-batch)
	if err := <-reqs[2].resp; err != ErrWriteConflict {
		t.Errorf("TxC expected ErrWriteConflict (Intra-batch), got %v", err)
	}

	// TxD should succeed
	if err := <-reqs[3].resp; err != nil {
		t.Errorf("TxD expected success, got %v", err)
	}

	// 6. Verify DB State (Persistence)
	checkKey(t, db, "k_A", "val_A")
	checkKey(t, db, "k_D", "val_D")
	checkKeyMissing(t, db, "k_B")

	// Verify "k_A" was NOT overwritten by TxC
	checkKey(t, db, "k_A", "val_A")
}

// TestProcessCommitBatch_IndexFailure simulates a failure during the index update phase (Stage 3).
// This is a critical scenario where data is persisted but not indexed. we tried hard to rollback
func TestProcessCommitBatch_IndexFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Establish baseline: Write k1 successfully
	tx := db.NewTransaction(true)
	tx.Put([]byte("k1"), []byte("v1"))

	reqs := []commitRequest{
		{tx: tx, resp: make(chan error, 1)},
	}
	db.processCommitBatch(reqs)

	// Ensure baseline success
	if err := <-reqs[0].resp; err != nil {
		t.Fatalf("Baseline write failed: %v", err)
	}
	checkKey(t, db, "k1", "v1")

	// 2. Inject Index Failure
	// Define a distinct error so we can verify wrapping works
	simulatedErr := errors.New("simulated index failure")
	testingApplyBatchIndexErr = simulatedErr
	defer func() { testingApplyBatchIndexErr = nil }()

	// 3. Attempt a write that will fail at the Index stage
	tx = db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))

	reqs = []commitRequest{
		{tx: tx, resp: make(chan error, 1)},
	}

	db.processCommitBatch(reqs)

	// 4. Verify the Error Response
	err = <-reqs[0].resp
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Check that we got the wrapper message indicating rollback happened
	if !strings.Contains(err.Error(), "safe rollback performed") {
		t.Errorf("Expected rollback message, got: %v", err)
	}

	// Check that it wraps the underlying cause
	if !errors.Is(err, simulatedErr) {
		t.Errorf("Expected error to wrap '%v', got '%v'", simulatedErr, err)
	}

	// 5. Verify Rollback (In-Memory)
	// The key should NOT be visible because index update failed
	checkKeyMissing(t, db, "k")

	// Delete VLog and Index. This forces a full rebuild from the WAL.
	// If the WAL wasn't truncated correctly during the rollback,
	// the replay will resurrect the phantom key "k".
	if err := os.RemoveAll(filepath.Join(dir, "vlog")); err != nil {
		t.Fatal(err)
	}

	// 6. Verify Rollback (Persistence)
	// Close and Reopen to ensure WAL/VLog were actually truncated.
	// If rollback failed, "k" would reappear here (Phantom Write).
	db.Close()
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// "k" should be gone forever
	checkKeyMissing(t, db2, "k")

	// "k1" should still be there
	checkKey(t, db2, "k1", "v1")

	if db.transactionID != db2.transactionID {
		t.Fatal("transactionID should be advanced")
	}
}

// TestProcessCommitBatch_AllFail verifies the behavior when prepareBatch rejects
// every transaction in the list. The pipeline should abort early (before persistence).
func TestProcessCommitBatch_AllFail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup conflict
	txInit := db.NewTransaction(true)
	_ = txInit.Put([]byte("exists"), []byte("v1"))
	_ = txInit.Commit() // TxID becomes 1

	// Tx1: Conflict
	tx1 := db.NewTransaction(true)
	tx1.readTxID = 0
	tx1.readSet["exists"] = struct{}{}
	_ = tx1.Put([]byte("new1"), []byte("val1"))

	// Tx2: Conflict
	tx2 := db.NewTransaction(true)
	tx2.readTxID = 0
	tx2.readSet["exists"] = struct{}{}
	_ = tx2.Put([]byte("new2"), []byte("val2"))

	reqs := []commitRequest{
		{tx: tx1, resp: make(chan error, 1)},
		{tx: tx2, resp: make(chan error, 1)},
	}

	// Execute
	db.processCommitBatch(reqs)

	// Verify Failures
	if err := <-reqs[0].resp; err != ErrWriteConflict {
		t.Errorf("Tx1 expected ErrWriteConflict, got %v", err)
	}
	if err := <-reqs[1].resp; err != ErrWriteConflict {
		t.Errorf("Tx2 expected ErrWriteConflict, got %v", err)
	}

	// Verify Optimization: Global clocks should NOT have advanced if batch was empty
	// Initial Commit was TxID 1. Since valid batch was empty, it should stay 1.
	if db.transactionID != 1 {
		t.Errorf("Expected TransactionID to remain 1, got %d", db.transactionID)
	}
}

// TestProcessCommitBatch_PreparationFailure exercises the system-level error path in prepareBatch.
// It forces a failure via the testingPrepareBatchErr hook.
func TestProcessCommitBatch_PreparationFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Inject the failure hook
	forcedErr := errors.New("simulated critical system failure")
	testingPrepareBatchErr = forcedErr
	defer func() { testingPrepareBatchErr = nil }()

	// Create a transaction that would otherwise be valid
	tx := db.NewTransaction(true)
	tx.Put([]byte("key"), []byte("val"))

	reqs := []commitRequest{
		{tx: tx, resp: make(chan error, 1)},
	}

	// Execute pipeline
	db.processCommitBatch(reqs)

	// Verify the error propagated to the request
	select {
	case err := <-reqs[0].resp:
		if err != forcedErr {
			t.Errorf("Expected forced error %v, got %v", forcedErr, err)
		}
	default:
		t.Error("Expected error response, got nothing")
	}
}

// TestProcessCommitBatch_CriticalCrash verifies that the system crashes (os.Exit)
// when a rollback fails after a failed VLog write.
// This ensures that we do not leave the system in an inconsistent state.
func TestProcessCommitBatch_CriticalCrash(t *testing.T) {
	// 1. The Crasher Subprocess
	if os.Getenv("BE_CRASHER") == "1" {
		dir, _ := os.MkdirTemp("", "crash_test")
		defer os.RemoveAll(dir)

		db, err := Open(dir, Options{})
		if err != nil {
			// Should not happen, but exit 0 to distinguish from the expected crash
			os.Exit(0)
		}

		// Inject Hook: Trigger critical failure conditions
		testingPersistBatchHook = func() {
			// 1. Sabotage VLog to force write failure (closes the active file handle)
			db.valueLog.Close()
			// 2. Sabotage WAL to force rollback failure (truncation on closed file fails)
			db.writeAheadLog.Close()
		}

		tx := db.NewTransaction(true)
		tx.Put([]byte("k"), []byte("v"))

		// This call should result in os.Exit(1)
		db.processCommitBatch([]commitRequest{{tx: tx, resp: make(chan error, 1)}})

		// If we reach here, the crash logic failed
		os.Exit(0)
	}

	// 2. The Test Runner (Parent)
	cmd := exec.Command(os.Args[0], "-test.run=TestProcessCommitBatch_CriticalCrash")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err := cmd.Run()

	// Check exit code
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		// Pass: The process crashed as expected (non-zero exit code)
		return
	}
	t.Fatalf("Process did not crash as expected. Err: %v", err)
}

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
