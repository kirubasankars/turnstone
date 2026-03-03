package stonedb

import (
	"testing"
)

// TestIsolation_WriteWriteConflict verifies that two concurrent transactions
// cannot update the same key. First committer wins.
func TestIsolation_WriteWriteConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("conflict_key")

	// 1. Initial setup
	{
		tx := db.NewTransaction(true)
		tx.Put(key, []byte("v0"))
		tx.Commit()
	}

	// 2. Start Tx1
	tx1 := db.NewTransaction(true)
	val1, _ := tx1.Get(key) // Read v0

	// 3. Start Tx2
	tx2 := db.NewTransaction(true)
	val2, _ := tx2.Get(key) // Read v0

	// 4. Tx1 updates
	tx1.Put(key, append(val1, []byte("-tx1")...))

	// 5. Tx2 updates
	tx2.Put(key, append(val2, []byte("-tx2")...))

	// 6. Tx2 commits FIRST -> Should succeed
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Tx2 failed to commit: %v", err)
	}

	// 7. Tx1 commits SECOND -> Should fail (Write Conflict on same key)
	if err := tx1.Commit(); err != ErrWriteConflict {
		t.Errorf("Tx1 expected ErrWriteConflict, got: %v", err)
	}
}

// TestIsolation_BlindWriteConflict verifies that Snapshot Isolation rules apply
// even if the transactions do NOT read the key before writing it.
// Tx1: Put(A) -> Commit
// Tx2: Put(A) -> Commit (Concurrent with Tx1)
// Expect: Tx2 fails if Tx1 committed first.
func TestIsolation_BlindWriteConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("blind_key")

	// Setup: Key exists at version 1
	txInit := db.NewTransaction(true)
	txInit.Put(key, []byte("v1"))
	txInit.Commit()

	// 1. Start TxA (Snapshot at v1)
	txA := db.NewTransaction(true)

	// 2. Start TxB (Snapshot at v1)
	txB := db.NewTransaction(true)

	// 3. TxA writes (No Read)
	txA.Put(key, []byte("v2-A"))

	// 4. TxB writes (No Read)
	txB.Put(key, []byte("v2-B"))

	// 5. TxA Commits -> Success. Version becomes 2.
	if err := txA.Commit(); err != nil {
		t.Fatalf("TxA failed commit: %v", err)
	}

	// 6. TxB Commits -> Should Fail.
	// TxB's snapshot is v1. The key is now at v2 (committed by TxA).
	// Under Snapshot Isolation, writing to a key that changed after start time is a conflict.
	if err := txB.Commit(); err != ErrWriteConflict {
		t.Errorf("TxB Blind Write expected ErrWriteConflict, got: %v", err)
	}
}

// TestIsolation_ReadWriteConflict verifies that if Tx1 reads a key,
// and Tx2 updates that key and commits, Tx1 cannot commit.
// This enforces Serializability/OCC (preventing Stale Reads).
func TestIsolation_ReadWriteConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("shared_key")

	// Setup
	{
		tx := db.NewTransaction(true)
		tx.Put(key, []byte("v0"))
		tx.Commit()
	}

	// Tx1 Reads
	tx1 := db.NewTransaction(true)
	tx1.Get(key)

	// Tx2 Updates same key
	tx2 := db.NewTransaction(true)
	tx2.Put(key, []byte("v1"))
	tx2.Commit()

	// Tx1 tries to update something else and commit
	tx1.Put([]byte("other"), []byte("val"))

	// Should fail because its Read Set (key) is stale (modified by Tx2)
	if err := tx1.Commit(); err != ErrWriteConflict {
		t.Errorf("Tx1 expected ErrWriteConflict due to stale read, got: %v", err)
	}
}

// TestIsolation_DisjointWrites_Succeeds checks the user scenario:
// Tx1 reads a, b; updates b.
// Tx2 reads a, c; updates c.
// Result: Since 'a' is not updated, and b/c are disjoint, there is no conflict
// under Snapshot Isolation (SI) or OCC. Both should succeed.
func TestIsolation_DisjointWrites_Succeeds(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")

	// Setup
	{
		tx := db.NewTransaction(true)
		tx.Put(keyA, []byte("valA"))
		tx.Put(keyB, []byte("valB"))
		tx.Put(keyC, []byte("valC"))
		tx.Commit()
	}

	// Tx1 starts
	tx1 := db.NewTransaction(true)
	tx1.Get(keyA)
	tx1.Get(keyB)
	tx1.Put(keyB, []byte("valB-updated"))

	// Tx2 starts
	tx2 := db.NewTransaction(true)
	tx2.Get(keyA)
	tx2.Get(keyC)
	tx2.Put(keyC, []byte("valC-updated"))

	// Tx2 commits
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Tx2 failed to commit: %v", err)
	}

	// Tx1 commits
	// Verification:
	// Tx1 Read Set: {A, B}.
	// A: Valid (Not modified by Tx2).
	// B: Valid (Not modified by Tx2).
	// C: Modified by Tx2, but NOT in Tx1's read set.
	// Therefore, commit should succeed.
	if err := tx1.Commit(); err != nil {
		t.Errorf("Tx1 failed to commit in disjoint scenario: %v", err)
	}
}

// TestTransaction_ReadYourOwnWrites verifies that a transaction sees its own modifications
// (Put and Delete) immediately within the transaction, masking the underlying database state.
func TestTransaction_ReadYourOwnWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Setup: Commit "key_A" -> "val_db"
	{
		tx := db.NewTransaction(true)
		tx.Put([]byte("key_A"), []byte("val_db"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// 2. Start a new transaction
	tx := db.NewTransaction(true)
	defer tx.Discard()

	// Scenario A: Local Put shadows DB value
	// We put "val_local". A subsequent Get should return "val_local", not "val_db".
	if err := tx.Put([]byte("key_A"), []byte("val_local")); err != nil {
		t.Fatal(err)
	}
	val, err := tx.Get([]byte("key_A"))
	if err != nil {
		t.Fatalf("Get key_A failed: %v", err)
	}
	if string(val) != "val_local" {
		t.Errorf("Expected val_local, got %s", val)
	}

	// Scenario B: Local Put for new key
	if err := tx.Put([]byte("key_B"), []byte("val_B")); err != nil {
		t.Fatal(err)
	}
	val, err = tx.Get([]byte("key_B"))
	if err != nil {
		t.Fatalf("Get key_B failed: %v", err)
	}
	if string(val) != "val_B" {
		t.Errorf("Expected val_B, got %s", val)
	}

	// Scenario C: Local Delete shadows Local Put
	// We delete "key_B". Get should return ErrKeyNotFound.
	if err := tx.Delete([]byte("key_B")); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Get([]byte("key_B"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for key_B after delete, got %v", err)
	}

	// Scenario D: Local Delete shadows DB value
	// key_A is currently "val_local" in pendingOps. Delete it.
	if err := tx.Delete([]byte("key_A")); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Get([]byte("key_A"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for key_A after delete, got %v", err)
	}

	// Scenario E: Re-Put after Delete
	// Verify we can write to a key that was previously deleted in the same transaction.
	if err := tx.Put([]byte("key_A"), []byte("val_resurrected")); err != nil {
		t.Fatal(err)
	}
	val, err = tx.Get([]byte("key_A"))
	if err != nil {
		t.Fatalf("Get key_A failed: %v", err)
	}
	if string(val) != "val_resurrected" {
		t.Errorf("Expected val_resurrected, got %s", val)
	}
}
