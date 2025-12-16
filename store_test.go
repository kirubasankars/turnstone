package main

import (
	"os"
	"sync"
	"testing"
	"time"

	"log/slog"
)

func setupTestStore(t *testing.T) (*Store, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "turnstonedb-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store, err := NewStore(dir, logger, true, false, true, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(dir)
	}

	return store, cleanup
}

func TestStore_GetSet(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// Begin a transaction
	readVersion, generation := store.AcquireSnapshot()

	// Set a value
	ops := []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world")},
	}
	err := store.ApplyBatch(ops, nil, readVersion, generation)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// Commit the transaction
	store.ReleaseSnapshot(readVersion)

	// Begin a new transaction to get the value
	readVersion2, generation2 := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion2)

	// Get the value
	val, err := store.Get("hello", readVersion2, generation2)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(val) != "world" {
		t.Errorf("Expected value 'world', got '%s'", string(val))
	}
}

func TestStore_GetSetDelete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// Begin a transaction
	readVersion, generation := store.AcquireSnapshot()

	// Set a value
	ops := []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world")},
	}
	err := store.ApplyBatch(ops, nil, readVersion, generation)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// Commit the transaction
	store.ReleaseSnapshot(readVersion)

	// Begin a new transaction to get the value
	readVersion2, generation2 := store.AcquireSnapshot()

	// Get the value
	val, err := store.Get("hello", readVersion2, generation2)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(val) != "world" {
		t.Errorf("Expected value 'world', got '%s'", string(val))
	}

	store.ReleaseSnapshot(readVersion2)

	// Begin a new transaction to delete the value
	readVersion3, generation3 := store.AcquireSnapshot()

	// Delete the value
	ops = []bufferedOp{
		{opType: OpJournalDelete, key: "hello"},
	}
	err = store.ApplyBatch(ops, nil, readVersion3, generation3)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// Commit the transaction
	store.ReleaseSnapshot(readVersion3)

	// Begin a new transaction to get the value
	readVersion4, generation4 := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion4)

	// Get the value
	_, err = store.Get("hello", readVersion4, generation4)
	if err != ErrKeyNotFound {
		t.Errorf("Expected error ErrKeyNotFound, got %v", err)
	}
}

func TestStore_TransactionIsolation(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)

	// T1: Set a value in a separate goroutine
	go func() {
		defer wg.Done()
		readVersion1, generation1 := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion1)
		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}
		err := store.ApplyBatch(ops1, nil, readVersion1, generation1)
		if err != nil {
			t.Errorf("ApplyBatch failed: %v", err)
		}
	}()

	// T2: Start a new transaction before T1 is committed
	readVersion2, generation2 := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion2)

	// T2: Try to get the value set by T1
	_, err := store.Get("hello", readVersion2, generation2)
	if err != ErrKeyNotFound {
		t.Errorf("Expected error ErrKeyNotFound, got %v", err)
	}

	// Wait for T1 to finish
	wg.Wait()

	// T3: Start a new transaction after T1 is committed
	readVersion3, generation3 := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion3)

	// T3: Get the value set by T1
	val, err := store.Get("hello", readVersion3, generation3)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(val) != "world" {
		t.Errorf("Expected value 'world', got '%s'", string(val))
	}
}
