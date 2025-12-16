package main

import (
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
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

func TestStore_TransactionIsolation_WriteWriteConflict_SameReadVersion(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	readVersion, generation := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion)

	var wg sync.WaitGroup
	wg.Add(2)

	t1Done := make(chan struct{})

	// T1: Set a value in a separate goroutine
	go func() {
		defer wg.Done()
		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}
		err := store.ApplyBatch(ops1, nil, readVersion, generation)
		if err != nil {
			t.Errorf("ApplyBatch failed: %v", err)
		}
		close(t1Done)
	}()

	// T2: Set a value to the same key in a separate goroutine
	go func() {
		defer wg.Done()
		<-t1Done // Wait for T1 to complete
		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world2")},
		}
		err := store.ApplyBatch(ops2, nil, readVersion, generation)
		if err != ErrConflict {
			t.Errorf("Expected error ErrConflict, got %v", err)
		}
	}()

	wg.Wait()
}

func TestStore_TransactionIsolation_WriteWriteConflict_SameReadVersion2(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ready := make(chan struct{}, 2)
	canStart := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ready <- struct{}{}
		<-canStart

		err := store.ApplyBatch(ops1, nil, readVersion, generation)
		if err != nil {
			t.Errorf("T1 ApplyBatch failed (T1 should succeed): %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world2")},
		}
		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ready <- struct{}{}

		<-canStart

		time.Sleep(1 * time.Second)

		err := store.ApplyBatch(ops2, nil, readVersion, generation)
		if err != ErrConflict {
			t.Errorf("T2 Expected error ErrConflict, got %v", err)
		}
	}()

	<-ready
	<-ready

	close(canStart)

	wg.Wait()
}

func TestStore_TransactionIsolation_WriteWriteConflict_DiffReadVersion(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ready := make(chan struct{}, 2)
	canStart := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ready <- struct{}{}
		<-canStart

		err := store.ApplyBatch(ops1, nil, readVersion, generation)
		if err != nil {
			t.Errorf("T1 ApplyBatch failed (T1 should succeed): %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world2")},
		}

		ready <- struct{}{}
		<-canStart

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		time.Sleep(1 * time.Second)

		err := store.ApplyBatch(ops2, nil, readVersion, generation)
		if err != ErrConflict {
			t.Errorf("T2 Expected error ErrConflict, got %v", err)
		}
	}()

	<-ready
	<-ready

	readVersion, generation := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion)

	ops3 := []bufferedOp{
		{opType: OpJournalSet, key: "hello1", val: []byte("world2")},
	}

	err := store.ApplyBatch(ops3, nil, readVersion, generation)
	if err != nil {
		t.Errorf("T3 ApplyBatch failed (T3 should succeed): %v", err)
	}

	close(canStart)

	wg.Wait()
}
