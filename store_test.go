package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupTestStore(t *testing.T) (*Store, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "turnstone-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store, err := NewStore(dir, logger, true, false, true, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	cleanup := func() {
		_ = store.Close()
		_ = os.RemoveAll(dir)
	}

	return store, cleanup
}

func TestStore_GetSet(t *testing.T) {
	// SET, GET
	store, cleanup := setupTestStore(t)
	defer cleanup()

	readVersion, generation := store.AcquireSnapshot()

	ops := []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world")},
	}
	err := store.ApplyBatch(ops, readVersion, generation)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	store.ReleaseSnapshot(readVersion)

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
	// SET, GET, DEL, GET (not found)
	store, cleanup := setupTestStore(t)
	defer cleanup()

	readVersion, generation := store.AcquireSnapshot()

	ops := []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world")},
	}
	err := store.ApplyBatch(ops, readVersion, generation)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	store.ReleaseSnapshot(readVersion)

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

	readVersion3, generation3 := store.AcquireSnapshot()

	ops = []bufferedOp{
		{opType: OpJournalDelete, key: "hello"},
	}
	err = store.ApplyBatch(ops, readVersion3, generation3)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	store.ReleaseSnapshot(readVersion3)

	readVersion4, generation4 := store.AcquireSnapshot()
	defer store.ReleaseSnapshot(readVersion4)

	_, err = store.Get("hello", readVersion4, generation4)
	if err != ErrKeyNotFound {
		t.Errorf("Expected error ErrKeyNotFound, got %v", err)
	}
}

func TestStore_TransactionIsolation_WriteWriteConflict_SameReadVersion(t *testing.T) {
	// Tx1 BEGIN  (100) | Tx2 BEGIN (100)       |
	// SET hello world  | set hello world       |
	// COMMIT           | sleep 1               |
	//                  | COMMIT (conflict)     |
	store, cleanup := setupTestStore(t)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}
		err := store.ApplyBatch(ops1, readVersion, generation)
		if err != nil {
			t.Errorf("ApplyBatch failed: %v", err)
		}
	}()

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world2")},
		}
		time.Sleep(1 * time.Second)
		err := store.ApplyBatch(ops2, readVersion, generation)
		if err != ErrConflict {
			t.Errorf("Expected error ErrConflict, got %v", err)
		}
	}()

	wg.Wait()
}

func TestStore_TransactionIsolation_WriteWriteConflict_SameReadVersion2(t *testing.T) {
	// Tx1 BEGIN  (100) | Tx2 BEGIN (100)       |
	// SET hello world  | set hello world       |
	// COMMIT           | sleep 1               |
	//                  | COMMIT (conflict)     |

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

		err := store.ApplyBatch(ops1, readVersion, generation)
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

		err := store.ApplyBatch(ops2, readVersion, generation)
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
	// Tx1 BEGIN  (100) |                       |
	//                  |                       | Tx3 BEGIN (100)
	//                  |                       | SET hello1 world2
	//                  |                       | COMMIT
	//                  | Tx2 BEGIN (105)       |
	// SET hello world  | sleep 1               |
	// COMMIT           | SET hello world2      |
	//                  | COMMIT (conflict)     |

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

		err := store.ApplyBatch(ops1, readVersion, generation)
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

		err := store.ApplyBatch(ops2, readVersion, generation)
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

	err := store.ApplyBatch(ops3, readVersion, generation)
	if err != nil {
		t.Errorf("T3 ApplyBatch failed (T3 should succeed): %v", err)
	}

	close(canStart)

	wg.Wait()
}

func TestStore_TransactionIsolation_ReadWriteConflict(t *testing.T) {
	// Tx  BEGIN  (100) |                       |
	// SET hello world2 |                       |
	// SET foo bar      |                       |
	// COMMIT           |                       |
	//                  | Tx1 BEGIN (105)       |
	//                  | SET foo1 bar1         |
	//                  | COMMIT                |
	//                  | BEGIN                 | BEGIN
	//                  | GET hello             | sleep 1
	//                  | SET foo bar1          | SET hello world2
	//                  | COMMIT                | GET hello3 world
	//                  |                       | COMMIT (conflict)
	store, cleanup := setupTestStore(t)
	defer cleanup()

	readVersion, generation := store.AcquireSnapshot()
	ops := []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world2")},
		{opType: OpJournalSet, key: "foo", val: []byte("bar")},
	}
	err := store.ApplyBatch(ops, readVersion, generation)
	if err != nil {
		t.Errorf("Setup ApplyBatch failed (Setup should succeed): %v", err)
	}
	store.ReleaseSnapshot(readVersion)

	ready := make(chan struct{}, 2)
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "foo1", val: []byte("bar1")},
		}
		err := store.ApplyBatch(ops1, readVersion, generation)
		if err != nil {
			t.Errorf("T1 ApplyBatch failed (T1 should succeed): %v", err)
		}
		store.ReleaseSnapshot(readVersion)

		readVersion, generation = store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ready <- struct{}{}
		<-start

		ops1 = []bufferedOp{
			{opType: OpJournalGet, key: "hello", val: []byte("world")},
			{opType: OpJournalSet, key: "foo", val: []byte("bar1")},
		}

		err = store.ApplyBatch(ops1, readVersion, generation)
		if err != nil {
			t.Errorf("T1 ApplyBatch failed (T1 should succeed): %v", err)
		}
	}()

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ready <- struct{}{}
		<-start

		time.Sleep(1 * time.Second)

		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world2")},
			{opType: OpJournalGet, key: "hello3", val: []byte("world")},
		}
		err := store.ApplyBatch(ops2, readVersion, generation)
		if err != ErrConflict {
			t.Errorf("T2 Expected error ErrConflict, got %v", err)
		}
	}()

	<-ready
	<-ready

	close(start)

	wg.Wait()
}

func TestStore_Compaction_Generation(t *testing.T) {
	// SET, SET, GET, COMPACT, GET
	dir, err := os.MkdirTemp("", "turnstone-test-compaction")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store, err := NewStore(dir, logger, true, false, true, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add some data
	readVersion, generation := store.AcquireSnapshot()
	ops := []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world")},
	}
	err = store.ApplyBatch(ops, readVersion, generation)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}
	store.ReleaseSnapshot(readVersion)

	readVersion, generation = store.AcquireSnapshot()
	v, _ := store.Get("hello", readVersion, generation)
	if string(v) != "world" {
		t.Fatal("Get is not matched")
	}
	ops = []bufferedOp{
		{opType: OpJournalSet, key: "hello", val: []byte("world2")},
	}
	err = store.ApplyBatch(ops, readVersion, generation)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}
	store.ReleaseSnapshot(readVersion)

	// Trigger compaction
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Wait for compaction to finish
	for store.compactionRunning.Load() {
		time.Sleep(100 * time.Millisecond)
	}

	// Check if new generation files are created
	// Generation should be 2 now
	if _, err := os.Stat(filepath.Join(dir, "2.db")); err != nil {
		t.Errorf("Expected file 2.db to exist, but it doesn't")
	}
	if _, err := os.Stat(filepath.Join(dir, "2.idx")); err != nil {
		t.Errorf("Expected file 2.idx to exist, but it doesn't")
	}

	// Check if old files are removed
	if _, err := os.Stat(filepath.Join(dir, "1.db")); !os.IsNotExist(err) {
		t.Errorf("Expected file 1.db to be removed, but it exists")
	}
	if _, err := os.Stat(filepath.Join(dir, "1.idx")); !os.IsNotExist(err) {
		t.Errorf("Expected file 1.idx to be removed, but it exists")
	}

	// Check if data is still accessible
	readVersion2, generation2 := store.AcquireSnapshot()
	val, err := store.Get("hello", readVersion2, generation2)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "world2" {
		t.Errorf("Expected value 'world2', got '%s'", string(val))
	}
	store.ReleaseSnapshot(readVersion2)

	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("Failed to close store: %v", err)
	}

	// Create a new store in the same directory and check if it loads the latest generation
	store2, err := NewStore(dir, logger, true, false, true, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if store2.generation != 2 {
		t.Errorf("Expected store to load generation 2, but got %d", store2.generation)
	}

	// Check if data is still accessible in the new store
	readVersion3, generation3 := store2.AcquireSnapshot()
	val2, err := store2.Get("hello", readVersion3, generation3)
	if err != nil {
		t.Fatalf("Get from store2 failed: %v", err)
	}
	if string(val2) != "world2" {
		t.Errorf("Expected value 'world2' from store2, got '%s'", string(val2))
	}
	store2.ReleaseSnapshot(readVersion3)
}

func TestStore_SetSet_DiffKey(t *testing.T) {
	// Tx1 BEGIN  (100) | Tx2 BEGIN (100)       |
	// SET hello world  | set hello1 world      |
	// COMMIT           | COMMIT                |
	store, cleanup := setupTestStore(t)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}
		err := store.ApplyBatch(ops1, readVersion, generation)
		if err != nil {
			t.Errorf("ApplyBatch failed: %v", err)
		}
	}()

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello1", val: []byte("world")},
		}
		time.Sleep(1 * time.Second)
		err := store.ApplyBatch(ops2, readVersion, generation)
		if err != nil {
			t.Errorf("ApplyBatch failed: %v", err)
		}
	}()

	wg.Wait()
}

func TestStore_SetSet_SameKey(t *testing.T) {
	// Tx1 BEGIN  (100) | Tx2 BEGIN (100)       |
	// SET hello world  | set hello1 world      |
	// COMMIT           | COMMIT                |
	store, cleanup := setupTestStore(t)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(2)

	var c int64

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}
		err := store.ApplyBatch(ops1, readVersion, generation)
		if err != nil {
			atomic.AddInt64(&c, 1)
		}
	}()

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops2 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}
		err := store.ApplyBatch(ops2, readVersion, generation)
		if err != nil {
			atomic.AddInt64(&c, 1)
		}
	}()

	wg.Wait()

	if c != 1 {
		t.Error("a transaction should fail")
	}
}

func TestStore_GetSet_RelatedKey(t *testing.T) {
	// Tx1 BEGIN  (100) | Tx2 BEGIN (100)       |
	// SET hello world  | get hello             |
	//                  | SET hello1 world      |
	// COMMIT           | COMMIT (conflict)     |
	store, cleanup := setupTestStore(t)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(2)

	ready := make(chan struct{}, 2)
	start := make(chan struct{})

	var c int64

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops1 := []bufferedOp{
			{opType: OpJournalSet, key: "hello", val: []byte("world")},
		}

		ready <- struct{}{}
		<-start

		err := store.ApplyBatch(ops1, readVersion, generation)
		if err != nil {
			atomic.AddInt64(&c, 1)
		}
	}()

	go func() {
		defer wg.Done()

		readVersion, generation := store.AcquireSnapshot()
		defer store.ReleaseSnapshot(readVersion)

		ops2 := []bufferedOp{
			{opType: OpJournalGet, key: "hello", val: []byte("world")},
			{opType: OpJournalSet, key: "hello1", val: []byte("world2")},
		}

		ready <- struct{}{}
		<-start

		err := store.ApplyBatch(ops2, readVersion, generation)
		if err != nil {
			atomic.AddInt64(&c, 1)
		}
	}()

	<-ready
	<-ready

	close(start)

	wg.Wait()

	if c != 1 {
		t.Error("a transaction should fail")
	}
}
