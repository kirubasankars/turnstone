package main

import (
	"os"
	"reflect"
	"testing"

	"go.etcd.io/bbolt"
)

func TestIndexEntry(t *testing.T) {
	e := packIndexEntry(123, false)
	if e.Offset() != 123 {
		t.Errorf("Expected offset 123, got %d", e.Offset())
	}
	if e.Deleted() {
		t.Error("Expected not deleted")
	}

	e = packIndexEntry(456, true)
	if e.Offset() != 456 {
		t.Errorf("Expected offset 456, got %d", e.Offset())
	}
	if !e.Deleted() {
		t.Error("Expected deleted")
	}
}

func TestIndex_SetAndGet(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	idx := NewIndex(db)
	// Added length argument (10) to Set calls
	idx.Set("key1", 1, 10, false, 0)
	idx.Set("key1", 2, 10, false, 0)
	idx.Set("key2", 3, 10, false, 0)

	// Test Get
	entry, ok := idx.Get("key1", 3)
	if !ok || entry.Offset() != 2 {
		t.Errorf("Expected to get offset 2 for key1, got %v, %v", entry, ok)
	}

	entry, ok = idx.Get("key1", 2)
	if !ok || entry.Offset() != 1 {
		t.Errorf("Expected to get offset 1 for key1, got %v, %v", entry, ok)
	}

	entry, ok = idx.Get("key2", 4)
	if !ok || entry.Offset() != 3 {
		t.Errorf("Expected to get offset 3 for key2, got %v, %v", entry, ok)
	}

	// Test GetLatest
	entry, ok = idx.GetLatest("key1")
	if !ok || entry.Offset() != 2 {
		t.Errorf("Expected to get latest offset 2 for key1, got %v, %v", entry, ok)
	}

	// Test Get on non-existent key
	_, ok = idx.Get("non-existent", 1)
	if ok {
		t.Error("Expected not to get a non-existent key")
	}
}

func TestIndex_Delete(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	idx := NewIndex(db)
	idx.Set("key1", 1, 10, false, 0)
	idx.Set("key1", 2, 10, true, 0)

	_, ok := idx.Get("key1", 3)
	if ok {
		t.Error("Expected key1 to be deleted")
	}

	entry, ok := idx.Get("key1", 2)
	if !ok || entry.Offset() != 1 {
		t.Errorf("Expected to get offset 1 for key1, got %v, %v", entry, ok)
	}
}

func TestIndex_RotateAndFlush(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	idx := NewIndex(db)
	idx.Set("key1", 1, 10, false, 0)
	idx.Set("key2", 2, 10, false, 0)

	// Rotate
	rotated := idx.Rotate()
	if !rotated {
		t.Fatal("Expected rotation to happen")
	}

	// Updated: Check idx.active.keys due to memState refactor
	if len(idx.active.keys) != 0 {
		t.Error("Expected active index to be empty after rotation")
	}

	// Updated: Check idx.flushing.keys due to memState refactor
	if idx.flushing == nil {
		t.Fatal("Expected flushing index to be non-nil")
	}
	if len(idx.flushing.keys) != 2 {
		t.Errorf("Expected flushing index to have 2 keys, got %d", len(idx.flushing.keys))
	}

	// Flush
	err := idx.FlushToBolt(3)
	if err != nil {
		t.Fatalf("Error flushing to bolt: %v", err)
	}
	idx.FinishFlush()

	if idx.flushing != nil {
		t.Error("Expected flushing index to be nil after finishing flush")
	}

	// Verify data in bolt
	entry, ok := idx.Get("key1", 2)
	if !ok || entry.Offset() != 1 {
		t.Errorf("Expected to get offset 1 for key1 from bolt, got %v, %v", entry, ok)
	}

	entry, ok = idx.Get("key2", 3)
	if !ok || entry.Offset() != 2 {
		t.Errorf("Expected to get offset 2 for key2 from bolt, got %v, %v", entry, ok)
	}
}

func TestIndex_HistoryPruning(t *testing.T) {
	hist := []IndexEntry{
		packIndexEntry(1, false),
		packIndexEntry(2, false),
		packIndexEntry(3, false),
		packIndexEntry(4, false),
	}

	idx := &Index{}
	pruned := idx.pruneHistory(hist, 3)

	expected := []IndexEntry{
		packIndexEntry(2, false),
		packIndexEntry(3, false),
		packIndexEntry(4, false),
	}

	if !reflect.DeepEqual(pruned, expected) {
		t.Errorf("Expected pruned history %v, got %v", expected, pruned)
	}
}

func TestIndex_EncodeDecodeHistory(t *testing.T) {
	hist := []IndexEntry{
		packIndexEntry(1, false),
		packIndexEntry(2, true),
		packIndexEntry(3, false),
	}

	idx := &Index{}
	encoded := idx.encodeHistory(hist)
	decoded := idx.decodeHistory(encoded)

	if !reflect.DeepEqual(hist, decoded) {
		t.Errorf("Expected decoded history %v, got %v", hist, decoded)
	}
}

func TestIndex_Len(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	idx := NewIndex(db)
	idx.Set("key1", 1, 10, false, 0)
	idx.Set("key2", 2, 10, false, 0)
	if idx.Len() != 2 {
		t.Errorf("Expected length 2, got %d", idx.Len())
	}

	idx.Rotate()
	if idx.Len() != 2 {
		t.Errorf("Expected length 2, got %d", idx.Len())
	}
	idx.Set("key3", 3, 10, false, 0)
	if idx.Len() != 3 {
		t.Errorf("Expected length 3, got %d", idx.Len())
	}

	err := idx.FlushToBolt(3)
	if err != nil {
		t.Fatalf("Error flushing to bolt: %v", err)
	}
	idx.FinishFlush()

	if idx.Len() != 3 {
		t.Errorf("Expected length 3, got %d", idx.Len())
	}
}

// newTestDB creates a new temporary bbolt database for testing.
func newTestDB(t *testing.T) (*bbolt.DB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "turnstone-test-")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	db, err := bbolt.Open(f.Name(), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		db.Close()
		os.Remove(f.Name())
	}
	return db, cleanup
}

func TestIndex_searchHistory(t *testing.T) {
	hist := []IndexEntry{
		packIndexEntry(10, false), // version 1
		packIndexEntry(20, false), // version 2
		packIndexEntry(30, true),  // version 3 (deleted)
		packIndexEntry(40, false), // version 4
	}

	idx := &Index{}

	// Read at version 5, should see version 4
	entry, ok := idx.searchHistory(hist, 50)
	if !ok || entry.Offset() != 40 {
		t.Errorf("Read at 50: expected offset 40, got %v (%v)", entry, ok)
	}

	// Read at version 35, should see the deleted marker and find nothing
	entry, ok = idx.searchHistory(hist, 35)
	if ok {
		t.Errorf("Read at 35: expected nothing (deleted), got %v", entry)
	}

	// Read at version 25, should see version 2
	entry, ok = idx.searchHistory(hist, 25)
	if !ok || entry.Offset() != 20 {
		t.Errorf("Read at 25: expected offset 20, got %v (%v)", entry, ok)
	}

	// Read at version 15, should see version 1
	entry, ok = idx.searchHistory(hist, 15)
	if !ok || entry.Offset() != 10 {
		t.Errorf("Read at 15: expected offset 10, got %v (%v)", entry, ok)
	}

	// Read at version 5, should see nothing
	_, ok = idx.searchHistory(hist, 5)
	if ok {
		t.Error("Read at 5: expected nothing")
	}

	// Empty history
	_, ok = idx.searchHistory([]IndexEntry{}, 100)
	if ok {
		t.Error("Empty history: expected nothing")
	}
}
