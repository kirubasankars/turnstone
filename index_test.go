package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func runIndexTests(t *testing.T, factory func(t *testing.T) Index) {
	t.Run("CRUD", func(t *testing.T) {
		idx := factory(t)
		defer idx.Close()
		testIndex_CRUD(t, idx)
	})
	t.Run("Versioning", func(t *testing.T) {
		idx := factory(t)
		defer idx.Close()
		testIndex_Versioning(t, idx)
	})
	t.Run("Checkpoints", func(t *testing.T) {
		idx := factory(t)
		defer idx.Close()
		testIndex_Checkpoints(t, idx)
	})
}

func testIndex_CRUD(t *testing.T, idx Index) {
	key := "key1"
	idx.Set(key, 100, 10, 1, false, 0)

	entry, ok := idx.GetHead(key)
	if !ok || entry.Offset != 100 {
		t.Fatal("GetHead failed")
	}

	idx.Remove(key)
	_, ok = idx.GetHead(key)
	if ok {
		t.Error("Remove failed")
	}
}

func testIndex_Versioning(t *testing.T, idx Index) {
	key := "vKey"
	idx.Set(key, 100, 10, 10, false, 0)
	idx.Set(key, 200, 20, 20, false, 0)

	// Read old version
	entry, ok := idx.Get(key, 15)
	if !ok || entry.LSN != 10 {
		t.Errorf("Read 15 mismatch. Want 10, got %v", entry)
	}
	// Read newer version
	entry, ok = idx.Get(key, 25)
	if !ok || entry.LSN != 20 {
		t.Errorf("Read 25 mismatch. Want 20, got %v", entry)
	}
}

func testIndex_Checkpoints(t *testing.T, idx Index) {
	idx.PutCheckpoint(100, 12345)
	ckpts, err := idx.GetCheckpoints()
	if err != nil {
		t.Fatal(err)
	}
	if off, ok := ckpts[100]; !ok || off != 12345 {
		t.Errorf("Checkpoint mismatch")
	}
}

// Implementations

func TestLevelDBIndex(t *testing.T) {
	dir := t.TempDir()
	factory := func(t *testing.T) Index {
		subDir := filepath.Join(dir, fmt.Sprintf("db-%d", time.Now().UnixNano()))
		os.MkdirAll(subDir, 0o755)
		idx, err := NewLevelDBIndex(subDir)
		if err != nil {
			t.Fatalf("Failed to open leveldb: %v", err)
		}
		return idx
	}
	runIndexTests(t, factory)
}
