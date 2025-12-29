package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Helper to run generic tests against different Index implementations
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
	t.Run("UpdateHead", func(t *testing.T) {
		idx := factory(t)
		defer idx.Close()
		testIndex_UpdateHead(t, idx)
	})
	t.Run("Checkpoints", func(t *testing.T) {
		idx := factory(t)
		defer idx.Close()
		testIndex_Checkpoints(t, idx)
	})
}

func testIndex_CRUD(t *testing.T, idx Index) {
	key := "key1"

	// 1. Set
	idx.Set(key, 100, 10, 1, false, 0)

	// 2. GetHead
	entry, ok := idx.GetHead(key)
	if !ok {
		t.Fatal("GetHead failed")
	}
	if entry.Offset != 100 || entry.Length != 10 || entry.LSN != 1 {
		t.Errorf("Entry mismatch: %+v", entry)
	}

	// 3. GetLatest
	entry, ok = idx.GetLatest(key)
	if !ok {
		t.Fatal("GetLatest failed")
	}
	if entry.Offset != 100 {
		t.Errorf("GetLatest offset mismatch")
	}

	// 4. Remove
	idx.Remove(key)
	_, ok = idx.GetHead(key)
	if ok {
		t.Error("Remove failed, key still exists")
	}
}

func testIndex_Versioning(t *testing.T, idx Index) {
	key := "vKey"

	// Write v1 at LSN 10
	idx.Set(key, 100, 10, 10, false, 0)
	// Write v2 at LSN 20
	idx.Set(key, 200, 20, 20, false, 0)
	// Write v3 (Delete) at LSN 30
	idx.Set(key, 300, 30, 30, true, 0)

	// Read at LSN 5 -> Should not find
	_, ok := idx.Get(key, 5)
	if ok {
		t.Error("Read at LSN 5 found data")
	}

	// Read at LSN 15 -> Should see v1
	entry, ok := idx.Get(key, 15)
	if !ok || entry.LSN != 10 || entry.Offset != 100 {
		t.Errorf("Read at LSN 15 mismatch. Want v1 (10), got %v", entry)
	}

	// Read at LSN 25 -> Should see v2
	entry, ok = idx.Get(key, 25)
	if !ok || entry.LSN != 20 || entry.Offset != 200 {
		t.Errorf("Read at LSN 25 mismatch. Want v2 (20), got %v", entry)
	}

	// Read at LSN 35 -> Should see v3 (Deleted) -> Get returns not found usually if deleted
	// The store implementation usually interprets Deleted=true as "KeyNotFound",
	// but the Index layer returns the entry with Deleted=true or false depending on logic.
	// Looking at MemoryIndex.Get: "if versions[i].Deleted { return IndexEntry{}, false }"
	// So Get should return false.
	_, ok = idx.Get(key, 35)
	if ok {
		t.Error("Read at LSN 35 (after delete) returned true")
	}
}

func testIndex_UpdateHead(t *testing.T, idx Index) {
	key := "uKey"
	idx.Set(key, 100, 10, 50, false, 0)

	// Update head with SAME LSN (compaction scenario)
	success := idx.UpdateHead(key, 999, 10, 50)
	if !success {
		t.Error("UpdateHead failed for matching LSN")
	}

	entry, ok := idx.GetHead(key)
	if !ok || entry.Offset != 999 {
		t.Errorf("UpdateHead did not update offset. Got %d", entry.Offset)
	}

	// Try UpdateHead with WRONG LSN
	success = idx.UpdateHead(key, 888, 10, 51)
	if success {
		t.Error("UpdateHead succeeded for mismatching LSN")
	}

	entry, _ = idx.GetHead(key)
	if entry.Offset == 888 {
		t.Error("UpdateHead changed offset despite LSN mismatch")
	}
}

func testIndex_Checkpoints(t *testing.T, idx Index) {
	idx.PutCheckpoint(100, 12345)
	idx.PutCheckpoint(200, 67890)

	ckpts, err := idx.GetCheckpoints()
	if err != nil {
		t.Fatalf("GetCheckpoints error: %v", err)
	}

	if off, ok := ckpts[100]; !ok || off != 12345 {
		t.Errorf("Checkpoint 100 mismatch")
	}
	if off, ok := ckpts[200]; !ok || off != 67890 {
		t.Errorf("Checkpoint 200 mismatch")
	}
}

// --- Implementation Specific Tests ---

func TestMemoryIndex(t *testing.T) {
	factory := func(t *testing.T) Index {
		i, _ := NewMemoryIndex("")
		return i
	}
	runIndexTests(t, factory)

	t.Run("Offload", func(t *testing.T) {
		idx, _ := NewMemoryIndex("")
		testIndex_Offload_Memory(t, idx)
	})
}

func TestShardedIndex(t *testing.T) {
	factory := func(t *testing.T) Index {
		i, _ := NewShardedIndex("")
		return i
	}
	runIndexTests(t, factory)

	t.Run("Offload", func(t *testing.T) {
		idx, _ := NewShardedIndex("")
		testIndex_Offload_Memory(t, idx) // Sharded uses similar logic
	})
}

func TestLevelDBIndex(t *testing.T) {
	dir := t.TempDir()
	factory := func(t *testing.T) Index {
		// NewLevelDBIndex requires a dir.
		// We can reuse the dir but must ensure unique paths for concurrency safety in subtests.
		subDir := filepath.Join(dir, fmt.Sprintf("db-%d", time.Now().UnixNano()))
		os.MkdirAll(subDir, 0o755)
		idx, err := NewLevelDBIndex(subDir)
		if err != nil {
			t.Fatalf("Failed to open leveldb: %v", err)
		}
		return idx
	}
	runIndexTests(t, factory)

	t.Run("Persistence", func(t *testing.T) {
		dbDir := filepath.Join(dir, "persist-test")
		os.MkdirAll(dbDir, 0o755)

		// 1. Write
		idx1, err := NewLevelDBIndex(dbDir)
		if err != nil {
			t.Fatal(err)
		}
		idx1.Set("pKey", 100, 10, 1, false, 0)
		if err := idx1.PutState(50, 5000); err != nil {
			t.Fatal(err)
		}
		idx1.Close()

		// 2. Re-open
		idx2, err := NewLevelDBIndex(dbDir)
		if err != nil {
			t.Fatal(err)
		}
		defer idx2.Close()

		entry, ok := idx2.GetHead("pKey")
		if !ok || entry.Offset != 100 {
			t.Error("Failed to recover key from LevelDB")
		}

		lsn, off, err := idx2.GetState()
		if err != nil {
			t.Errorf("GetState error: %v", err)
		}
		if lsn != 50 || off != 5000 {
			t.Errorf("State mismatch. Want 50/5000, got %d/%d", lsn, off)
		}
	})

	t.Run("Offload", func(t *testing.T) {
		dbDir := filepath.Join(dir, "offload-test")
		os.MkdirAll(dbDir, 0o755)
		idx, err := NewLevelDBIndex(dbDir)
		if err != nil {
			t.Fatal(err)
		}
		defer idx.Close()
		testIndex_Offload_LevelDB(t, idx)
	})
}

// Logic for offloading is specific to in-memory implementation details regarding slice manipulation
func testIndex_Offload_Memory(t *testing.T, idx Index) {
	key := "cleanKey"
	// Create history: LSN 10, 20, 30, 40
	idx.Set(key, 10, 1, 10, false, 0)
	idx.Set(key, 20, 1, 20, false, 0)
	idx.Set(key, 30, 1, 30, false, 0)
	idx.Set(key, 40, 1, 40, false, 0)

	// MinReadLSN = 25.
	// Versions < 25 are 10, 20.
	// Logic: Keep versions >= MinReadLSN (30, 40).
	// AND keep at least one version < MinReadLSN (20) to satisfy reads at LSN 25 (which sees 20).
	// Prune 10.

	count, err := idx.OffloadColdKeys(25)
	if err != nil {
		t.Fatalf("Offload error: %v", err)
	}

	// We expect 1 pruned (LSN 10).
	if count != 1 {
		t.Errorf("Expected 1 pruned, got %d", count)
	}

	// Verify availability
	// LSN 25 should return version 20 (the one we kept as the "visible previous")
	entry, ok := idx.Get(key, 25)
	if !ok || entry.LSN != 20 {
		t.Errorf("Pruning broke visibility for MinReadLSN. Got %v", entry)
	}
}

// testIndex_Offload_LevelDB verifies that cold versions are removed from LevelDB
// while maintaining snapshot consistency.
func testIndex_Offload_LevelDB(t *testing.T, idx Index) {
	key := "coldKey"
	// Create history: LSN 10, 20, 30, 40
	idx.Set(key, 100, 10, 10, false, 0)
	idx.Set(key, 200, 10, 20, false, 0)
	idx.Set(key, 300, 10, 30, false, 0)
	idx.Set(key, 400, 10, 40, false, 0)

	// MinReadLSN = 25.
	// Expected behavior:
	// - LSN 40 (>= 25) -> Kept
	// - LSN 30 (>= 25) -> Kept
	// - LSN 20 (< 25, is the latest old version) -> Kept (for snapshot reads at 25)
	// - LSN 10 (< 25, older) -> Pruned

	count, err := idx.OffloadColdKeys(25)
	if err != nil {
		t.Fatalf("Offload error: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 pruned (LSN 10), got %d", count)
	}

	// Verify LSN 10 is gone
	// Get(key, 15) would return LSN 10 if it existed (since 10 <= 15).
	// Since 10 is pruned and next is 20 (> 15), this should return not found.
	if _, ok := idx.Get(key, 15); ok {
		t.Error("Read at LSN 15 should return nothing (LSN 10 should be pruned)")
	}

	// Verify LSN 20 exists (latest < 25)
	entry, ok := idx.Get(key, 25)
	if !ok || entry.LSN != 20 {
		t.Errorf("LSN 20 should be kept as visible version. Got %v", entry)
	}

	// Verify LSN 30 exists
	entry, ok = idx.Get(key, 35)
	if !ok || entry.LSN != 30 {
		t.Errorf("LSN 30 should be kept. Got %v", entry)
	}
}
