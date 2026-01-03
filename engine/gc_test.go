package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/protocol"
)

// --- 4. Internals & Garbage Collection ---

func TestBufferRotation(t *testing.T) {
	// Temporarily lower threshold
	restore := SetVLogMaxFileSize(1024) // 1KB threshold
	defer restore()

	db, dir := setupDB(t)
	// IMPORTANT: No defer db.Close() here because we close manually to check files

	// Write 2KB of data
	bigVal := make([]byte, 512)
	for i := 0; i < 5; i++ {
		tx := db.BeginTx()
		tx.Put(fmt.Sprintf("k%d", i), bigVal)
		tx.Commit()
	}

	// Close the DB to ensure all background flushes complete and buffers are written to disk
	db.Close()

	// Check if multiple vlog files exist
	files, _ := os.ReadDir(dir)
	vlogCount := 0
	for _, f := range files {
		if len(f.Name()) > 5 && f.Name()[len(f.Name())-5:] == ".vlog" {
			vlogCount++
		}
	}

	if vlogCount < 2 {
		t.Errorf("Expected multiple vlog files for rotation test, got %d", vlogCount)
	}
}

func TestGarbageCollection(t *testing.T) {
	db, dir := setupDB(t)
	defer db.Close()

	// 1. Setup Phase: Create fragmentation

	// Transaction 1: Write "keep_me" and "update_me" to vlog File 0
	tx1 := db.BeginTx()
	tx1.Put("keep_me", []byte("v1"))
	tx1.Put("update_me", []byte("v1")) // This will become garbage
	tx1.Commit()
	tx1ID := tx1.ID

	// Force flush to disk -> Creates 0.vlog. Active becomes 1.
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush 1 failed: %v", err)
	}

	// Transaction 2: Update "update_me" and add "new_key" to vlog File 1
	tx2 := db.BeginTx()
	tx2.Put("update_me", []byte("v2")) // Updates previous key
	tx2.Put("new_key", []byte("v3"))
	tx2.Commit()

	// Force flush to disk -> Creates 1.vlog. Active becomes 2.
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush 2 failed: %v", err)
	}

	// Verify pre-conditions: Old version should still be readable via Time Travel
	valOld, err := db.Get("update_me", tx1ID)
	if err != nil || string(valOld) != "v1" {
		t.Fatalf("Pre-GC: Failed to read old version: %v", err)
	}

	// 2. Execution Phase: Run GC on old file
	// Important: We must ensure minActive > tx1ID so that tx1 is considered "inactive history".
	// We start a new transaction to establish a high read horizon.
	horizonTx := db.BeginTx()
	defer horizonTx.Abort()

	if err := db.RunVLogGC(0); err != nil {
		t.Fatalf("GC on file 0 failed: %v", err)
	}

	// Force flush again to ensure moved data is persisted to disk (in 2.vlog)
	// Retry loop for Flush in case of async overlap
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for flush during GC test")
		default:
		}
		err := db.Flush()
		if err == nil {
			break
		}
		if err == ErrFlushPending {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		t.Fatalf("Flush 3 failed: %v", err)
	}

	// 3. Verification Phase

	// A. "keep_me" was valid and unique, should have been moved to new log.
	val, err := db.Get("keep_me", db.CurrentTxID())
	if err != nil {
		t.Fatalf("Read 'keep_me' failed after GC: %v", err)
	}
	if string(val) != "v1" {
		t.Errorf("Data corruption in 'keep_me'. Expected v1, got %s", string(val))
	}

	// B. "update_me" (v1) was shadowed by v2. Since tx1ID < horizonTx.ID, it should be deleted.
	_, err = db.Get("update_me", tx1ID)
	if err != protocol.ErrKeyNotFound {
		t.Errorf("History Leak: Old version of 'update_me' should be deleted. Got: %v", err)
	}

	// Verify physical removal from LevelDB index using helper
	if has, err := db.HasIndex("update_me", tx1ID); err != nil {
		t.Fatal(err)
	} else if has {
		t.Error("Index Leak: Old version of 'update_me' still in LevelDB")
	}

	// C. "update_me" (v2) is the latest, should still exist.
	valV2, err := db.Get("update_me", db.CurrentTxID())
	if err != nil || string(valV2) != "v2" {
		t.Errorf("Latest version of 'update_me' missing or corrupt: %v", err)
	}

	// Verify file removal
	if _, err := os.Stat(filepath.Join(dir, "0.vlog")); !os.IsNotExist(err) {
		t.Errorf("Compaction failed: 0.vlog still exists")
	}
}

func TestTombstoneGC(t *testing.T) {
	db, dir := setupDB(t)
	defer db.Close()

	// 1. Write Key (Tx1) -> File 0
	tx1 := db.BeginTx()
	tx1.Put("del_key", []byte("data"))
	tx1.Commit()
	tx1ID := tx1.ID
	db.Flush() // 0.vlog

	// 2. Delete Key (Tx2) -> File 1
	tx2 := db.BeginTx()
	tx2.Delete("del_key")
	tx2.Commit()
	db.Flush() // 1.vlog

	// 3. GC File 0
	// Use horizon to allow cleanup
	horizonTx := db.BeginTx()
	defer horizonTx.Abort()

	if err := db.RunVLogGC(0); err != nil {
		t.Fatal(err)
	}
	// Flush GC writes
	db.Flush()

	// 4. Verify History is Gone
	// The original data at Tx1 should be wiped because it's shadowed by the tombstone
	// and is older than the horizon.
	_, err := db.Get("del_key", tx1ID)
	if err != protocol.ErrKeyNotFound {
		t.Errorf("Expected deleted key history to be removed, but found it")
	}

	// Verify physical removal from LevelDB index
	if has, err := db.HasIndex("del_key", tx1ID); err != nil {
		t.Fatal(err)
	} else if has {
		t.Error("Index Leak: Old version of 'del_key' still in LevelDB")
	}

	// 5. Verify Latest is still "Not Found" (Tombstone effect)
	_, err = db.Get("del_key", db.CurrentTxID())
	if err != protocol.ErrKeyNotFound {
		t.Errorf("Expected key to be deleted (tombstone), got %v", err)
	}

	// File 0 should be gone
	if _, err := os.Stat(filepath.Join(dir, "0.vlog")); !os.IsNotExist(err) {
		t.Error("0.vlog should be deleted")
	}
}
