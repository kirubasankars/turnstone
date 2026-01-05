// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package stonedb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_CrashConsistency(t *testing.T) {
	dir := t.TempDir()

	// 1. Open and write data
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.NewTransaction(true)
	tx.Put([]byte("persist"), []byte("true"))
	tx.Commit()

	// 2. "Crash" (Close without Checkpoint/Rotation implies WAL replay needed)
	db.Close()

	// 3. Re-open
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// 4. Verify data exists (recovered from WAL)
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("persist"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "true" {
		t.Errorf("Data lost after restart")
	}
}

func TestRecovery_IndexRebuild(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.NewTransaction(true)
	tx.Put([]byte("a"), []byte("1"))
	tx.Commit()
	db.Close()

	// Manually corrupt/delete the index directory
	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}

	// Re-open
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Verify data is still accessible
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get failed after index rebuild: %v", err)
	}
	if string(val) != "1" {
		t.Errorf("Index rebuild failed")
	}
}

func TestDB_Open_RebuildIndex_Corruption(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Write data
	tx := db.NewTransaction(true)
	tx.Put([]byte("a"), []byte("b"))
	tx.Commit()
	db.Close()

	// Corrupt Index: Delete MANIFEST or CURRENT
	os.Remove(filepath.Join(dir, "index", "CURRENT"))

	// Reopen - should detect LDB failure and rebuild
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Failed to open with corrupt index: %v", err)
	}
	defer db2.Close()

	// Verify data
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("a"))
	if err != nil || string(val) != "b" {
		t.Error("Data not found after index rebuild")
	}
}

func TestDB_Open_InconsistentIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Write data
	tx := db.NewTransaction(true)
	tx.Put([]byte("a"), []byte("b"))
	tx.Commit()

	// Manually mess up the TransactionID in LevelDB to be older than memory
	// This simulates a crash where Index wasn't flushed but WAL was?
	badID := make([]byte, 8)
	binary.BigEndian.PutUint64(badID, 0) // Reset to 0
	db.ldb.Put(sysTransactionIDKey, badID, nil)

	db.Close()

	// Reopen - should detect inconsistency (LDB ID < WAL ID) and rebuild
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db2.Close()

	// If rebuild happened, we should find "a"
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("a"))
	if err != nil || string(val) != "b" {
		t.Error("Index check failed to trigger rebuild or rebuild failed")
	}
}

func TestDB_LoadGarbageStats(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	// Manually inject garbage stats into LDB
	k := make([]byte, len(sysStaleBytesPrefix)+4)
	copy(k, sysStaleBytesPrefix)
	binary.BigEndian.PutUint32(k[len(sysStaleBytesPrefix):], 99) // File 99

	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, 1024) // 1024 bytes garbage

	db.ldb.Put(k, v, nil)
	db.Close()

	// Reopen
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Check loaded stats
	if stat, ok := db2.deletedBytesByFile[99]; !ok || stat != 1024 {
		t.Errorf("Failed to load garbage stats. Got %d", stat)
	}
}

func TestDB_LDB_CorruptFile(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	db.Close()

	// Corrupt the CURRENT file to be garbage (not missing)
	os.WriteFile(filepath.Join(dir, "index", "CURRENT"), []byte("GARBAGE"), 0o644)

	// Open should fail or rebuild
	db2, err := Open(dir, Options{})
	if err != nil {
		// Failure acceptable, but if success, ensure close
	} else {
		db2.Close()
	}
}
