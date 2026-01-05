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
	"fmt"
	"testing"
)

func TestBasicCRUD(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024} // 1MB

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// 1. Insert Data
	t.Run("Insert", func(t *testing.T) {
		tx := db.NewTransaction(true)
		defer tx.Discard()

		if err := tx.Put([]byte("user:1"), []byte("Alice")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := tx.Put([]byte("user:2"), []byte("Bob")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	})

	// 2. Read Data
	t.Run("Read", func(t *testing.T) {
		tx := db.NewTransaction(false)
		defer tx.Discard()

		val, err := tx.Get([]byte("user:1"))
		if err != nil {
			t.Fatalf("Get user:1 failed: %v", err)
		}
		if string(val) != "Alice" {
			t.Errorf("Expected Alice, got %s", val)
		}

		val, err = tx.Get([]byte("user:2"))
		if err != nil {
			t.Fatalf("Get user:2 failed: %v", err)
		}
		if string(val) != "Bob" {
			t.Errorf("Expected Bob, got %s", val)
		}

		_, err = tx.Get([]byte("user:3"))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound for missing key, got %v", err)
		}
	})

	// 3. Update Data
	t.Run("Update", func(t *testing.T) {
		tx := db.NewTransaction(true)
		defer tx.Discard()

		if err := tx.Put([]byte("user:1"), []byte("Alice Cooper")); err != nil {
			t.Fatalf("Update failed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify Update
		rtx := db.NewTransaction(false)
		defer rtx.Discard()
		val, err := rtx.Get([]byte("user:1"))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(val) != "Alice Cooper" {
			t.Errorf("Expected Alice Cooper, got %s", val)
		}
	})

	// 4. Delete Data
	t.Run("Delete", func(t *testing.T) {
		tx := db.NewTransaction(true)
		defer tx.Discard()

		if err := tx.Delete([]byte("user:2")); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify Delete
		rtx := db.NewTransaction(false)
		defer rtx.Discard()
		_, err := rtx.Get([]byte("user:2"))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound after delete, got %v", err)
		}
	})
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024 * 1024}

	// 1. Open and Write
	{
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("First open failed: %v", err)
		}

		tx := db.NewTransaction(true)
		if err := tx.Put([]byte("persist_key"), []byte("persist_val")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		tx.Discard()

		if err := db.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// 2. Reopen and Read
	{
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Second open failed: %v", err)
		}
		defer func() { _ = db.Close() }()

		tx := db.NewTransaction(false)
		defer tx.Discard()

		val, err := tx.Get([]byte("persist_key"))
		if err != nil {
			t.Fatalf("Get failed after reopen: %v", err)
		}
		if string(val) != "persist_val" {
			t.Errorf("Expected persist_val, got %s", val)
		}
	}
}

func TestWALScan(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxWALSize: 1024} // Small WAL size to force rotation

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write enough data to force multiple WAL files
	// ValueLogHeader is ~29 bytes. Payload is Key+Val.
	// We want to trigger rotation (1024 bytes).
	// Let's write 50 entries of ~50 bytes each -> 2500 bytes.

	const count = 50
	for i := 0; i < count; i++ {
		tx := db.NewTransaction(true)
		key := []byte(fmt.Sprintf("k%04d", i))
		val := []byte(fmt.Sprintf("v%04d", i))
		if err := tx.Put(key, val); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx.Discard()
	}

	// Scan from the middle
	startOpID := uint64(25)
	foundCount := 0

	err = db.ScanWAL(startOpID, func(entries []ValueLogEntry) error {
		for _, e := range entries {
			if e.OperationID < startOpID {
				t.Errorf("Got OpID %d, expected >= %d", e.OperationID, startOpID)
			}
			foundCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ScanWAL failed: %v", err)
	}

	// We wrote 'count' operations. OpIDs start at 1.
	// Scanning from 25 means we expect 25, 26, ... 50 (Total 26 items).
	expected := count - int(startOpID) + 1
	if foundCount != expected {
		t.Errorf("ScanWAL count mismatch: expected %d, got %d", expected, foundCount)
	}
}
