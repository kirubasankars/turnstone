// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"fmt"
	"testing"
)

func TestBasicCRUD(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

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
	opts := Options{}

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
	opts := Options{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

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

	head := db.LastLogOffset()
	seg, _, err := db.ReadLogSegment(0, head/2)
	if err != nil {
		t.Fatalf("ReadLogSegment failed: %v", err)
	}
	frames, err := validateLogSegment(seg)
	if err != nil {
		t.Fatal(err)
	}
	startOffset := int64(0)
	for _, f := range frames {
		startOffset += f.length
	}

	foundCount := 0
	err = db.ScanWAL(startOffset, func(recs []WALRecord) error {
		foundCount += len(recs)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanWAL failed: %v", err)
	}
	if foundCount == 0 {
		t.Error("ScanWAL returned no records from mid offset")
	}
}
