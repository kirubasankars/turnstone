// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"fmt"
	"testing"
)

func TestDataLog_SparseOpOffsetsDensity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const commits = 200
	for i := 0; i < commits; i++ {
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	head := db.LastOpID()
	maxSparse := int(head/opIndexSparse) + 2
	if len(db.log.opOffsets) > maxSparse {
		t.Fatalf("expected at most %d sparse entries, got %d (head=%d)", maxSparse, len(db.log.opOffsets), head)
	}
}

func TestDataLog_ScanAfterSparseIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const count = 50
	for i := 0; i < count; i++ {
		tx := db.NewTransaction(true)
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := tx.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	headOpID := db.LastOpID()
	startOpID := headOpID / 2
	foundCount := 0
	err = db.ScanWAL(startOpID, func(recs []WALRecord) error {
		for _, r := range recs {
			if r.OpID < startOpID {
				t.Errorf("got OpID %d, expected >= %d", r.OpID, startOpID)
			}
			foundCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ScanWAL failed: %v", err)
	}
	expected := int(headOpID-startOpID) + 1
	if foundCount != expected {
		t.Errorf("ScanWAL count mismatch: expected %d, got %d", expected, foundCount)
	}
}

func TestDataLog_PurgeTrimsOpOffsets(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	head := db.LastOpID()
	floor := head / 2
	if err := db.PurgeWAL(floor); err != nil {
		t.Fatal(err)
	}

	for op := range db.log.opOffsets {
		if op < floor {
			t.Fatalf("opOffsets still contains op %d below floor %d", op, floor)
		}
	}
	if db.log.scanAnchor.opID >= floor {
		t.Fatalf("scanAnchor op %d should be below floor %d", db.log.scanAnchor.opID, floor)
	}
	maxSparse := int(head/opIndexSparse) - int(floor/opIndexSparse) + 2
	if len(db.log.opOffsets) > maxSparse {
		t.Fatalf("expected at most %d sparse entries after purge, got %d", maxSparse, len(db.log.opOffsets))
	}
}

func TestDataLog_ScanAtFloorAfterPurge(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	head := db.LastOpID()
	floor := head / 2
	if err := db.PurgeWAL(floor); err != nil {
		t.Fatal(err)
	}

	if err := db.ScanWAL(floor-1, func([]WALRecord) error { return nil }); err != ErrLogUnavailable {
		t.Fatalf("ScanWAL below floor: expected ErrLogUnavailable, got %v", err)
	}

	count := 0
	if err := db.ScanWAL(floor, func(recs []WALRecord) error {
		count += len(recs)
		return nil
	}); err != nil {
		t.Fatalf("ScanWAL at floor failed: %v", err)
	}
	expected := int(head-floor) + 1
	if count != expected {
		t.Errorf("ScanWAL at floor: expected %d records, got %d", expected, count)
	}
}
