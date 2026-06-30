// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"testing"
)

func TestDataLog_ScanFromByteOffset(t *testing.T) {
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

	head := db.LastLogOffset()
	mid, _, err := db.ReadLogRange(0, head/2)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := validateFrames(mid)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) == 0 {
		t.Fatal("expected frames in partial read")
	}
	startOffset := int64(0)
	for _, f := range frames {
		startOffset += f.length
	}

	foundCount := 0
	err = db.ScanLog(startOffset, func(recs []Record) error {
		foundCount += len(recs)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanLog failed: %v", err)
	}
	if foundCount == 0 {
		t.Fatal("expected records from mid offset")
	}
}

func TestDataLog_PurgeSetsScanFloor(t *testing.T) {
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

	head := db.LastLogOffset()
	_, floor, err := db.ReadLogRange(0, head/2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetScanFloor(floor); err != nil {
		t.Fatal(err)
	}
	if db.ScanFloor() != floor {
		t.Fatalf("expected scan floor %d, got %d", floor, db.ScanFloor())
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

	head := db.LastLogOffset()
	_, floor, err := db.ReadLogRange(0, head/2)
	if err != nil {
		t.Fatal(err)
	}
	if floor == 0 {
		t.Fatal("expected frame-aligned purge floor")
	}
	if err := db.SetScanFloor(floor); err != nil {
		t.Fatal(err)
	}

	if err := db.ScanLog(floor-1, func([]Record) error { return nil }); err != ErrLogUnavailable {
		t.Fatalf("ScanLog below floor: expected ErrLogUnavailable, got %v", err)
	}

	count := 0
	if err := db.ScanLog(floor, func(recs []Record) error {
		count += len(recs)
		return nil
	}); err != nil {
		t.Fatalf("ScanLog at floor failed: %v", err)
	}
	if count == 0 {
		t.Error("expected records at or above floor")
	}
}
