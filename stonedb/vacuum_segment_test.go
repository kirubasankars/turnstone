// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"fmt"
	"testing"
)

func TestRunVacuum_PerSegmentThreshold(t *testing.T) {
	const segSize = 2048
	dir := t.TempDir()
	db, err := Open(dir, Options{
		SegmentTargetSize:    segSize,
		CompactionMinGarbage: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("hot")
	val := func(i int) []byte { return []byte(fmt.Sprintf("value-%04d", i)) }

	// Fill the first segment and seal it on commit.
	for i := 0; i < 80; i++ {
		tx := db.NewTransaction(true)
		if err := tx.Put(key, val(i)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if db.segments.sealedCount() < 1 {
		t.Fatalf("expected at least one sealed segment, got %d", db.segments.sealedCount())
	}

	beforeStale := db.segments.staleBytes(0)
	if beforeStale < 200 {
		t.Fatalf("segment 0 stale bytes %d below threshold", beforeStale)
	}

	didWork, err := db.RunVacuum()
	if err != nil {
		t.Fatal(err)
	}
	if !didWork {
		t.Fatal("expected vacuum to process sealed segment")
	}
	if db.segments.staleBytes(0) != 0 {
		t.Fatalf("expected segment 0 stale cleared, got %d", db.segments.staleBytes(0))
	}

	tx := db.NewTransaction(false)
	got, err := tx.Get(key)
	tx.Discard()
	if err != nil {
		t.Fatalf("get after vacuum: %v", err)
	}
	if string(got) != string(val(79)) {
		t.Fatalf("latest value mismatch: %q", got)
	}
}

func TestRunVacuum_SkipsActiveSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		SegmentTargetSize:    4096,
		CompactionMinGarbage: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v1"))
	tx.Commit()
	tx = db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v2"))
	tx.Commit()

	if db.segments.sealedCount() != 0 {
		t.Fatalf("expected no sealed segments yet, got %d", db.segments.sealedCount())
	}

	didWork, err := db.RunVacuum()
	if err != nil {
		t.Fatal(err)
	}
	if didWork {
		t.Fatal("expected vacuum to skip active-only log")
	}
}
