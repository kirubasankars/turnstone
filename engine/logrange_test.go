// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateLogSegment_RejectsPartialFrame(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	tx.Commit()

	seg, _, err := db.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(seg) == 0 {
		t.Fatal("expected non-empty segment")
	}

	if _, err := validateFrames(seg[:len(seg)-1]); err == nil {
		t.Fatal("expected partial frame to be rejected")
	}
}

func TestReadLogRange_StatementBoundaries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		tx.Commit()
	}

	var offset int64
	for {
		seg, next, err := db.ReadLogRange(offset, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(seg) == 0 {
			break
		}
		frames, err := validateFrames(seg)
		if err != nil {
			t.Fatalf("segment not statement-safe at offset %d: %v", offset, err)
		}
		if len(frames) == 0 {
			t.Fatalf("expected at least one frame in segment at offset %d", offset)
		}
		offset = next
	}
}

func TestApplyLogRange_RoundTrip(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leader, err := Open(leaderDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	for i := 0; i < 10; i++ {
		tx := leader.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("val-%d", i)))
		tx.Commit()
	}

	seg, endOffset, err := leader.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	follower, err := Open(followerDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	applied, err := follower.ApplyLogRange(seg)
	if err != nil {
		t.Fatal(err)
	}
	if applied != endOffset {
		t.Fatalf("expected end offset %d, got %d", endOffset, applied)
	}

	count, _ := follower.KeyCount()
	if count != 10 {
		t.Fatalf("expected 10 keys, got %d", count)
	}

	for i := 0; i < 10; i++ {
		tx := follower.NewTransaction(false)
		val, err := tx.Get([]byte(fmt.Sprintf("key-%d", i)))
		tx.Discard()
		if err != nil {
			t.Fatalf("key-%d: %v", i, err)
		}
		if string(val) != fmt.Sprintf("val-%d", i) {
			t.Fatalf("key-%d: got %q", i, val)
		}
	}
}

func TestIsFrameBoundary(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	tx.Commit()

	head := db.LastLogOffset()
	if !db.IsValidFrameOffset(0) {
		t.Fatal("offset 0 should be valid")
	}
	if !db.IsValidFrameOffset(head) {
		t.Fatal("head offset should be valid")
	}
	if db.IsValidFrameOffset(1) {
		t.Fatal("offset 1 should not be a frame boundary")
	}

	seg, next, err := db.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if next != head {
		t.Fatalf("expected next offset %d after full read, got %d", head, next)
	}
	if len(seg) == 0 {
		t.Fatal("expected segment bytes")
	}
}

func TestReadLogRange_MissingSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	tx.Commit()

	logPath := filepath.Join(dir, "wal", "seg-000001.wal")
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}

	_, _, err = db.ReadLogRange(0, 1<<20)
	if err != ErrLogUnavailable {
		t.Fatalf("expected ErrLogUnavailable after segment delete, got %v", err)
	}
	db.Close()
}
