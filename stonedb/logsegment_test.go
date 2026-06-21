// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"fmt"
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

	seg, _, _, err := db.ReadLogSegment(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(seg) == 0 {
		t.Fatal("expected non-empty segment")
	}

	if _, err := validateLogSegment(seg[:len(seg)-1]); err == nil {
		t.Fatal("expected partial frame to be rejected")
	}
}

func TestReadLogSegment_StatementBoundaries(t *testing.T) {
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
		seg, next, _, err := db.ReadLogSegment(offset, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(seg) == 0 {
			break
		}
		frames, err := validateLogSegment(seg)
		if err != nil {
			t.Fatalf("segment not statement-safe at offset %d: %v", offset, err)
		}
		if len(frames) == 0 {
			t.Fatalf("expected at least one frame in segment at offset %d", offset)
		}
		offset = next
	}
}

func TestApplyLogSegment_RoundTrip(t *testing.T) {
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

	seg, _, lastOpID, err := leader.ReadLogSegment(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	follower, err := Open(followerDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	applied, err := follower.ApplyLogSegment(seg)
	if err != nil {
		t.Fatal(err)
	}
	if applied != lastOpID {
		t.Fatalf("expected last opID %d, got %d", lastOpID, applied)
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

func TestByteOffsetAfterOpID(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	tx.Commit()
	opID := db.LastOpID()

	off, ok := db.ByteOffsetAfterOpID(opID)
	if !ok {
		t.Fatal("expected offset for opID")
	}
	if off <= 0 {
		t.Fatalf("expected positive offset, got %d", off)
	}

	seg, next, _, err := db.ReadLogSegment(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if next != off {
		t.Fatalf("expected next offset %d after full read, got %d", off, next)
	}
	if len(seg) == 0 {
		t.Fatal("expected segment bytes")
	}
}
