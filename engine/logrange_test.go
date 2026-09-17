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

func TestInitLogAtLSN_ApplyPreservesPrimaryOffsets(t *testing.T) {
	const origin int64 = 5_000
	leader := mustOpen(t, t.TempDir(), Options{})
	defer leader.Close()

	tx := leader.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	seg, endOff, err := leader.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	follower := mustOpen(t, t.TempDir(), Options{})
	defer follower.Close()
	if err := follower.InitLogAtLSN(origin); err != nil {
		t.Fatal(err)
	}
	if follower.LastLogOffset() != origin {
		t.Fatalf("LastLogOffset=%d want %d", follower.LastLogOffset(), origin)
	}
	if follower.OldestLogOffset() != origin {
		t.Fatalf("OldestLogOffset=%d want %d", follower.OldestLogOffset(), origin)
	}
	if err := follower.InitLogAtLSN(origin); err != nil {
		t.Fatal(err)
	}

	applied, err := follower.ApplyLogRange(seg)
	if err != nil {
		t.Fatal(err)
	}
	wantEnd := origin + endOff
	if applied != wantEnd {
		t.Fatalf("applied end %d want %d", applied, wantEnd)
	}

	readTx := follower.NewTransaction(false)
	defer readTx.Discard()
	got, err := readTx.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v" {
		t.Fatalf("got %q want v", got)
	}
}

func TestInitLogAtLSN_RejectsNonEmpty(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{})
	defer db.Close()
	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitLogAtLSN(100); err == nil {
		t.Fatal("expected error on non-empty log")
	}
}

func TestReadLogRange_AfterPurgeOldestRetained(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{
		WalSegmentSize:            256,
		IndexCompactOnRetention:   indexCompactDisabled(),
		WalCopyForwardOnRetention: walCopyForwardDisabled(),
	})
	defer db.Close()

	for i := 0; i < 40; i++ {
		if err := commitKeyValue(db, []byte(fmt.Sprintf("pad-%d", i)), "x"); err != nil {
			t.Fatal(err)
		}
	}
	if db.log.SegmentCount() < 2 {
		t.Fatalf("expected multiple segments, got %d", db.log.SegmentCount())
	}
	if err := commitKeyValue(db, []byte("keep"), "live"); err != nil {
		t.Fatal(err)
	}

	var deleteThrough int64
	for _, info := range db.log.SegmentInfos() {
		if info.Active {
			continue
		}
		deleteThrough = info.EndLSN
		break
	}
	if deleteThrough <= 0 {
		t.Fatal("expected a sealed segment to delete")
	}
	if _, _, err := db.log.deleteSegmentsThrough(deleteThrough); err != nil {
		t.Fatal(err)
	}
	floor := db.OldestLogOffset()
	if floor <= 0 {
		t.Fatalf("expected oldest LSN > 0 after purge, got %d", floor)
	}
	if _, _, err := db.ReadLogRange(0, 1<<20); err != ErrLogUnavailable {
		t.Fatalf("ReadLogRange(0) got %v want ErrLogUnavailable", err)
	}
	seg, endOff, err := db.ReadLogRange(floor, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(seg) == 0 {
		t.Fatal("expected retained frames")
	}

	readTx := db.NewTransaction(false)
	got, err := readTx.Get([]byte("keep"))
	readTx.Discard()
	if err != nil || string(got) != "live" {
		t.Fatalf("primary keep: %v %q", err, got)
	}

	follower := mustOpen(t, t.TempDir(), Options{})
	defer follower.Close()
	if err := follower.InitLogAtLSN(floor); err != nil {
		t.Fatal(err)
	}
	applied, err := follower.ApplyLogRange(seg)
	if err != nil {
		t.Fatal(err)
	}
	if applied != endOff {
		t.Fatalf("applied %d want %d", applied, endOff)
	}
	readTx = follower.NewTransaction(false)
	got, err = readTx.Get([]byte("keep"))
	readTx.Discard()
	if err != nil || string(got) != "live" {
		t.Fatalf("follower keep: %v %q", err, got)
	}
}
