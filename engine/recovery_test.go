// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"os"
	"testing"
)

func TestRecovery_CrashConsistency(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptTail: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	tx.Put([]byte("crash_key"), []byte("before_commit"))
	// Do not commit — simulate crash
	db.log.closeActiveFileForTest()
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	rtx := db2.NewTransaction(false)
	_, err = rtx.Get([]byte("crash_key"))
	if err != ErrKeyNotFound {
		t.Errorf("Uncommitted write should not survive crash, got %v", err)
	}
	if len(db2.clog) != 0 {
		t.Errorf("expected empty clog after crash recovery, got %d entries", len(db2.clog))
	}
}

func TestRecovery_LogReplay(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

	{
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		tx := db.NewTransaction(true)
		tx.Put([]byte("persist"), []byte("value"))
		tx.Commit()
		db.Close()
	}

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	tx := db2.NewTransaction(false)
	val, err := tx.Get([]byte("persist"))
	if err != nil || string(val) != "value" {
		t.Fatalf("replay failed: %v %q", err, val)
	}
	count, err := db2.KeyCount()
	if err != nil || count == 0 {
		t.Errorf("index empty after replay: count=%d err=%v", count, err)
	}
}

func TestRecovery_TruncateCorruptTail(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptTail: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	tx.Put([]byte("ok"), []byte("val"))
	tx.Commit()
	db.Close()

	logPath := db.log.activeSegmentPath()
	logicalEnd := db.log.WriteOffset() - db.log.segments[db.log.activeIndex].baseLSN
	f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF}, logicalEnd); err != nil {
		t.Fatal(err)
	}
	if err := writeSegmentFooter(f, db.log.segmentSize, logicalEnd+3); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open after truncate: %v", err)
	}
	defer db2.Close()

	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("ok"))
	if err != nil || string(val) != "val" {
		t.Fatalf("expected recovered key, got %v %q", err, val)
	}
}

func TestRecovery_KeyCountAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

	{
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte("after_reopen"), []byte("data")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	count, err := db2.KeyCount()
	if err != nil || count != 1 {
		t.Fatalf("KeyCount after reopen: want 1, got %d err=%v", count, err)
	}
	tx := db2.NewTransaction(false)
	val, err := tx.Get([]byte("after_reopen"))
	tx.Discard()
	if err != nil || string(val) != "data" {
		t.Fatalf("GET after reopen: err=%v val=%q", err, val)
	}
}

func TestRecovery_LargeReplay(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}
	const n = 5000

	{
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		tx := db.NewTransaction(true)
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("large-replay-%d", i))
			if err := tx.Put(key, []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	count, err := db2.KeyCount()
	if err != nil || count != n {
		t.Fatalf("KeyCount after large replay: want %d, got %d err=%v", n, count, err)
	}
}

func TestRecovery_UncommittedSetWithoutBegin(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptTail: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyRecord(Record{
		Type: RecordSet, XID: 42, Key: []byte("orphan"), Value: []byte("hidden"),
	}); err != nil {
		t.Fatal(err)
	}
	db.log.closeActiveFileForTest()
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	rtx := db2.NewTransaction(false)
	defer rtx.Discard()
	if _, err := rtx.Get([]byte("orphan")); err != ErrKeyNotFound {
		t.Fatalf("SET without COMMIT must not survive reopen, got %v", err)
	}
}

func TestRecovery_CopyForwardedSetAfterCommitStaysVisible(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptTail: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyRecord(Record{Type: RecordSet, XID: 7, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyRecord(Record{Type: RecordCommit, XID: 7}); err != nil {
		t.Fatal(err)
	}
	// Copy-forward appends the live SET frame again without a second COMMIT.
	if err := db.ApplyRecord(Record{Type: RecordSet, XID: 7, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	rtx := db2.NewTransaction(false)
	defer rtx.Discard()
	val, err := rtx.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("copy-forwarded SET after COMMIT must stay visible, got %v %q", err, val)
	}
}

func TestApplyRecord_CopyForwardedSetStaysVisibleLive(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.ApplyRecord(Record{Type: RecordSet, XID: 7, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyRecord(Record{Type: RecordCommit, XID: 7}); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyRecord(Record{Type: RecordSet, XID: 7, Key: []byte("k"), Value: []byte("v2")}); err != nil {
		t.Fatal(err)
	}

	rtx := db.NewTransaction(false)
	val, err := rtx.Get([]byte("k"))
	rtx.Discard()
	if err != nil || string(val) != "v2" {
		t.Fatalf("live GET between copied SET and extra COMMIT: %v %q", err, val)
	}

	if err := db.ApplyRecord(Record{Type: RecordCommit, XID: 7}); err != nil {
		t.Fatal(err)
	}
	rtx = db.NewTransaction(false)
	defer rtx.Discard()
	val, err = rtx.Get([]byte("k"))
	if err != nil || string(val) != "v2" {
		t.Fatalf("GET after extra COMMIT: %v %q", err, val)
	}
}

func TestApplyLogRange_CopyForwardedSetStaysVisibleLive(t *testing.T) {
	dir := t.TempDir()
	leader, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	if err := leader.ApplyRecord(Record{Type: RecordSet, XID: 9, Key: []byte("k"), Value: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := leader.ApplyRecord(Record{Type: RecordCommit, XID: 9}); err != nil {
		t.Fatal(err)
	}
	if err := leader.ApplyRecord(Record{Type: RecordSet, XID: 9, Key: []byte("k"), Value: []byte("b")}); err != nil {
		t.Fatal(err)
	}

	seg, _, err := leader.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	frames, err := validateFrames(seg)
	if err != nil || len(frames) < 3 {
		t.Fatalf("leader frames: n=%d err=%v", len(frames), err)
	}
	cut := frames[0].length + frames[1].length

	follower, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()
	if _, err := follower.ApplyLogRange(seg[:cut]); err != nil {
		t.Fatal(err)
	}
	if _, err := follower.ApplyLogRange(seg[cut:]); err != nil {
		t.Fatal(err)
	}
	rtx := follower.NewTransaction(false)
	val, err := rtx.Get([]byte("k"))
	rtx.Discard()
	if err != nil || string(val) != "b" {
		t.Fatalf("follower GET between copied SET and extra COMMIT: %v %q", err, val)
	}

	if err := follower.ApplyRecord(Record{Type: RecordCommit, XID: 9}); err != nil {
		t.Fatal(err)
	}
	rtx = follower.NewTransaction(false)
	defer rtx.Discard()
	val, err = rtx.Get([]byte("k"))
	if err != nil || string(val) != "b" {
		t.Fatalf("follower GET after extra COMMIT: %v %q", err, val)
	}
}
