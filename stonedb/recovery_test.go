// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_CrashConsistency(t *testing.T) {
	dir := t.TempDir()
	opts := Options{TruncateCorruptWAL: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	tx.Put([]byte("crash_key"), []byte("before_commit"))
	// Do not commit — simulate crash
	db.log.file.Close()
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
	opts := Options{TruncateCorruptWAL: true}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	tx.Put([]byte("ok"), []byte("val"))
	tx.Commit()
	db.Close()

	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF}); err != nil {
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

func TestRecovery_KeyCountAfterPromote(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

	{
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Promote(); err != nil {
			t.Fatal(err)
		}
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte("after_promote"), []byte("data")); err != nil {
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
		t.Fatalf("KeyCount after promote+reopen: want 1, got %d err=%v", count, err)
	}
	tx := db2.NewTransaction(false)
	val, err := tx.Get([]byte("after_promote"))
	tx.Discard()
	if err != nil || string(val) != "data" {
		t.Fatalf("GET after promote+reopen: err=%v val=%q", err, val)
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
		if err := db.Promote(); err != nil {
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

func TestRecovery_TimelineMeta(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	db.Promote()
	db.Close()

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.CurrentTimeline() != 1 {
		t.Errorf("expected timeline 1, got %d", db2.CurrentTimeline())
	}
}
