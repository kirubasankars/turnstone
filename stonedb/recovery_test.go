// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
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
	if db2.index.CountKeys() == 0 {
		t.Error("index empty after replay")
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
