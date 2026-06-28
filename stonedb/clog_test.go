// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"testing"
)

func TestClog_EmptyAfterManyCommits(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		tx := db.NewTransaction(true)
		key := []byte("k")
		if err := tx.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	if len(db.clog) != 0 {
		t.Fatalf("expected empty clog after commits, got %d entries", len(db.clog))
	}

	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	val, err := rtx.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("Get after commits: %v %q", err, val)
	}
}

func TestClog_EmptyAfterAbort(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	tx.Discard()

	if len(db.clog) != 0 {
		t.Fatalf("expected empty clog after abort, got %d entries", len(db.clog))
	}

	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	_, err = rtx.Get([]byte("k"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected key not found after abort, got %v", err)
	}
}

func TestClog_EmptyAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("persist"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if len(db2.clog) != 0 {
		t.Fatalf("expected empty clog after reopen, got %d entries", len(db2.clog))
	}

	rtx := db2.NewTransaction(false)
	defer rtx.Discard()
	val, err := rtx.Get([]byte("persist"))
	if err != nil || string(val) != "data" {
		t.Fatalf("Get after reopen: %v %q", err, val)
	}
}

func TestClog_EmptyAfterApplyRecordAbort(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const xid = uint64(42)
	recs := []WALRecord{
		{Type: WALRecordBegin, XID: xid},
		{Type: WALRecordSet, XID: xid, Key: []byte("k"), Value: []byte("v")},
		{Type: WALRecordAbort, XID: xid},
	}
	for _, r := range recs {
		if err := db.ApplyRecord(r); err != nil {
			t.Fatalf("ApplyRecord: %v", err)
		}
	}

	if len(db.clog) != 0 {
		t.Fatalf("expected empty clog after ApplyRecord abort, got %d entries", len(db.clog))
	}

	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	_, err = rtx.Get([]byte("k"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected key not found after abort, got %v", err)
	}
}
