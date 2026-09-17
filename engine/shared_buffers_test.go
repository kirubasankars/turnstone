// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"bytes"
	"testing"
)

func TestDB_SharedBuffersHitOnSecondGet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ValueCacheBytes: -1, SharedBuffersBytes: 128 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.log.buffers == nil {
		t.Fatal("expected shared buffers")
	}

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("buf"), []byte("shared-page")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	misses := db.log.buffers.Misses()
	rtx := db.NewTransaction(false)
	val, err := rtx.Get([]byte("buf"))
	rtx.Discard()
	if err != nil || string(val) != "shared-page" {
		t.Fatalf("first Get: %v %q", err, val)
	}
	if db.log.buffers.Misses() <= misses {
		t.Fatal("first Get should populate shared buffers")
	}

	hits := db.log.buffers.Hits()
	rtx = db.NewTransaction(false)
	val, err = rtx.Get([]byte("buf"))
	rtx.Discard()
	if err != nil || string(val) != "shared-page" {
		t.Fatalf("second Get: %v %q", err, val)
	}
	if db.log.buffers.Hits() <= hits {
		t.Fatal("second Get should hit shared buffers")
	}
}

func TestDB_SharedBuffersDisabled(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ValueCacheBytes: -1, SharedBuffersBytes: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.log.buffers != nil {
		t.Fatal("SharedBuffersBytes < 0 should disable the page pool")
	}

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	val, err := rtx.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("Get without buffers: %v %q", err, val)
	}
}

func TestDB_SharedBuffersWriteThroughKeepsNeighbor(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ValueCacheBytes: -1, SharedBuffersBytes: 128 << 10, WalSegmentSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i, v := range []string{"one", "two", "three"} {
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte{byte('a' + i)}, []byte(v)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	rtx := db.NewTransaction(false)
	got, err := rtx.Get([]byte("a"))
	rtx.Discard()
	if err != nil || !bytes.Equal(got, []byte("one")) {
		t.Fatalf("Get a: %v %q", err, got)
	}

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("d"), []byte("four")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rtx = db.NewTransaction(false)
	defer rtx.Discard()
	got, err = rtx.Get([]byte("a"))
	if err != nil || !bytes.Equal(got, []byte("one")) {
		t.Fatalf("Get a after neighbor write: %v %q", err, got)
	}
	got, err = rtx.Get([]byte("d"))
	if err != nil || !bytes.Equal(got, []byte("four")) {
		t.Fatalf("Get d: %v %q", err, got)
	}
}

func TestSharedBuffers_ClockEvicts(t *testing.T) {
	b := newSharedBuffers(sharedBufferPageSize * sharedBufferMinPages)
	fill := func(dst []byte) error {
		for i := range dst {
			dst[i] = 1
		}
		return nil
	}
	var pinned []int
	for page := uint32(0); page < uint32(sharedBufferMinPages+4); page++ {
		idx, data, err := b.pin(bufferTag{id: 1, page: page}, fill)
		if err != nil {
			t.Fatal(err)
		}
		if data[0] != 1 {
			t.Fatal("expected filled page")
		}
		b.unpin(idx)
		pinned = append(pinned, idx)
	}
	if b.Misses() < uint64(sharedBufferMinPages) {
		t.Fatalf("misses %d, want at least %d", b.Misses(), sharedBufferMinPages)
	}
	if b.evicts == 0 {
		t.Fatal("expected clock sweep to evict a page")
	}
	_ = pinned
}
