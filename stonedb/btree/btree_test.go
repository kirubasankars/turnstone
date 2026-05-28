// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree_test

import (
	"fmt"
	"testing"

	"turnstone/stonedb/btree"
)

func TestBTreeBasic(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%04d", i))
		if err := tree.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		want := []byte(fmt.Sprintf("val-%04d", i))
		got, err := tree.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("get %d: got %q want %q", i, got, want)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	if count != 1000 {
		t.Fatalf("expected 1000 keys, got %d", count)
	}
}

func TestBTreeDelete(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i += 2 {
		if err := tree.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	if count != 50 {
		t.Fatalf("expected 50 keys after delete, got %d", count)
	}
}

func TestBTreeSeekPrevLast(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	keys := []string{"a", "c", "e", "g"}
	for _, k := range keys {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()

	if !it.Seek([]byte("d")) {
		t.Fatal("seek d should be valid")
	}
	if string(it.Key()) != "e" {
		t.Fatalf("seek d: got %q", it.Key())
	}
	if !it.Prev() {
		t.Fatal("prev should work")
	}
	if string(it.Key()) != "c" {
		t.Fatalf("prev: got %q", it.Key())
	}
	if !it.Last() {
		t.Fatal("last should work")
	}
	if string(it.Key()) != "g" {
		t.Fatalf("last: got %q", it.Key())
	}
}

func TestBTreeBatch(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	puts := [][2][]byte{{[]byte("a"), []byte("1")}, {[]byte("b"), []byte("2")}}
	if err := tree.ApplyBatch(puts, nil, true); err != nil {
		t.Fatal(err)
	}
	v, err := tree.Get([]byte("b"))
	if err != nil || string(v) != "2" {
		t.Fatalf("batch get: %v %q", err, v)
	}
}

func TestBTreeReopen(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("persist"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Sync(); err != nil {
		t.Fatal(err)
	}
	tree.Close()

	tree2, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree2.Close()
	v, err := tree2.Get([]byte("persist"))
	if err != nil || string(v) != "yes" {
		t.Fatalf("reopen: %v %q", err, v)
	}
}
