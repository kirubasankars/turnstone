// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree_test

import (
	"fmt"
	"strings"
	"testing"
)

func TestLeafCountCap128(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 200; i++ {
		k := []byte{byte(i)}
		if err := tree.Put(k, k); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	if count != 200 {
		t.Fatalf("expected 200 keys, got %d", count)
	}
}

func TestLargeKeyPageSizeSplit(t *testing.T) {
	tree := openTree(t)
	keyPrefix := strings.Repeat("k", 200)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("%s-%04d", keyPrefix, i))
		v := []byte(fmt.Sprintf("v-%04d", i))
		if err := tree.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("%s-%04d", keyPrefix, i))
		want := []byte(fmt.Sprintf("v-%04d", i))
		got, err := tree.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("get %d: got %q want %q", i, got, want)
		}
	}
}

func TestMiddleLeafSplitOldNext(t *testing.T) {
	tree := openTree(t)
	// Fill enough sequential keys to create multiple leaves.
	for i := 0; i < 80; i++ {
		k := []byte(fmt.Sprintf("seq-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	// Insert keys that belong in an earlier leaf, forcing a middle split.
	for i := 0; i < 40; i++ {
		k := []byte(fmt.Sprintf("early-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	prev := ""
	for it.Next() {
		cur := string(it.Key())
		if prev > cur {
			t.Fatalf("order violation: %q after %q", cur, prev)
		}
		prev = cur
		count++
	}
	if count != 120 {
		t.Fatalf("expected 120 keys, got %d", count)
	}
}

func TestSplitInternalMultiLevel(t *testing.T) {
	tree := openTree(t)
	keyPrefix := strings.Repeat("x", 200)
	val := []byte("v")
	n := 5000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%s-%05d", keyPrefix, i))
		if err := tree.Put(k, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%s-%05d", keyPrefix, i))
		got, err := tree.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != string(val) {
			t.Fatalf("get %d: got %q", i, got)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	prev := ""
	for it.Next() {
		cur := string(it.Key())
		if prev > cur {
			t.Fatalf("order violation at %d: %q after %q", count, cur, prev)
		}
		prev = cur
		count++
	}
	if count != n {
		t.Fatalf("iterator count: got %d want %d", count, n)
	}
}

func TestSplitLeafUpdatesNextLink(t *testing.T) {
	tree := openTree(t)
	// Fill sequential keys to create a multi-leaf chain.
	for i := 0; i < 150; i++ {
		k := []byte(fmt.Sprintf("z-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	// Fill the first leaf until it splits and updates oldNext.
	for i := 0; i < 80; i++ {
		k := []byte(fmt.Sprintf("a-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	if count != 230 {
		t.Fatalf("expected 230 keys, got %d", count)
	}
}

func TestDeleteEmptyNonRootLeaves(t *testing.T) {
	tree := openTree(t)
	keyPrefix := strings.Repeat("d", 180)
	// Build a multi-level tree.
	for i := 0; i < 800; i++ {
		k := []byte(fmt.Sprintf("%s-%04d", keyPrefix, i))
		if err := tree.Put(k, []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Delete every other key to exercise leaf emptying in various positions.
	for i := 0; i < 800; i += 2 {
		k := []byte(fmt.Sprintf("%s-%04d", keyPrefix, i))
		if err := tree.Delete(k); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	// Remaining keys must still be readable.
	for i := 1; i < 800; i += 2 {
		k := []byte(fmt.Sprintf("%s-%04d", keyPrefix, i))
		if _, err := tree.Get(k); err != nil {
			t.Fatalf("get remaining %d: %v", i, err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	if count != 400 {
		t.Fatalf("expected 400 keys after delete, got %d", count)
	}
}
