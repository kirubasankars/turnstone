// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree_test

import (
	"errors"
	"fmt"
	"testing"

	"turnstone/stonedb/btree"
)

func TestIteratorEmptyTree(t *testing.T) {
	tree := openTree(t)
	it := tree.NewIterator(nil)
	defer it.Release()

	if it.First() || it.Last() || it.Seek([]byte("a")) || it.Prev() {
		t.Fatal("empty tree iterator ops should be false")
	}
	if it.Key() != nil || it.Value() != nil {
		t.Fatal("empty tree Key/Value should be nil")
	}
	if it.Next() {
		t.Fatal("Next on empty tree should return false")
	}
	if it.Valid() {
		t.Fatal("empty tree iterator should be invalid")
	}
}

func TestIteratorClosedTree(t *testing.T) {
	tree := openTree(t)
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}
	it := tree.NewIterator(nil)
	defer it.Release()
	if !errors.Is(it.Error(), btree.ErrClosed) {
		t.Fatalf("new iterator on closed tree: %v", it.Error())
	}
}

func TestIteratorSeekAndValue(t *testing.T) {
	tree := openTree(t)
	for _, k := range []string{"a", "c", "e", "g", "i"} {
		if err := tree.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()

	cases := []struct {
		seek   string
		want   string
		valid  bool
	}{
		{"", "a", true},
		{"a", "a", true},
		{"b", "c", true},
		{"d", "e", true},
		{"e", "e", true},
		{"z", "", false},
	}
	for _, tc := range cases {
		if tc.seek == "" {
			if !it.Seek([]byte("0")) {
				if tc.valid {
					t.Fatalf("seek %q: expected valid", tc.seek)
				}
				continue
			}
		} else if !it.Seek([]byte(tc.seek)) {
			if tc.valid {
				t.Fatalf("seek %q: expected valid", tc.seek)
			}
			continue
		}
		if !tc.valid {
			t.Fatalf("seek %q: expected invalid", tc.seek)
		}
		if !it.Valid() {
			t.Fatalf("seek %q: Valid() false", tc.seek)
		}
		if string(it.Key()) != tc.want {
			t.Fatalf("seek %q: key %q want %q", tc.seek, it.Key(), tc.want)
		}
		if string(it.Value()) != "v-"+tc.want {
			t.Fatalf("seek %q: value %q", tc.seek, it.Value())
		}
	}
}

func TestIteratorNextOnInvalid(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	if !it.Next() {
		t.Fatal("Next on invalid should act like First")
	}
	if string(it.Key()) != "k0" {
		t.Fatalf("first key: got %q", it.Key())
	}
}

func TestIteratorPrevInvalid(t *testing.T) {
	tree := openTree(t)
	it := tree.NewIterator(nil)
	defer it.Release()
	if it.Prev() {
		t.Fatal("Prev on invalid should be false")
	}
}

func TestIteratorRangeStartOnly(t *testing.T) {
	tree := openTree(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("c")})
	defer it.Release()
	if !it.First() {
		t.Fatal("First should succeed")
	}
	if string(it.Key()) != "c" {
		t.Fatalf("First: got %q want c", it.Key())
	}

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		if !it.Next() {
			break
		}
	}
	if len(keys) != 3 || keys[0] != "c" || keys[2] != "e" {
		t.Fatalf("start-only range: %v", keys)
	}
}

func TestIteratorRangeLimitOnly(t *testing.T) {
	tree := openTree(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Limit: []byte("c")})
	defer it.Release()
	if !it.First() {
		t.Fatal("First should succeed")
	}
	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		if !it.Next() {
			break
		}
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("limit-only range: %v", keys)
	}
}

func TestIteratorRangeBoth(t *testing.T) {
	tree := openTree(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("b"), Limit: []byte("e")})
	defer it.Release()
	if !it.First() {
		t.Fatal("First should succeed")
	}
	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		if !it.Next() {
			break
		}
	}
	if len(keys) != 3 || keys[0] != "b" || keys[2] != "d" {
		t.Fatalf("bounded range: %v", keys)
	}
}

func TestIteratorRangeEmpty(t *testing.T) {
	tree := openTree(t)
	for _, k := range []string{"a", "b", "c"} {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("x"), Limit: []byte("y")})
	defer it.Release()
	if it.First() {
		t.Fatal("empty range First should be false")
	}
	if it.Last() {
		t.Fatal("empty range Last should be false")
	}
}

func TestIteratorLastInRange(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("key-010"), Limit: []byte("key-020")})
	defer it.Release()
	if !it.Last() {
		t.Fatal("Last in range should succeed")
	}
	if string(it.Key()) != "key-019" {
		t.Fatalf("Last in range: got %q want key-019", it.Key())
	}
}

func TestIteratorFirstStartInLaterLeaf(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("leaf-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("leaf-0150")})
	defer it.Release()
	if !it.First() {
		t.Fatal("First with start in later leaf should succeed")
	}
	if string(it.Key()) != "leaf-0150" {
		t.Fatalf("First: got %q", it.Key())
	}
}

func TestIteratorPrevCrossLeaf(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("p-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	if !it.Seek([]byte("p-0100")) {
		t.Fatal("seek should succeed")
	}
	if !it.Prev() {
		t.Fatal("prev should succeed")
	}
	if string(it.Key()) != "p-0099" {
		t.Fatalf("prev cross leaf: got %q", it.Key())
	}
}

func TestIteratorLastWalksPreviousLeaves(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("key-010"), Limit: []byte("key-015")})
	defer it.Release()
	if !it.Last() {
		t.Fatal("Last should walk back to in-range key")
	}
	if string(it.Key()) != "key-014" {
		t.Fatalf("Last: got %q want key-014", it.Key())
	}
}

func TestIteratorPrevSkipsOutOfRange(t *testing.T) {
	tree := openTree(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := tree.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("b"), Limit: []byte("e")})
	defer it.Release()
	if !it.Seek([]byte("d")) {
		t.Fatal("seek should succeed")
	}
	if !it.Prev() {
		t.Fatal("prev should succeed")
	}
	if string(it.Key()) != "c" {
		t.Fatalf("prev: got %q want c", it.Key())
	}
	if !it.Prev() {
		t.Fatal("prev should succeed again")
	}
	if string(it.Key()) != "b" {
		t.Fatalf("prev: got %q want b", it.Key())
	}
}

func TestIteratorAdvanceToValidSkipsBeforeStart(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("scan-%03d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("scan-050")})
	defer it.Release()
	if !it.Seek([]byte("scan-000")) {
		t.Fatal("seek before start should advance to first in-range key")
	}
	if string(it.Key()) != "scan-050" {
		t.Fatalf("seek: got %q want scan-050", it.Key())
	}
}

func TestIteratorLastMultiLeafErrorPath(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 150; i++ {
		k := []byte(fmt.Sprintf("last-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("last-0005"), Limit: []byte("last-0010")})
	defer it.Release()
	if !it.Last() {
		t.Fatal("Last should find in-range key in earlier leaf")
	}
	if string(it.Key()) != "last-0009" {
		t.Fatalf("Last: got %q", it.Key())
	}
}

func TestIteratorPrevCrossLeafSkips(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("pv-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("pv-0030"), Limit: []byte("pv-0135")})
	defer it.Release()
	if !it.Seek([]byte("pv-0130")) {
		t.Fatal("seek should succeed")
	}
	if !it.Prev() {
		t.Fatal("prev into previous leaf should succeed")
	}
	if string(it.Key()) != "pv-0129" {
		t.Fatalf("prev after cross-leaf: got %q want pv-0129", it.Key())
	}
}

func TestIteratorPrevSkipsOutOfRangeInLeaf(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("sk-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(&btree.Range{Start: []byte("sk-0064"), Limit: []byte("sk-0065")})
	defer it.Release()
	if !it.Seek([]byte("sk-0064")) {
		t.Fatal("seek should succeed")
	}
	if it.Prev() {
		t.Fatal("prev should fail when all prior keys are out of range")
	}
}

func TestIteratorRelease(t *testing.T) {
	tree := openTree(t)
	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	it := tree.NewIterator(nil)
	if !it.Next() {
		t.Fatal("Next should succeed")
	}
	it.Release()
	if it.Valid() {
		t.Fatal("iterator should be invalid after Release")
	}
}

func TestIteratorFirstLastMultiLeaf(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 150; i++ {
		k := []byte(fmt.Sprintf("m-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	if !it.First() {
		t.Fatal("First should succeed")
	}
	if string(it.Key()) != "m-0000" {
		t.Fatalf("First: got %q", it.Key())
	}
	if !it.Last() {
		t.Fatal("Last should succeed")
	}
	if string(it.Key()) != "m-0149" {
		t.Fatalf("Last: got %q", it.Key())
	}
}
