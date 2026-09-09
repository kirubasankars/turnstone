// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree_test

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"turnstone/stonedb/btree"
)

func TestGetNotFound(t *testing.T) {
	tree := openTree(t)

	_, err := tree.Get([]byte("missing"))
	if !errors.Is(err, btree.ErrNotFound) {
		t.Fatalf("empty tree get: got %v want ErrNotFound", err)
	}

	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	_, err = tree.Get([]byte("b"))
	if !errors.Is(err, btree.ErrNotFound) {
		t.Fatalf("missing key get: got %v want ErrNotFound", err)
	}
}

func TestClosedTreeOperations(t *testing.T) {
	tree := openTree(t)
	if err := tree.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}

	if err := tree.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}

	if _, err := tree.Get([]byte("k")); !errors.Is(err, btree.ErrClosed) {
		t.Fatalf("Get after close: %v", err)
	}
	if err := tree.Put([]byte("k"), []byte("v")); !errors.Is(err, btree.ErrClosed) {
		t.Fatalf("Put after close: %v", err)
	}
	if err := tree.Delete([]byte("k")); !errors.Is(err, btree.ErrClosed) {
		t.Fatalf("Delete after close: %v", err)
	}
	if err := tree.Sync(); !errors.Is(err, btree.ErrClosed) {
		t.Fatalf("Sync after close: %v", err)
	}
	puts := [][2][]byte{{[]byte("a"), []byte("1")}}
	if err := tree.ApplyBatch(puts, nil, true); !errors.Is(err, btree.ErrClosed) {
		t.Fatalf("ApplyBatch after close: %v", err)
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	if it.First() {
		t.Fatal("First on closed tree should be false")
	}
	if it.Seek([]byte("k")) {
		t.Fatal("Seek on closed tree should be false")
	}
	if it.Last() {
		t.Fatal("Last on closed tree should be false")
	}
	if !errors.Is(it.Error(), btree.ErrClosed) {
		t.Fatalf("iterator error: %v", it.Error())
	}
}

func TestPutOverwrite(t *testing.T) {
	tree := openTree(t)
	key := []byte("same")
	if err := tree.Put(key, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put(key, []byte("new")); err != nil {
		t.Fatal(err)
	}
	got, err := tree.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("overwrite: got %q want new", got)
	}
}

func TestKeyOrderingEdgeCases(t *testing.T) {
	tree := openTree(t)
	cases := [][2][]byte{
		{[]byte{}, []byte("empty-key")},
		{[]byte("a"), []byte("1")},
		{[]byte("ab"), []byte("2")},
		{[]byte{0x00}, []byte("null")},
		{[]byte("prefix"), []byte("p")},
		{[]byte("prefix\x00"), []byte("ps")},
	}
	for _, kv := range cases {
		if err := tree.Put(kv[0], kv[1]); err != nil {
			t.Fatalf("put %q: %v", kv[0], err)
		}
	}

	for _, kv := range cases {
		got, err := tree.Get(kv[0])
		if err != nil {
			t.Fatalf("get %q: %v", kv[0], err)
		}
		if string(got) != string(kv[1]) {
			t.Fatalf("get %q: got %q want %q", kv[0], got, kv[1])
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	prev := ""
	count := 0
	for it.Next() {
		count++
		cur := string(it.Key())
		if prev > cur {
			t.Fatalf("order violation: %q after %q", cur, prev)
		}
		prev = cur
	}
	if count != len(cases) {
		t.Fatalf("iterator count: got %d want %d", count, len(cases))
	}
}

func TestApplyBatchMixedNoSync(t *testing.T) {
	tree := openTree(t)
	if err := tree.Put([]byte("keep"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("drop"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	puts := [][2][]byte{{[]byte("add"), []byte("3")}}
	deletes := [][]byte{[]byte("drop")}
	if err := tree.ApplyBatch(puts, deletes, false); err != nil {
		t.Fatal(err)
	}

	if _, err := tree.Get([]byte("drop")); !errors.Is(err, btree.ErrNotFound) {
		t.Fatalf("deleted key still present: %v", err)
	}
	for _, kv := range [][2]string{{"keep", "1"}, {"add", "3"}} {
		got, err := tree.Get([]byte(kv[0]))
		if err != nil {
			t.Fatalf("get %s: %v", kv[0], err)
		}
		if string(got) != kv[1] {
			t.Fatalf("get %s: got %q want %q", kv[0], got, kv[1])
		}
	}
}

func TestDeleteNoOps(t *testing.T) {
	tree := openTree(t)
	if err := tree.Delete([]byte("missing")); err != nil {
		t.Fatalf("delete on empty tree: %v", err)
	}
	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Delete([]byte("missing")); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
	got, err := tree.Get([]byte("a"))
	if err != nil || string(got) != "1" {
		t.Fatalf("key should remain: %v %q", err, got)
	}
}

func TestDeleteAllKeysFreelistReuse(t *testing.T) {
	tree := openTree(t)
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i++ {
		if err := tree.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tree.Get([]byte("k000")); !errors.Is(err, btree.ErrNotFound) {
		t.Fatalf("tree should be empty: %v", err)
	}

	if err := tree.Put([]byte("after-empty"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	got, err := tree.Get([]byte("after-empty"))
	if err != nil || string(got) != "yes" {
		t.Fatalf("reinsert after empty: %v %q", err, got)
	}
}

func TestReopenGrownTree(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("grow-%05d", i))
		v := []byte(fmt.Sprintf("val-%05d", i))
		if err := tree.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := tree.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}

	tree2, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree2.Close()

	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("grow-%05d", i))
		want := []byte(fmt.Sprintf("val-%05d", i))
		got, err := tree2.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("get %d: got %q want %q", i, got, want)
		}
	}

	it := tree2.NewIterator(nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	if count != 5000 {
		t.Fatalf("iterator count: got %d want 5000", count)
	}
}

func TestOpenGarbageFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bt")
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, err := tree.Get([]byte("k"))
	if err != nil || string(got) != "v" {
		t.Fatalf("get after garbage open: %v %q", err, got)
	}
}

func TestOpenMkdirFailure(t *testing.T) {
	parent := t.TempDir()
	blocker := filepath.Join(parent, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := btree.Open(filepath.Join(blocker, "nested"))
	if err == nil {
		t.Fatal("expected mkdir failure")
	}
}

func TestSyncAfterUnlink(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data.bt")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := tree.Sync(); err == nil {
		t.Fatal("expected sync failure after unlink")
	}
	_ = tree.Close()
}

func TestMapBackedRoundTrip(t *testing.T) {
	tree := openTree(t)
	want := make(map[string]string)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 500; i++ {
		op := rng.Intn(3)
		key := fmt.Sprintf("key-%04d", rng.Intn(200))
		switch op {
		case 0, 1:
			val := fmt.Sprintf("val-%d", i)
			want[key] = val
			if err := tree.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
		case 2:
			if _, ok := want[key]; ok {
				delete(want, key)
				if err := tree.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	for k, v := range want {
		got, err := tree.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("get %s: got %q want %q", k, got, v)
		}
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if _, ok := want[key]; ok {
			continue
		}
		if _, err := tree.Get([]byte(key)); !errors.Is(err, btree.ErrNotFound) {
			t.Fatalf("unexpected key %s: %v", key, err)
		}
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	seen := make(map[string]string)
	prev := ""
	for it.Next() {
		k := string(it.Key())
		v := string(it.Value())
		if prev > k {
			t.Fatalf("order violation: %q after %q", k, prev)
		}
		prev = k
		if _, ok := seen[k]; ok {
			t.Fatalf("duplicate key %s", k)
		}
		seen[k] = v
	}
	if len(seen) != len(want) {
		t.Fatalf("iterator keys: got %d want %d", len(seen), len(want))
	}
	for k, v := range want {
		if seen[k] != v {
			t.Fatalf("iterator value for %s: got %q want %q", k, seen[k], v)
		}
	}
}
