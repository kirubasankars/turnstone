// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetLocked(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	tree.mu.RLock()
	got, err := tree.GetLocked([]byte("k"))
	tree.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v" {
		t.Fatalf("GetLocked: got %q", got)
	}
}

func TestLeftmostLeafFallback(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("lf-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	tree.setLeftLeaf(0)
	leaf := tree.leftmostLeaf()
	if leaf == 0 {
		t.Fatal("leftmostLeaf should not be zero")
	}
	if tree.pageType(leaf) != pageTypeLeaf {
		t.Fatalf("expected leaf page, got type %d", tree.pageType(leaf))
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	if !it.First() {
		t.Fatal("First should succeed with cleared leftLeaf meta")
	}
	if string(it.Key()) != "lf-0000" {
		t.Fatalf("First key: got %q", it.Key())
	}
}

func TestLeafEntryOutOfRange(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	root := tree.rootPage()
	if _, _, err := tree.leafEntry(root, -1); err == nil {
		t.Fatal("expected error for idx -1")
	}
	if _, _, err := tree.leafEntry(root, 99); err == nil {
		t.Fatal("expected error for idx 99")
	}
}

func TestLeafUsedBytesEmptyLeaf(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	leaf, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(leaf, 0, 0)
	if got := tree.leafUsedBytes(leaf); got != leafHeaderSize {
		t.Fatalf("empty leaf used bytes: got %d want %d", got, leafHeaderSize)
	}
}

func TestCompareKeysAndCloneBytes(t *testing.T) {
	if compareKeys([]byte("a"), []byte("ab")) >= 0 {
		t.Fatal("prefix key order wrong")
	}
	if compareKeys([]byte("ab"), []byte("a")) <= 0 {
		t.Fatal("longer key order wrong")
	}
	if compareKeys([]byte("a"), []byte("a")) != 0 {
		t.Fatal("equal keys should compare 0")
	}
	if cloneBytes(nil) != nil {
		t.Fatal("cloneBytes(nil) should be nil")
	}
	cl := cloneBytes([]byte("x"))
	cl[0] = 'y'
	if string(cl) == "x" {
		t.Fatal("clone should copy")
	}
}

func TestReadU64ShortBuffer(t *testing.T) {
	if readU64([]byte{1, 2, 3}, 0) != 0 {
		t.Fatal("short buffer readU64 should return 0")
	}
}

func TestPageOutOfBounds(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if p := tree.mf.page(tree.numPages() + 1000); p != nil {
		t.Fatal("page past end should be nil")
	}
}

func TestRemoveLeafLeftLeafPrevBranch(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Create three linked leaves manually to exercise removeLeaf branches.
	l1, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l3, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(l1, 0, l2)
	tree.rewriteLeaf(l1, [][2][]byte{{[]byte("a"), []byte("1")}})
	tree.initLeaf(l2, l1, l3)
	tree.rewriteLeaf(l2, [][2][]byte{{[]byte("b"), []byte("2")}})
	tree.initLeaf(l3, l2, 0)
	tree.rewriteLeaf(l3, [][2][]byte{{[]byte("c"), []byte("3")}})
	tree.setRoot(l1)
	tree.setLeftLeaf(l1)

	// Remove middle leaf; leftLeaf meta still points at l1.
	if err := tree.removeLeaf(l2); err != nil {
		t.Fatal(err)
	}
	if tree.leafNext(l1) != l3 {
		t.Fatalf("l1.next: got %d want %d", tree.leafNext(l1), l3)
	}
	if tree.leafPrev(l3) != l1 {
		t.Fatalf("l3.prev: got %d want %d", tree.leafPrev(l3), l1)
	}

	// Remove rightmost leaf while leftLeaf still l1.
	if err := tree.removeLeaf(l3); err != nil {
		t.Fatal(err)
	}
	if tree.leafNext(l1) != 0 {
		t.Fatalf("l1.next after l3 remove: got %d", tree.leafNext(l1))
	}

	// Remove leftmost leaf; exercises setLeftLeaf(prev) branch.
	if err := tree.removeLeaf(l1); err != nil {
		t.Fatal(err)
	}
	if tree.leftLeaf() != 0 {
		t.Fatalf("leftLeaf after removing all: got %d", tree.leftLeaf())
	}
}

func TestSplitInternalNotInserted(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	left, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	right, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	parent, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	otherChild, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}

	tree.initInternal(left)
	tree.internalSet(left, []internalEntry{{child: otherChild}})
	tree.initInternal(right)
	tree.internalSet(right, []internalEntry{{child: otherChild}})
	tree.initInternal(parent)
	// Parent does not reference left page; splitInternal should append promote entry.
	tree.internalSet(parent, []internalEntry{{child: otherChild}})

	entries := tree.internalEntries(left)
	entries = append(entries, internalEntry{key: []byte("sep"), child: right})
	if err := tree.splitInternal([]pathEntry{{page: parent, child: 0}}, left, entries); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveLeftmostLeafSetsNext(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	l1, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(l1, 0, l2)
	tree.rewriteLeaf(l1, [][2][]byte{{[]byte("first"), []byte("1")}})
	tree.initLeaf(l2, l1, 0)
	tree.rewriteLeaf(l2, [][2][]byte{{[]byte("second"), []byte("2")}})
	tree.setRoot(l1)
	tree.setLeftLeaf(l1)

	if err := tree.removeLeaf(l1); err != nil {
		t.Fatal(err)
	}
	if tree.leftLeaf() != l2 {
		t.Fatalf("leftLeaf: got %d want %d", tree.leftLeaf(), l2)
	}
}

func TestRemoveRightmostLeafSetsPrev(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	l1, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(l1, 0, l2)
	tree.rewriteLeaf(l1, [][2][]byte{{[]byte("first"), []byte("1")}})
	tree.initLeaf(l2, l1, 0)
	tree.rewriteLeaf(l2, [][2][]byte{{[]byte("second"), []byte("2")}})
	tree.setRoot(l1)
	tree.setLeftLeaf(l1)

	if err := tree.removeLeaf(l2); err != nil {
		t.Fatal(err)
	}
	if tree.leftLeaf() != l1 {
		t.Fatalf("leftLeaf: got %d want %d", tree.leftLeaf(), l1)
	}
}

func TestLeftmostLeafEmptyAndDescent(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if got := tree.leftmostLeaf(); got != 0 {
		t.Fatalf("empty tree leftmost: got %d", got)
	}

	for i := 0; i < 300; i++ {
		k := []byte(fmt.Sprintf("desc-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	tree.setLeftLeaf(0)
	if got := tree.leftmostLeaf(); got == 0 {
		t.Fatal("expected leftmost leaf via descent")
	}
}

func TestSplitInternalNewRoot(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	sepKey := []byte(strings.Repeat("r", 200))
	makeLeaf := func(k []byte) uint64 {
		t.Helper()
		p, err := tree.allocPage()
		if err != nil {
			t.Fatal(err)
		}
		tree.initLeaf(p, 0, 0)
		tree.rewriteLeaf(p, [][2][]byte{{k, []byte("v")}})
		return p
	}

	root, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initInternal(root)
	entries := []internalEntry{{child: makeLeaf([]byte("r0"))}}
	for i := 0; i < 19; i++ {
		k := append([]byte(fmt.Sprintf("r%02d-", i)), sepKey...)
		entries = append(entries, internalEntry{key: k, child: makeLeaf(k)})
	}
	tree.setRoot(root)

	if err := tree.splitInternal(nil, root, entries); err != nil {
		t.Fatal(err)
	}
	if tree.pageType(tree.rootPage()) != pageTypeInternal {
		t.Fatal("expected internal root after split")
	}
}

func TestIteratorKeyValueAndAdvanceErrors(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	it := tree.NewIterator(nil)
	defer it.Release()
	it.valid = true
	it.leaf = tree.rootPage()
	it.idx = 999

	if got := it.Key(); got != nil {
		t.Fatalf("Key with bad idx: %v", got)
	}
	if it.Error() == nil {
		t.Fatal("expected Key error")
	}

	it.err = nil
	it.valid = true
	if got := it.Value(); got != nil {
		t.Fatalf("Value with bad idx: %v", got)
	}
	if it.Error() == nil {
		t.Fatal("expected Value error")
	}

	it.err = nil
	it.valid = true
	it.leaf = tree.rootPage()
	it.idx = 999
	if it.Prev() {
		t.Fatal("Prev with bad idx should fail")
	}
	if it.Error() == nil {
		t.Fatal("expected Prev error")
	}

	it.err = nil
	it.valid = true
	it.leaf = 0
	it.idx = 0
	if it.advanceToValid() {
		t.Fatal("advanceToValid with leaf 0 should fail")
	}
}

func TestOpenMmapFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "data.bt"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir)
	if err == nil {
		t.Fatal("expected open failure when data.bt is a directory")
	}
}

func TestGrowFailure(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	path := filepath.Join(dir, "data.bt")
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := tree.mf.grow(tree.numPages() + 1000); err == nil {
		t.Fatal("expected grow failure on read-only file")
	}
	_ = os.Chmod(path, 0o644)
}

func TestRemoveLeafLeftLeafUsesPrev(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	l1, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l3, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(l1, 0, l2)
	tree.rewriteLeaf(l1, [][2][]byte{{[]byte("a"), []byte("1")}})
	tree.initLeaf(l2, l1, l3)
	tree.rewriteLeaf(l2, [][2][]byte{{[]byte("b"), []byte("2")}})
	tree.initLeaf(l3, l2, 0)
	tree.rewriteLeaf(l3, [][2][]byte{{[]byte("c"), []byte("3")}})
	tree.setRoot(l1)
	tree.setLeftLeaf(l3)

	if err := tree.removeLeaf(l3); err != nil {
		t.Fatal(err)
	}
	if tree.leftLeaf() != l2 {
		t.Fatalf("leftLeaf: got %d want %d", tree.leftLeaf(), l2)
	}
}

func TestPutFailsWhenGrowBlocked(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	path := filepath.Join(dir, "data.bt")
	tree.setFreeHead(0)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)

	if err := tree.Put([]byte("overflow"), []byte("x")); err == nil {
		t.Fatal("expected Put failure when grow blocked")
	}
}

func TestApplyBatchErrorPaths(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	path := filepath.Join(dir, "data.bt")
	tree.setFreeHead(0)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)

	puts := [][2][]byte{{[]byte("batch-fail"), []byte("x")}}
	if err := tree.ApplyBatch(puts, nil, false); err == nil {
		t.Fatal("expected ApplyBatch put failure")
	}
}

func TestLeafEntrySizeCorruptIntermediate(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	leaf := tree.rootPage()
	p := tree.mf.page(leaf)
	writeU32(p, leafHeaderSize, uint32(9999))
	writeU32(p, leafHeaderSize+4, 1)

	if _, _, err := tree.leafEntry(leaf, 1); err == nil {
		t.Fatal("expected corrupt leaf entry error")
	}
}

func TestLeafEntrySizeTruncatedHeader(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	leaf, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(leaf, 0, 0)
	p := tree.mf.page(leaf)
	off := leafHeaderSize
	writeU32(p, off, uint32(pageSize-leafHeaderSize-8-4))
	writeU32(p, off+4, 0)
	tree.setLeafCount(leaf, 2)

	if got := tree.leafEntrySize(leaf, 2); got != pageSize-4 {
		t.Fatalf("leafEntrySize: got %d want %d", got, pageSize-4)
	}
}

func TestIteratorLastPrevLeafError(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	l1, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	l2, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initLeaf(l1, 0, l2)
	tree.rewriteLeaf(l1, [][2][]byte{{[]byte("left"), []byte("1")}})
	tree.initLeaf(l2, l1, 0)
	tree.rewriteLeaf(l2, [][2][]byte{{[]byte("right"), []byte("2")}})
	tree.setRoot(l2)
	tree.setLeftLeaf(l1)

	off := tree.leafEntrySize(l1, 0)
	p := tree.mf.page(l1)
	writeU32(p, off, uint32(9999))
	writeU32(p, off+4, 1)

	it := tree.NewIterator(&Range{Start: []byte("z")})
	defer it.Release()
	if it.Last() {
		t.Fatal("Last should fail on corrupt previous leaf")
	}
	if it.Error() == nil {
		t.Fatal("expected Last error")
	}
}

func TestIteratorLastAndAdvanceErrors(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if err := tree.Put([]byte("solo"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("lasterr-%04d", i))
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	corruptEntry := func(page uint64, idx int) {
		t.Helper()
		off := tree.leafEntrySize(page, idx)
		p := tree.mf.page(page)
		writeU32(p, off, uint32(9999))
		writeU32(p, off+4, 1)
	}

	it := tree.NewIterator(&Range{Start: []byte("zzzz")})
	defer it.Release()
	if it.Last() {
		t.Fatal("Last should fail when no keys match range")
	}

	it2 := tree.NewIterator(nil)
	right := tree.rightmostLeaf()
	corruptEntry(right, tree.leafCount(right)-1)
	if it2.Last() {
		t.Fatal("Last with corrupt entry should fail")
	}
	if it2.Error() == nil {
		t.Fatal("expected Last error on corrupt entry")
	}
	it2.Release()

	it3 := tree.NewIterator(nil)
	defer it3.Release()
	if !it3.Seek([]byte("lasterr-0000")) {
		t.Fatal("seek should succeed")
	}
	corruptEntry(it3.leaf, it3.idx)
	if it3.advanceToValid() {
		t.Fatal("advanceToValid should fail on corrupt entry")
	}
	if it3.Error() == nil {
		t.Fatal("expected advanceToValid error on corrupt entry")
	}

	left := tree.leftmostLeaf()
	it4 := tree.NewIterator(&Range{Start: []byte("zzzz")})
	defer it4.Release()
	corruptEntry(left, 0)
	if it4.Last() {
		t.Fatal("Last should fail when walking corrupt previous leaf")
	}
	if it4.Error() == nil {
		t.Fatal("expected Last error on corrupt previous leaf")
	}
}

func TestAllocPageGrowError(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	path := filepath.Join(dir, "data.bt")
	tree.setFreeHead(0)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)

	if _, err := tree.allocPage(); err == nil {
		t.Fatal("expected allocPage grow failure")
	}
}

func TestSplitLeafAndInsertAllocFailure(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 128; i++ {
		k := []byte{byte(i + 1)}
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(dir, "data.bt")
	tree.setFreeHead(0)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)

	if err := tree.Put([]byte{0xff}, []byte("x")); err == nil {
		t.Fatal("expected splitLeaf alloc failure")
	}
}

func TestSplitInternalAllocFailure(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	left, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initInternal(left)
	entries := []internalEntry{{child: left}, {key: []byte("sep"), child: left}}

	path := filepath.Join(dir, "data.bt")
	tree.setFreeHead(0)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)

	if err := tree.splitInternal(nil, left, entries); err == nil {
		t.Fatal("expected splitInternal alloc failure")
	}
}

func TestInsertInParentRootAllocFailure(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 128; i++ {
		k := []byte{byte(i + 1)}
		if err := tree.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	freePage, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.setFreeHead(freePage)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))

	path := filepath.Join(dir, "data.bt")
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}

	if err := tree.Put([]byte{0xff}, []byte("x")); err == nil {
		t.Fatal("expected insertInParent alloc failure")
	}
}

func TestSplitInternalSecondAllocFailure(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	left, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initInternal(left)
	entries := []internalEntry{{child: left}, {key: []byte("sep"), child: left}}

	freePage, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.setFreeHead(freePage)
	tree.setNumPages(uint64(len(tree.mf.data) / pageSize))

	path := filepath.Join(dir, "data.bt")
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}

	if err := tree.splitInternal(nil, left, entries); err == nil {
		t.Fatal("expected splitInternal second alloc failure")
	}
}

func TestSplitInternalRecursiveParent(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	sepKey := []byte(strings.Repeat("s", 200))
	makeLeaf := func(k []byte) uint64 {
		t.Helper()
		p, err := tree.allocPage()
		if err != nil {
			t.Fatal(err)
		}
		tree.initLeaf(p, 0, 0)
		tree.rewriteLeaf(p, [][2][]byte{{k, []byte("v")}})
		return p
	}

	// Parent holds the maximum separators that still fit on one page.
	parent, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initInternal(parent)
	parentEntries := []internalEntry{{child: makeLeaf([]byte("base"))}}
	for i := 0; i < 18; i++ {
		k := append([]byte(fmt.Sprintf("%02d-", i)), sepKey...)
		parentEntries = append(parentEntries, internalEntry{key: k, child: makeLeaf(k)})
	}
	if !tree.internalFits(parentEntries) {
		t.Fatal("parent entries should fit")
	}
	tree.internalSet(parent, parentEntries)

	// Left internal node under parent, filled enough to require a split.
	left, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initInternal(left)
	leftEntries := []internalEntry{{child: makeLeaf([]byte("l0"))}}
	for i := 0; i < 19; i++ {
		k := append([]byte(fmt.Sprintf("l%02d-", i)), sepKey...)
		leftEntries = append(leftEntries, internalEntry{key: k, child: makeLeaf(k)})
	}
	if tree.internalFits(leftEntries) {
		t.Fatal("left entries should not fit without split")
	}

	grand, err := tree.allocPage()
	if err != nil {
		t.Fatal(err)
	}
	tree.initInternal(grand)
	tree.internalSet(grand, []internalEntry{{child: parent}})
	tree.setRoot(grand)

	// Replace one parent child with left so the recursive insert targets a full parent.
	parentEntries[1].child = left
	tree.internalSet(parent, parentEntries)

	path := []pathEntry{{page: grand, child: 0}, {page: parent, child: 1}}
	if err := tree.splitInternal(path, left, leftEntries); err != nil {
		t.Fatal(err)
	}
}
