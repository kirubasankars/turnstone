// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"testing"
)

// countGetVisibleWalk mirrors GetVisible but returns how many chain nodes were visited.
func countGetVisibleWalk(idx *Index, key []byte, snap Snapshot, myXid uint64, update bool, visible func(uint64, Snapshot) bool) (steps int, ver *indexVersion, ok bool) {
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		steps++
		if update && v.xmin == myXid {
			cp := v
			ver = &cp
			ok = true
			return false
		}
		if visible(v.xmin, snap) {
			cp := v
			ver = &cp
			ok = true
			return false
		}
		return true
	})
	return steps, ver, ok
}

func testVisible(clog map[uint64]TxStatus) func(uint64, Snapshot) bool {
	return func(xmin uint64, snap Snapshot) bool {
		if xmin >= snap.Xmax || snap.contains(xmin) {
			return false
		}
		if st, exists := clog[xmin]; exists {
			return st == TxCommitted
		}
		return true
	}
}

func putVersionChain(t *testing.T, idx *Index, key []byte, xmins ...uint64) {
	t.Helper()
	for i, xmin := range xmins {
		idx.Put(key, indexVersion{
			offset:    int64(100 * (i + 1)),
			valueLen:  3,
			xmin:      xmin,
			tombstone: false,
		})
	}
}

func TestGetVisible_StopsAtOwnWriteInOneStep(t *testing.T) {
	key := []byte("k")
	idx := NewIndex()
	defer idx.Close()

	putVersionChain(t, idx, key, 1, 2, 3)

	snap := Snapshot{Xmax: 4, Xip: map[uint64]bool{3: true}}
	visible := testVisible(nil)

	steps, ver, ok := countGetVisibleWalk(idx, key, snap, 3, true, visible)
	if !ok || ver == nil {
		t.Fatal("expected visible own write")
	}
	if ver.xmin != 3 {
		t.Fatalf("expected xmin 3, got %d", ver.xmin)
	}
	if steps != 1 {
		t.Fatalf("expected 1 walk step for read-your-own-writes, got %d", steps)
	}
}

func TestGetVisible_StopsAtHeadCommittedInOneStep(t *testing.T) {
	key := []byte("k")
	idx := NewIndex()
	defer idx.Close()

	putVersionChain(t, idx, key, 1, 2, 3)

	snap := Snapshot{Xmax: 4, Xip: map[uint64]bool{}}
	visible := testVisible(nil)

	steps, ver, ok := countGetVisibleWalk(idx, key, snap, 0, false, visible)
	if !ok || ver == nil {
		t.Fatal("expected visible committed head")
	}
	if ver.xmin != 3 {
		t.Fatalf("expected newest committed xmin 3, got %d", ver.xmin)
	}
	if steps != 1 {
		t.Fatalf("expected 1 walk step when head is visible, got %d", steps)
	}
}

func TestGetVisible_SkipsInProgressHead(t *testing.T) {
	key := []byte("k")
	idx := NewIndex()
	defer idx.Close()

	putVersionChain(t, idx, key, 1, 2, 3)

	// Tx 3 was in progress when the snapshot was taken.
	snap := Snapshot{Xmax: 4, Xip: map[uint64]bool{3: true}}
	visible := testVisible(nil)

	steps, ver, ok := countGetVisibleWalk(idx, key, snap, 0, false, visible)
	if !ok || ver == nil {
		t.Fatal("expected older visible version")
	}
	if ver.xmin != 2 {
		t.Fatalf("expected visible xmin 2 after skipping in-progress head, got %d", ver.xmin)
	}
	if steps != 2 {
		t.Fatalf("expected 2 walk steps (skip head, take next), got %d", steps)
	}
}

func TestGetVisible_SkipsVersionsAtOrAfterSnapshotBoundary(t *testing.T) {
	key := []byte("k")
	idx := NewIndex()
	defer idx.Close()

	putVersionChain(t, idx, key, 1, 2, 3, 4)

	// Snapshot cutoff xmax=3 hides xids 3 and 4 even if committed.
	snap := Snapshot{Xmax: 3, Xip: map[uint64]bool{}}
	visible := testVisible(nil)

	steps, ver, ok := countGetVisibleWalk(idx, key, snap, 0, false, visible)
	if !ok || ver == nil {
		t.Fatal("expected older visible version")
	}
	if ver.xmin != 2 {
		t.Fatalf("expected visible xmin 2, got %d", ver.xmin)
	}
	if steps != 3 {
		t.Fatalf("expected 3 walk steps (skip xids 4 and 3), got %d", steps)
	}
}

func TestGetVisible_SkipsAbortedVersionsViaDropXid(t *testing.T) {
	key := []byte("k")
	idx := NewIndex()
	defer idx.Close()

	putVersionChain(t, idx, key, 1, 2, 3)
	idx.DropXid(3)

	snap := Snapshot{Xmax: 4, Xip: map[uint64]bool{}}
	visible := testVisible(map[uint64]TxStatus{3: TxAborted})

	steps, ver, ok := countGetVisibleWalk(idx, key, snap, 0, false, visible)
	if !ok || ver == nil {
		t.Fatal("expected visible version after aborted head removed")
	}
	if ver.xmin != 2 {
		t.Fatalf("expected visible xmin 2 after DropXid(3), got %d", ver.xmin)
	}
	if steps != 1 {
		t.Fatalf("expected 1 walk step once aborted head is gone, got %d", steps)
	}

	var chainLen int
	idx.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) {
			chainLen = len(chain)
		}
	})
	if chainLen != 2 {
		t.Fatalf("expected 2 versions remaining in index after DropXid(3), got %d", chainLen)
	}
}

func TestGetVisible_DoesNotScanEntireChainWhenMatchFoundEarly(t *testing.T) {
	key := []byte("k")
	idx := NewIndex()
	defer idx.Close()

	putVersionChain(t, idx, key, 1, 2, 3, 4, 5)

	snap := Snapshot{Xmax: 10, Xip: map[uint64]bool{}}
	visible := testVisible(nil)

	steps, ver, ok := countGetVisibleWalk(idx, key, snap, 0, false, visible)
	if !ok || ver == nil {
		t.Fatal("expected visible version")
	}
	if ver.xmin != 5 {
		t.Fatalf("expected head xmin 5, got %d", ver.xmin)
	}
	if steps != 1 {
		t.Fatalf("expected early stop at head (1 step), not full chain scan; got %d steps", steps)
	}

	var chainLen int
	idx.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) {
			chainLen = len(chain)
		}
	})
	if chainLen != 5 {
		t.Fatalf("expected 5 versions stored in index, got %d", chainLen)
	}
	if steps == chainLen {
		t.Fatal("walk scanned entire chain despite visible head")
	}
}

func TestVisibility_ReadOwnUncommittedWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("mine")); err != nil {
		t.Fatal(err)
	}

	val, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get own uncommitted write: %v", err)
	}
	if string(val) != "mine" {
		t.Fatalf("expected mine, got %q", val)
	}
	tx.Discard()
}

func TestVisibility_SnapshotHidesInProgressWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	setup := db.NewTransaction(true)
	if err := setup.Put([]byte("k"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	txA := db.NewTransaction(true)
	if err := txA.Put([]byte("k"), []byte("v1-uncommitted")); err != nil {
		t.Fatal(err)
	}

	txB := db.NewTransaction(false)
	val, err := txB.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get under snapshot while other tx in progress: %v", err)
	}
	if string(val) != "v0" {
		t.Fatalf("expected snapshot value v0, got %q", val)
	}

	txB.Discard()
	txA.Discard()
}

func TestVisibility_SnapshotFrozenAfterConcurrentCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	setup := db.NewTransaction(true)
	if err := setup.Put([]byte("k"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	txB := db.NewTransaction(false)

	txA := db.NewTransaction(true)
	if err := txA.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := txA.Commit(); err != nil {
		t.Fatal(err)
	}

	val, err := txB.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get with frozen snapshot: %v", err)
	}
	if string(val) != "v0" {
		t.Fatalf("expected frozen snapshot value v0 after concurrent commit, got %q", val)
	}

	txB.Discard()

	rtx := db.NewTransaction(false)
	val, err = rtx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after new snapshot: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("expected new reader to see v1, got %q", val)
	}
	rtx.Discard()
}

func TestVisibility_AbortedWriteNotVisibleToReaders(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	setup := db.NewTransaction(true)
	if err := setup.Put([]byte("k"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	txA := db.NewTransaction(true)
	if err := txA.Put([]byte("k"), []byte("v-abort")); err != nil {
		t.Fatal(err)
	}
	txA.Discard()

	rtx := db.NewTransaction(false)
	val, err := rtx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after abort: %v", err)
	}
	if string(val) != "v0" {
		t.Fatalf("expected v0 after aborted overwrite, got %q", val)
	}
	rtx.Discard()

	var chainLen int
	db.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == "k" {
			chainLen = len(chain)
		}
	})
	if chainLen != 1 {
		t.Fatalf("expected aborted version removed from index chain, got %d versions", chainLen)
	}
}
