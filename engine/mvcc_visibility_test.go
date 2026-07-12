// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"testing"
)

func TestIsVisible_Table(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	setup := db.NewTransaction(true)
	if err := setup.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}
	committedXid := setup.xid

	inProgress := db.NewTransaction(true)
	if err := inProgress.Put([]byte("k2"), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	inProgressXid := inProgress.xid

	tests := []struct {
		name string
		xmin uint64
		snap Snapshot
		want bool
	}{
		{
			name: "committed before snapshot xmax",
			xmin: committedXid,
			snap: Snapshot{Xmax: committedXid + 10, Xip: map[uint64]bool{}},
			want: true,
		},
		{
			name: "at snapshot xmax boundary",
			xmin: committedXid,
			snap: Snapshot{Xmax: committedXid, Xip: map[uint64]bool{}},
			want: false,
		},
		{
			name: "in progress xid listed in snapshot xip",
			xmin: inProgressXid,
			snap: Snapshot{Xmax: inProgressXid + 5, Xip: map[uint64]bool{inProgressXid: true}},
			want: false,
		},
		{
			name: "active in progress xid without clog entry",
			xmin: inProgressXid,
			snap: Snapshot{Xmax: inProgressXid + 5, Xip: map[uint64]bool{}},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := db.isVisible(tc.xmin, tc.snap); got != tc.want {
				t.Fatalf("isVisible(%d, snap)=%v want %v", tc.xmin, got, tc.want)
			}
		})
	}

	inProgress.Discard()
}

func TestVisibleVersionForKey_Table(t *testing.T) {
	chain543 := []indexVersion{
		{offset: 500, xmin: 5},
		{offset: 400, xmin: 4},
		{offset: 300, xmin: 3},
	}
	visible := testVisible(nil)

	tests := []struct {
		name   string
		chain  []indexVersion
		snap   Snapshot
		myXid  uint64
		update bool
		wantOK bool
		wantX  uint64
	}{
		{
			name:   "read sees newest committed head",
			chain:  chain543,
			snap:   Snapshot{Xmax: 10, Xip: map[uint64]bool{}},
			wantOK: true,
			wantX:  5,
		},
		{
			name:   "writer sees own uncommitted head first",
			chain:  chain543,
			snap:   Snapshot{Xmax: 10, Xip: map[uint64]bool{5: true}},
			myXid:  5,
			update: true,
			wantOK: true,
			wantX:  5,
		},
		{
			name:   "reader skips in progress head",
			chain:  chain543,
			snap:   Snapshot{Xmax: 10, Xip: map[uint64]bool{5: true}},
			wantOK: true,
			wantX:  4,
		},
		{
			name:   "snapshot cutoff hides newer commits",
			chain:  chain543,
			snap:   Snapshot{Xmax: 4, Xip: map[uint64]bool{}},
			wantOK: true,
			wantX:  3,
		},
		{
			name:  "no visible version",
			chain: []indexVersion{{offset: 100, xmin: 9}},
			snap:  Snapshot{Xmax: 5, Xip: map[uint64]bool{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ver, ok := visibleVersionForKey(tc.chain, tc.snap, tc.myXid, tc.update, visible)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want=%v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if ver == nil || ver.xmin != tc.wantX {
				t.Fatalf("got xmin=%v want %d", ver, tc.wantX)
			}
		})
	}
}

func TestHasNewerCommitted_Table(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	key := []byte("k")
	putVersionChain(t, idx, key, 1, 2, 3, 4)

	clogAllCommitted := func(uint64) TxStatus { return TxCommitted }

	tests := []struct {
		name       string
		excludeXid uint64
		snap       Snapshot
		want       bool
	}{
		{
			name:       "no newer committed after snapshot",
			excludeXid: 4,
			snap:       Snapshot{Xmax: 10, Xip: map[uint64]bool{}},
			want:       false,
		},
		{
			name:       "newer committed at or after snapshot xmax",
			excludeXid: 4,
			snap:       Snapshot{Xmax: 3, Xip: map[uint64]bool{}},
			want:       true,
		},
		{
			name:       "committed head in snapshot xip counts as newer",
			excludeXid: 4,
			snap:       Snapshot{Xmax: 10, Xip: map[uint64]bool{3: true}},
			want:       true,
		},
		{
			name:       "own xid excluded from walk",
			excludeXid: 4,
			snap:       Snapshot{Xmax: 10, Xip: map[uint64]bool{4: true}},
			want:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := idx.HasNewerCommitted(key, tc.excludeXid, tc.snap, clogAllCommitted)
			if got != tc.want {
				t.Fatalf("HasNewerCommitted=%v want %v", got, tc.want)
			}
		})
	}
}

func TestHasNewerCommitted_SkipsNonCommittedHead(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	key := []byte("k")
	putVersionChain(t, idx, key, 1, 2)

	clog := func(xid uint64) TxStatus {
		if xid == 2 {
			return TxInProgress
		}
		return TxCommitted
	}

	snap := Snapshot{Xmax: 10, Xip: map[uint64]bool{2: true}}
	if idx.HasNewerCommitted(key, 0, snap, clog) {
		t.Fatal("expected no newer committed when head is in progress")
	}
}

func TestLatestResolved_Table(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	key := []byte("k")
	putVersionChain(t, idx, key, 1, 2, 3)

	clog := func(xid uint64) TxStatus {
		if xid == 3 {
			return TxAborted
		}
		if xid == 2 {
			return TxInProgress
		}
		return TxCommitted
	}

	ver, xmin, ok := idx.LatestResolved(key, 0, clog)
	if !ok || ver == nil {
		t.Fatal("expected resolved version")
	}
	if xmin != 2 {
		t.Fatalf("expected newest non-aborted xmin 2, got %d", xmin)
	}

	ver, xmin, ok = idx.LatestResolved(key, 2, clog)
	if !ok || ver == nil {
		t.Fatal("expected resolved version when excluding in-progress head")
	}
	if xmin != 1 {
		t.Fatalf("expected xmin 1 after excluding 2, got %d", xmin)
	}
}

func TestLiveKeyCount_IgnoresTombstoneHead(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	idx.Put([]byte("live"), indexVersion{offset: 10, xmin: 1, tombstone: false})
	idx.Put([]byte("deleted"), indexVersion{offset: 30, xmin: 3, tombstone: true})

	clogCommitted := func(uint64) TxStatus { return TxCommitted }
	if n := idx.LiveKeyCount(clogCommitted); n != 1 {
		t.Fatalf("expected 1 live key, got %d", n)
	}
}

func TestGetVisible_ReturnsTombstoneVersion(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	key := []byte("k")
	idx.Put(key, indexVersion{offset: 100, xmin: 1, tombstone: false})
	idx.Put(key, indexVersion{offset: 200, xmin: 2, tombstone: true})

	snap := Snapshot{Xmax: 10, Xip: map[uint64]bool{}}
	ver, ok := idx.GetVisible(key, snap, 0, false, testVisible(nil))
	if !ok || ver == nil {
		t.Fatal("GetVisible should return tombstone version")
	}
	if !ver.tombstone || ver.xmin != 2 {
		t.Fatalf("expected tombstone xmin=2, got %+v", ver)
	}
}

func TestGetVisible_NoVisibleVersionInChain(t *testing.T) {
	idx := NewIndex()
	defer idx.Close()

	key := []byte("k")
	putVersionChain(t, idx, key, 8, 9)

	snap := Snapshot{Xmax: 5, Xip: map[uint64]bool{}}
	_, ok := idx.GetVisible(key, snap, 0, false, testVisible(nil))
	if ok {
		t.Fatal("expected no visible version when all xids are after snapshot cutoff")
	}
}

func TestVisibility_DeleteSnapshotIsolation(t *testing.T) {
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

	txSnap := db.NewTransaction(false)

	txDel := db.NewTransaction(true)
	if err := txDel.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := txDel.Commit(); err != nil {
		t.Fatal(err)
	}

	val, err := txSnap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("frozen snapshot read after delete commit: %v", err)
	}
	if string(val) != "v0" {
		t.Fatalf("expected frozen v0, got %q", val)
	}
	txSnap.Discard()

	checkKeyMissing(t, db, "k")
}

func TestVisibility_OwnUncommittedDelete(t *testing.T) {
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

	tx := db.NewTransaction(true)
	if err := tx.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Get([]byte("k")); err != ErrKeyNotFound {
		t.Fatalf("writer expected ErrKeyNotFound for own delete, got %v", err)
	}

	other := db.NewTransaction(false)
	val, err := other.Get([]byte("k"))
	if err != nil {
		t.Fatalf("other reader expected v0, got err=%v", err)
	}
	if string(val) != "v0" {
		t.Fatalf("other reader expected v0, got %q", val)
	}

	tx.Discard()
	other.Discard()
}

func TestVisibility_BlindWriteConflictOnInProgressHead(t *testing.T) {
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
	if err := txA.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}

	txB := db.NewTransaction(true)
	if err := txB.Put([]byte("k"), []byte("v2")); err != ErrWriteConflict {
		t.Fatalf("expected blind write conflict on in-progress head, got %v", err)
	}

	txA.Discard()
	txB.Discard()
}

func TestVisibility_MultipleInProgressHiddenFromReader(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	setup := db.NewTransaction(true)
	if err := setup.Put([]byte("k"), []byte("base")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	txW1 := db.NewTransaction(true)
	if err := txW1.Put([]byte("k"), []byte("w1")); err != nil {
		t.Fatal(err)
	}

	txW2 := db.NewTransaction(true)
	if err := txW2.Put([]byte("other"), []byte("w2")); err != nil {
		t.Fatal(err)
	}

	txR := db.NewTransaction(false)
	val, err := txR.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "base" {
		t.Fatalf("expected base while two writers in progress, got %q", val)
	}

	txW1.Discard()
	txW2.Discard()
	txR.Discard()
}

func TestVisibility_ReopenRebuildsVisibleState(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	setup := db.NewTransaction(true)
	if err := setup.Put([]byte("k"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	abort := db.NewTransaction(true)
	if err := abort.Put([]byte("k"), []byte("gone")); err != nil {
		t.Fatal(err)
	}
	abortedXid := abort.xid
	abort.Discard()

	upd := db.NewTransaction(true)
	if err := upd.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := upd.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	checkKey(t, db2, "k", "v1")

	var xmins []uint64
	db2.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == "k" {
			for _, v := range chain {
				xmins = append(xmins, v.xmin)
			}
		}
	})
	if len(xmins) != 2 {
		t.Fatalf("expected committed version history after replay, got xmins=%v", xmins)
	}
	for _, x := range xmins {
		if x == abortedXid {
			t.Fatalf("aborted xid %d should not appear in replayed index chain", abortedXid)
		}
	}
	if xmins[0] <= xmins[1] {
		t.Fatalf("expected newest-first chain, got xmins=%v", xmins)
	}
}

func TestVisibility_CommittedDeleteVisibleOnlyAfterSnapshot(t *testing.T) {
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

	txBefore := db.NewTransaction(false)

	txDel := db.NewTransaction(true)
	if err := txDel.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := txDel.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := txBefore.Get([]byte("k")); err != nil {
		t.Fatalf("snapshot started before delete should still read value: %v", err)
	}
	txBefore.Discard()

	checkKeyMissing(t, db, "k")
}

func TestVisibility_ReadSetConflictAfterConcurrentDelete(t *testing.T) {
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

	txRead := db.NewTransaction(true)
	if _, err := txRead.Get([]byte("k")); err != nil {
		t.Fatal(err)
	}

	txDel := db.NewTransaction(true)
	if err := txDel.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := txDel.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := txRead.Put([]byte("other"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := txRead.Commit(); err != ErrWriteConflict {
		t.Fatalf("expected read-set conflict after concurrent delete, got %v", err)
	}
}
