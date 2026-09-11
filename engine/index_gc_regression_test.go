// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"testing"
)

func committedClog() func(uint64) TxStatus {
	return func(uint64) TxStatus { return TxCommitted }
}

func defaultVisible() func(uint64, Snapshot) bool {
	return func(xmin uint64, snap Snapshot) bool {
		return xmin < snap.Xmax && !snap.contains(xmin)
	}
}

func keptXmins(chain []indexVersion, mask []bool) []uint64 {
	out := make([]uint64, 0, len(chain))
	for i, v := range chain {
		if mask[i] {
			out = append(out, v.xmin)
		}
	}
	return out
}

func filteredXmins(ctx IndexGCContext, chain []indexVersion) []uint64 {
	out := ctx.FilterVersions(nil, chain)
	xs := make([]uint64, len(out))
	for i, v := range out {
		xs[i] = v.xmin
	}
	return xs
}

func TestIndexGC_keepMask_RegressionTable(t *testing.T) {
	chain54321 := []indexVersion{
		{offset: 500, xmin: 5},
		{offset: 400, xmin: 4},
		{offset: 300, xmin: 3},
		{offset: 200, xmin: 2},
		{offset: 100, xmin: 1},
	}

	tests := []struct {
		name    string
		chain   []indexVersion
		ctx     IndexGCContext
		want    []uint64
	}{
		{
			name:  "single reader sees head only",
			chain: chain54321,
			ctx: IndexGCContext{
				Readers: []IndexGCReader{{Snapshot: Snapshot{Xmax: 10}}},
				Clog:    committedClog(),
				Visible: defaultVisible(),
			},
			want: []uint64{5},
		},
		{
			name: "reader skips invisible head keeps prefix",
			chain: []indexVersion{
				{offset: 300, xmin: 5},
				{offset: 200, xmin: 4},
				{offset: 100, xmin: 3},
			},
			ctx: IndexGCContext{
				Readers: []IndexGCReader{{
					Snapshot: Snapshot{Xmax: 5, Xip: map[uint64]bool{4: true}},
				}},
				Clog:    committedClog(),
				Visible: defaultVisible(),
			},
			want: []uint64{5, 4, 3},
		},
		{
			name:  "multi reader union keeps deepest skip prefix",
			chain: chain54321,
			ctx: IndexGCContext{
				Readers: []IndexGCReader{
					{Snapshot: Snapshot{Xmax: 10}},
					{Snapshot: Snapshot{Xmax: 4}},
				},
				Clog:    committedClog(),
				Visible: defaultVisible(),
			},
			want: []uint64{5, 4, 3},
		},
		{
			name:  "no readers keeps through newest committed",
			chain: chain54321,
			ctx: IndexGCContext{
				Clog: committedClog(),
			},
			want: []uint64{5},
		},
		{
			name: "no readers walks in progress head to committed",
			chain: []indexVersion{
				{offset: 200, xmin: 2},
				{offset: 100, xmin: 1},
			},
			ctx: IndexGCContext{
				Clog: func(xid uint64) TxStatus {
					if xid == 1 {
						return TxCommitted
					}
					return TxInProgress
				},
			},
			want: []uint64{2, 1},
		},
		{
			name: "active xid forces keep of in progress version",
			chain: []indexVersion{
				{offset: 200, xmin: 7},
				{offset: 100, xmin: 1},
			},
			ctx: IndexGCContext{
				ActiveXids: map[uint64]struct{}{7: {}},
				Clog:       committedClog(),
			},
			want: []uint64{7},
		},
		{
			name: "log floor drops below floor even in skip prefix",
			chain: []indexVersion{
				{offset: 400, xmin: 4},
				{offset: 300, xmin: 3},
				{offset: 200, xmin: 2},
				{offset: 50, xmin: 1},
			},
			ctx: IndexGCContext{
				Readers: []IndexGCReader{{
					Snapshot: Snapshot{Xmax: 2, Xip: map[uint64]bool{}},
				}},
				LogFloor: 100,
				Clog:     committedClog(),
				Visible:  defaultVisible(),
			},
			want: []uint64{4, 3, 2},
		},
		{
			name: "writable reader keeps own uncommitted write only in prefix",
			chain: []indexVersion{
				{offset: 300, xmin: 3},
				{offset: 200, xmin: 2},
				{offset: 100, xmin: 1},
			},
			ctx: IndexGCContext{
				Readers: []IndexGCReader{{
					Snapshot: Snapshot{Xmax: 4, Xip: map[uint64]bool{3: true}},
					MyXid:    3,
					Update:   true,
				}},
				ActiveXids: map[uint64]struct{}{3: {}},
				Clog:       committedClog(),
				Visible:    defaultVisible(),
			},
			want: []uint64{3},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mask := tc.ctx.keepMask(tc.chain)
			got := keptXmins(tc.chain, mask)
			if len(got) != len(tc.want) {
				t.Fatalf("keepMask xmins=%v want=%v mask=%v", got, tc.want, mask)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("keepMask xmins=%v want=%v mask=%v", got, tc.want, mask)
				}
			}
			filtered := filteredXmins(tc.ctx, tc.chain)
			if len(filtered) != len(tc.want) {
				t.Fatalf("FilterVersions=%v want=%v", filtered, tc.want)
			}
			for i := range tc.want {
				if filtered[i] != tc.want[i] {
					t.Fatalf("FilterVersions=%v want=%v", filtered, tc.want)
				}
			}
		})
	}
}

func TestCompactIndex_RegressionPreservesFrozenSnapshotRead(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("snap-key")
	if err := commitKeyValue(db, key, "v0"); err != nil {
		t.Fatal(err)
	}

	txSnap := db.NewTransaction(false)
	defer txSnap.Discard()

	for i := 0; i < 10; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("aborted")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}

	if err := commitKeyValue(db, key, "v1"); err != nil {
		t.Fatal(err)
	}

	arenaBefore, _ := db.IndexArenaStats()
	ctx := db.BuildIndexGCContext()
	if len(ctx.Readers) != 1 {
		t.Fatalf("expected one active reader in GC ctx, got %d", len(ctx.Readers))
	}

	if _, err := db.CompactIndex(ctx); err != nil {
		t.Fatal(err)
	}

	val, err := txSnap.Get(key)
	if err != nil {
		t.Fatalf("frozen snapshot read after compact: %v", err)
	}
	if string(val) != "v0" {
		t.Fatalf("expected frozen v0 after compact, got %q", val)
	}

	arenaAfter, _ := db.IndexArenaStats()
	if arenaAfter >= arenaBefore {
		t.Fatalf("expected arena shrink after abort fragmentation, before=%d after=%d", arenaBefore, arenaAfter)
	}

	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	val, err = rtx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Fatalf("expected v1 for fresh reader, got %q", val)
	}
}

func TestCompactIndex_RegressionDropsTailWhenHeadVisible(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("tail-key")
	for i := 0; i < 5; i++ {
		if err := commitKeyValue(db, key, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	ctx := db.BuildIndexGCContext()
	res, err := db.CompactIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ArenaAfter >= res.ArenaBefore {
		t.Fatalf("expected shrink, result=%+v", res)
	}

	var chainLen int
	var headXmin uint64
	db.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) {
			chainLen = len(chain)
			if len(chain) > 0 {
				headXmin = chain[0].xmin
			}
		}
	})
	if chainLen != 1 {
		t.Fatalf("expected single retained version, got chainLen=%d", chainLen)
	}

	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	val, err := rtx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v4" {
		t.Fatalf("expected latest value v4, got %q", val)
	}
	if headXmin == 0 {
		t.Fatal("expected non-zero head xmin")
	}
}

func TestCompactIndex_RegressionScanFloorDropsOldVersions(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("floor-key")
	if err := commitKeyValue(db, key, "old"); err != nil {
		t.Fatal(err)
	}

	var oldOffset int64
	db.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) && len(chain) > 0 {
			oldOffset = chain[0].offset
		}
	})

	if err := commitKeyValue(db, key, "new"); err != nil {
		t.Fatal(err)
	}

	var newOffset int64
	db.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) && len(chain) > 0 {
			newOffset = chain[0].offset
		}
	})
	if newOffset <= oldOffset {
		t.Fatalf("expected increasing offsets, old=%d new=%d", oldOffset, newOffset)
	}

	if err := db.SetScanFloor(newOffset); err != nil {
		t.Fatal(err)
	}

	ctx := db.BuildIndexGCContext()
	if ctx.LogFloor != newOffset {
		t.Fatalf("expected LogFloor=%d, got %d", newOffset, ctx.LogFloor)
	}

	if _, err := db.CompactIndex(ctx); err != nil {
		t.Fatal(err)
	}

	var chain []indexVersion
	db.index.ForEachKey(func(k []byte, c []indexVersion) {
		if string(k) == string(key) {
			chain = c
		}
	})
	if len(chain) != 1 {
		t.Fatalf("expected one version after scan floor compact, got %d", len(chain))
	}
	if chain[0].offset < newOffset {
		t.Fatalf("retained offset %d below scan floor %d", chain[0].offset, newOffset)
	}

	rtx := db.NewTransaction(false)
	defer rtx.Discard()
	val, err := rtx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "new" {
		t.Fatalf("expected value new, got %q", val)
	}
}

func TestMaybeCompactIndex_RegressionCompactsFragmentedShard(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		IndexCompactOnRetention:   indexCompactDisabled(),
		IndexCompactFragmentation: 2.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("frag-key")
	if err := commitKeyValue(db, key, "keep"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("abort")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}

	arenaBefore, liveBefore := db.IndexArenaStats()
	if arenaBefore <= liveBefore*2 {
		t.Fatalf("expected fragmentation before compact, arena=%d live=%d", arenaBefore, liveBefore)
	}

	res, err := db.MaybeCompactIndex()
	if err != nil {
		t.Fatal(err)
	}
	if res.ShardsCompacted == 0 {
		t.Fatalf("expected compacted shard, arena=%d live=%d", arenaBefore, liveBefore)
	}
	if res.ArenaAfter >= res.ArenaBefore {
		t.Fatalf("expected arena shrink, result=%+v", res)
	}

	checkKey(t, db, string(key), "keep")
}

func TestCompactIndex_RegressionMatchesGetVisibleAfterPrune(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("match-key")
	if err := commitKeyValue(db, key, "base"); err != nil {
		t.Fatal(err)
	}

	txSnap := db.NewTransaction(false)

	if err := commitKeyValue(db, key, "later"); err != nil {
		t.Fatal(err)
	}

	before, err := txSnap.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	ctx := db.BuildIndexGCContext()
	if _, err := db.CompactIndex(ctx); err != nil {
		t.Fatal(err)
	}

	after, err := txSnap.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("compact changed visible value: before=%q after=%q", before, after)
	}
	txSnap.Discard()
}

func TestBuildIndexGCContext_RegressionCapturesActiveWriter(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatal(err)
	}

	ctx := db.BuildIndexGCContext()
	if len(ctx.Readers) != 1 {
		t.Fatalf("expected 1 reader, got %d", len(ctx.Readers))
	}
	if !ctx.Readers[0].Update || ctx.Readers[0].MyXid == 0 {
		t.Fatalf("expected active writable reader, got %+v", ctx.Readers[0])
	}
	if _, ok := ctx.ActiveXids[ctx.Readers[0].MyXid]; !ok {
		t.Fatalf("expected active xid %d in ctx", ctx.Readers[0].MyXid)
	}

	chain := []indexVersion{{offset: 10, xmin: ctx.Readers[0].MyXid}}
	mask := ctx.keepMask(chain)
	if len(mask) != 1 || !mask[0] {
		t.Fatalf("expected in-progress version kept, mask=%v", mask)
	}

	tx.Discard()
}

func TestIndexCompactOnRetention_RegressionOptOut(t *testing.T) {
	db, err := Open(t.TempDir(), Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.indexCompactOnRetention {
		t.Fatal("expected compaction disabled when explicitly opted out")
	}
}

func commitKeyValue(db *DB, key []byte, value string) error {
	tx := db.NewTransaction(true)
	if err := tx.Put(key, []byte(value)); err != nil {
		tx.Discard()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
