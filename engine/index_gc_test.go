// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"math"
	"testing"
)

func TestIndexGCContext_KeepMask_OldestSnapshotNeedsOlderVersion(t *testing.T) {
	chain := []indexVersion{
		{offset: 300, xmin: 5},
		{offset: 200, xmin: 4},
		{offset: 100, xmin: 3},
	}
	ctx := IndexGCContext{
		Readers: []IndexGCReader{{
			Snapshot: Snapshot{Xmax: 5, Xip: map[uint64]bool{4: true}},
		}},
		Clog:    func(uint64) TxStatus { return TxCommitted },
		Visible: func(xmin uint64, snap Snapshot) bool { return xmin < snap.Xmax && !snap.contains(xmin) },
	}

	mask := ctx.keepMask(chain)
	if !mask[0] || !mask[1] || !mask[2] {
		t.Fatalf("expected full prefix kept for skip path, mask=%v", mask)
	}

	out := ctx.FilterVersions(nil, chain)
	if len(out) != 3 {
		t.Fatalf("expected 3 kept versions, got %d", len(out))
	}
}

func TestIndexGCContext_KeepMask_NoReadersKeepsNewestCommitted(t *testing.T) {
	chain := []indexVersion{
		{offset: 200, xmin: 2},
		{offset: 100, xmin: 1},
	}
	ctx := IndexGCContext{
		Clog: func(xid uint64) TxStatus {
			if xid == 1 {
				return TxCommitted
			}
			return TxInProgress
		},
	}

	out := ctx.FilterVersions(nil, chain)
	if len(out) != 2 || out[0].xmin != 2 || out[1].xmin != 1 {
		t.Fatalf("expected head plus first committed, got %+v", out)
	}
}

func TestIndexGCContext_LogFloorDropsVersions(t *testing.T) {
	chain := []indexVersion{
		{offset: 200, xmin: 2},
		{offset: 50, xmin: 1},
	}
	ctx := IndexGCContext{
		LogFloor: 100,
		Clog:     func(uint64) TxStatus { return TxCommitted },
	}

	out := ctx.FilterVersions(nil, chain)
	if len(out) != 1 || out[0].xmin != 2 {
		t.Fatalf("expected only version above log floor, got %+v", out)
	}
}

func TestDB_CompactIndex_ShrinksAfterAbortFragmentation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{IndexCompactOnRetention: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("gc-key")
	tx := db.NewTransaction(true)
	if err := tx.Put(key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 8; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}

	arenaBefore, _ := db.IndexArenaStats()
	ctx := db.BuildIndexGCContext()
	res, err := db.CompactIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	arenaAfter, _ := db.IndexArenaStats()
	if arenaAfter >= arenaBefore {
		t.Fatalf("expected arena shrink, before=%d after=%d result=%+v", arenaBefore, arenaAfter, res)
	}

	rtx := db.NewTransaction(false)
	val, err := rtx.Get(key)
	if err != nil || string(val) != "v1" {
		t.Fatalf("expected readable v1 after compact, err=%v val=%q", err, val)
	}
	rtx.Discard()
}

func TestIndexGCContext_MinActiveSnapshotXmax(t *testing.T) {
	ctx := IndexGCContext{
		Readers: []IndexGCReader{
			{Snapshot: Snapshot{Xmax: 10}},
			{Snapshot: Snapshot{Xmax: 7}},
		},
	}
	if ctx.MinActiveSnapshotXmax() != 7 {
		t.Fatalf("expected min xmax 7, got %d", ctx.MinActiveSnapshotXmax())
	}
	if (IndexGCContext{}).MinActiveSnapshotXmax() != math.MaxUint64 {
		t.Fatal("expected MaxUint64 when no readers")
	}
}

func TestDB_MaybeCompactIndex_SkipsHealthyShards(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		IndexCompactOnRetention:   false,
		IndexCompactFragmentation: 3.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	res, err := db.MaybeCompactIndex()
	if err != nil {
		t.Fatal(err)
	}
	if res.ShardsCompacted != 0 {
		t.Fatalf("expected no compaction for healthy shard, got %+v", res)
	}
}
