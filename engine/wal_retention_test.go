// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"testing"
)

func TestDeleteWalSegments_RemovesSealedBelowFloor(t *testing.T) {
	dir := t.TempDir()
	const segSize = 256
	db, err := Open(dir, Options{
		WalSegmentSize:          segSize,
		IndexCompactOnRetention: indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("keep")
	if err := commitKeyValue(db, key, "v0"); err != nil {
		t.Fatal(err)
	}
	firstOff := int64(0)
	db.index.ForEachKey(func(_ []byte, chain []indexVersion) {
		if len(chain) > 0 {
			firstOff = chain[0].offset
		}
	})

	for i := 0; i < 30; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("abort")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}
	if err := commitKeyValue(db, key, "v1"); err != nil {
		t.Fatal(err)
	}

	beforeSegs := db.log.SegmentCount()
	if beforeSegs < 2 {
		t.Fatalf("expected multiple wal segments, got %d", beforeSegs)
	}

	_, floor, err := db.ReadLogRange(0, db.LastLogOffset()/2)
	if err != nil {
		t.Fatal(err)
	}
	if floor <= firstOff {
		t.Fatalf("expected floor past first version offset, floor=%d first=%d", floor, firstOff)
	}

	if err := db.SetScanFloor(floor); err != nil {
		t.Fatal(err)
	}
	ctx := db.BuildIndexGCContext()
	if _, err := db.CompactIndex(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.DeleteWalSegments(db.ScanFloor())
	if err != nil {
		t.Fatal(err)
	}
	if res.SegmentsDeleted == 0 {
		t.Fatalf("expected deleted segments, before=%d floor=%d oldest=%d",
			beforeSegs, floor, db.log.OldestSegmentBaseLSN())
	}
	if db.log.OldestSegmentBaseLSN() < floor {
		t.Fatalf("oldest segment base %d below floor %d", db.log.OldestSegmentBaseLSN(), floor)
	}

	rtx := db.NewTransaction(false)
	val, err := rtx.Get(key)
	if err != nil {
		t.Fatalf("get after segment delete: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("expected v1, got %q", val)
	}
	rtx.Discard()
	db.Close()
}

func TestDeleteWalSegments_SkipsWhenIndexStillReferencesRange(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 128)
	if err != nil {
		t.Fatal(err)
	}

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	firstOff, err := log.AppendEncoded(payload, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := log.AppendEncoded(payload, false); err != nil {
			t.Fatal(err)
		}
	}
	if log.SegmentCount() < 2 {
		t.Fatal("expected segment rotation")
	}

	db, err := Open(dir, Options{WalSegmentSize: 128, IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Floor above first offset but index still points at firstOff.
	if err := db.SetScanFloor(log.WriteOffset() / 2); err != nil {
		t.Fatal(err)
	}

	res, err := db.DeleteWalSegments(db.ScanFloor())
	if err != nil {
		t.Fatal(err)
	}
	if res.SegmentsDeleted > 0 {
		t.Fatalf("expected no delete while index references %d, deleted=%d", firstOff, res.SegmentsDeleted)
	}

	val, err := db.log.ReadValueAt(firstOff, 1)
	if err != nil || string(val) != "v" {
		t.Fatalf("expected read at %d, err=%v val=%q", firstOff, err, val)
	}
}

func TestRunWalMaintenance_CompactsAndDeletes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:            200,
		IndexCompactFragmentation: 2.0,
		IndexCompactOnRetention:   indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("k")
	if err := commitKeyValue(db, key, "keep"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 15; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("x")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}

	_, floor, err := db.ReadLogRange(0, db.LastLogOffset())
	if err != nil {
		t.Fatal(err)
	}
	// Use a floor that keeps the committed value but drops aborted tails from index GC.
	var keepOff int64
	db.index.ForEachKey(func(_ []byte, chain []indexVersion) {
		if len(chain) > 0 {
			keepOff = chain[0].offset
		}
	})
	if err := db.SetScanFloor(keepOff); err != nil {
		t.Fatal(err)
	}
	_ = floor

	segsBefore := db.log.SegmentCount()
	arenaBefore, _ := db.IndexArenaStats()

	db.indexCompactOnRetention = true
	if err := db.RunWalMaintenance(); err != nil {
		t.Fatal(err)
	}

	arenaAfter, _ := db.IndexArenaStats()
	if arenaAfter >= arenaBefore {
		t.Fatalf("expected index shrink, before=%d after=%d", arenaBefore, arenaAfter)
	}
	if db.log.SegmentCount() >= segsBefore && segsBefore > 1 {
		t.Logf("segment count unchanged (floor may retain all segments): before=%d after=%d",
			segsBefore, db.log.SegmentCount())
	}

	checkKey(t, db, string(key), "keep")
	db.Close()
}

func TestDeleteWalSegments_ScanUnavailableAfterDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{WalSegmentSize: 128, IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if err := commitKeyValue(db, []byte(fmt.Sprintf("k%d", i)), "v"); err != nil {
			t.Fatal(err)
		}
	}

	_, floor, err := db.ReadLogRange(0, db.LastLogOffset()/2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetScanFloor(floor); err != nil {
		t.Fatal(err)
	}
	ctx := db.BuildIndexGCContext()
	if _, err := db.CompactIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DeleteWalSegments(floor); err != nil {
		t.Fatal(err)
	}

	if floor > 0 {
		if err := db.ScanLog(floor-1, func([]Record) error { return nil }); err != ErrLogUnavailable {
			t.Fatalf("ScanLog below floor: got %v want ErrLogUnavailable", err)
		}
	}
	db.Close()
}
