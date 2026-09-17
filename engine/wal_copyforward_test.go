// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"testing"
	"time"
)

func TestMaybeCopyForwardWal_ReclaimsFragmentedLog(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 2.0,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("k")
	if err := commitKeyValue(db, key, "keep"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("abort")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}

	allocatedBefore := db.log.AllocatedBytesOnDisk()
	if allocatedBefore < 512 {
		t.Fatalf("expected fragmented wal, allocated=%d", allocatedBefore)
	}

	res, err := db.MaybeCopyForwardWal(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.FramesCopied == 0 {
		t.Fatalf("expected copy-forward, allocatedBefore=%d segments=%d", allocatedBefore, db.log.SegmentCount())
	}
	if res.BytesReclaimed == 0 {
		t.Fatalf("expected bytes reclaimed, result=%+v", res)
	}

	allocatedAfter := db.log.AllocatedBytesOnDisk()
	if allocatedAfter >= allocatedBefore {
		t.Fatalf("expected shrink, before=%d after=%d", allocatedBefore, allocatedAfter)
	}

	rtx := db.NewTransaction(false)
	val, err := rtx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "keep" {
		t.Fatalf("expected keep, got %q", val)
	}
	rtx.Discard()
	db.Close()
}

func TestMaybeCopyForwardWal_SkipsWithActiveTransaction(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              128,
		WalCopyForwardFragmentation: 1.5,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatal(err)
	}

	res, err := db.MaybeCopyForwardWal(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.FramesCopied != 0 {
		t.Fatalf("expected skip with active txn, got %+v", res)
	}
	tx.Discard()
}

func TestMaybeCopyForwardWal_ExcludesConcurrentWriter(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 2.0,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	if err := commitKeyValue(db, key, "keep"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("abort")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}

	writeErr := make(chan error, 1)
	testingAfterCopyForwardCollect = func() {
		if db.walRewriteMu.TryLock() {
			db.walRewriteMu.Unlock()
			t.Error("walRewriteMu not held during copy-forward collect")
		}
		go func() {
			tx := db.NewTransaction(true)
			if err := tx.Put([]byte("concurrent"), []byte("v")); err != nil {
				tx.Discard()
				writeErr <- err
				return
			}
			writeErr <- tx.Commit()
		}()
		time.Sleep(50 * time.Millisecond)
		select {
		case err := <-writeErr:
			t.Errorf("write completed during copy-forward: %v", err)
		default:
		}
	}
	t.Cleanup(func() { testingAfterCopyForwardCollect = nil })

	res, err := db.MaybeCopyForwardWal(0)
	if err != nil {
		t.Fatalf("copy-forward: %v", err)
	}
	if res.FramesCopied == 0 {
		t.Fatal("expected copy-forward")
	}

	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatalf("concurrent write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent write still blocked after copy-forward")
	}

	checkKey(t, db, "k", "keep")
	checkKey(t, db, "concurrent", "v")
}

func TestMaybeCopyForwardWal_RemapsIndexOffsets(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              200,
		WalCopyForwardFragmentation: 2.0,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("remap-key")
	if err := commitKeyValue(db, key, "v"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		w := db.NewTransaction(true)
		_ = w.Put(key, []byte("x"))
		w.Discard()
	}

	var beforeOff int64
	db.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) && len(chain) > 0 {
			beforeOff = chain[0].offset
		}
	})

	res, err := db.MaybeCopyForwardWal(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.FramesCopied == 0 {
		t.Fatal("expected copy-forward")
	}

	var afterOff int64
	db.index.ForEachKey(func(k []byte, chain []indexVersion) {
		if string(k) == string(key) && len(chain) > 0 {
			afterOff = chain[0].offset
		}
	})
	if afterOff == beforeOff {
		t.Fatalf("expected remapped offset, still %d", afterOff)
	}
	if afterOff <= beforeOff {
		t.Fatalf("expected forward remap old=%d new=%d", beforeOff, afterOff)
	}

	rtx := db.NewTransaction(false)
	val, err := rtx.Get(key)
	if err != nil || string(val) != "v" {
		t.Fatalf("read after remap: err=%v val=%q", err, val)
	}
	rtx.Discard()
	db.Close()
}

func TestMaybeCopyForwardWal_SkipsWhenRatioNotMet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 100,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := commitKeyValue(db, []byte("k"), "v"); err != nil {
		t.Fatal(err)
	}

	before := db.log.AllocatedBytesOnDisk()
	res, err := db.MaybeCopyForwardWal(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.FramesCopied != 0 {
		t.Fatalf("expected skip when ratio not met, got %+v", res)
	}
	if db.log.AllocatedBytesOnDisk() != before {
		t.Fatalf("expected unchanged allocation, before=%d after=%d", before, db.log.AllocatedBytesOnDisk())
	}
}

func TestMaybeCopyForwardWal_PreservesSnapshotReadBelowScanFloor(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 2.0,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("snap-key")
	if err := commitKeyValue(db, key, "old"); err != nil {
		t.Fatal(err)
	}

	rtx := db.NewTransaction(false)
	oldVal, err := rtx.Get(key)
	if err != nil || string(oldVal) != "old" {
		t.Fatalf("snapshot read before update: err=%v val=%q", err, oldVal)
	}

	for i := 0; i < 20; i++ {
		w := db.NewTransaction(true)
		if err := w.Put(key, []byte("abort")); err != nil {
			t.Fatal(err)
		}
		w.Discard()
	}
	if err := commitKeyValue(db, key, "new"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetScanFloor(db.log.WriteOffset()); err != nil {
		t.Fatal(err)
	}

	res, err := db.MaybeCopyForwardWal(db.ScanFloor())
	if err != nil {
		t.Fatal(err)
	}
	if res.FramesCopied == 0 {
		t.Fatal("expected copy-forward with fragmentation")
	}

	val, err := rtx.Get(key)
	if err != nil || string(val) != "old" {
		t.Fatalf("snapshot should still read old value, err=%v val=%q", err, val)
	}
	rtx.Discard()

	cur := db.NewTransaction(false)
	curVal, err := cur.Get(key)
	if err != nil || string(curVal) != "new" {
		t.Fatalf("current read should see new value, err=%v val=%q", err, curVal)
	}
	cur.Discard()
	db.Close()
}

func TestMaybeCopyForwardWal_ReopenPreservesRemappedData(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 2.0,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		if err := commitKeyValue(db, []byte(k), k+"-val"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		w := db.NewTransaction(true)
		_ = w.Put([]byte("a"), []byte("abort"))
		w.Discard()
	}

	if _, err := db.MaybeCopyForwardWal(0); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, Options{IndexCompactOnRetention: indexCompactDisabled()})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for _, k := range keys {
		checkKey(t, db2, k, k+"-val")
	}
}

func TestMaybeCopyForwardWal_CopyForwardsCommittedOverwrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 2.0,
		WalCopyForwardOnRetention:   walCopyForwardDisabled(),
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("ow")
	if err := commitKeyValue(db, key, "v0"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 20; i++ {
		if err := commitKeyValue(db, key, "v1"); err != nil {
			t.Fatal(err)
		}
	}

	allocatedBefore := db.log.AllocatedBytesOnDisk()
	res, err := db.MaybeCopyForwardWal(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.FramesCopied == 0 {
		t.Fatalf("expected copy-forward after overwrite generations, allocated=%d segments=%d", allocatedBefore, db.log.SegmentCount())
	}
	checkKey(t, db, "ow", "v1")
}

func TestRunWalMaintenance_IncludesCopyForward(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              256,
		WalCopyForwardFragmentation: 2.0,
		IndexCompactOnRetention:     indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("k")
	if err := commitKeyValue(db, key, "v"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		w := db.NewTransaction(true)
		_ = w.Put(key, []byte("a"))
		w.Discard()
	}

	before := db.log.AllocatedBytesOnDisk()
	if err := db.RunWalMaintenance(); err != nil {
		t.Fatal(err)
	}
	after := db.log.AllocatedBytesOnDisk()
	if after >= before {
		t.Fatalf("expected wal maintenance shrink, before=%d after=%d", before, after)
	}
	db.Close()
}

func TestRunWalMaintenance_ReclaimsOverwriteGenerations(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              4096,
		WalCopyForwardFragmentation: 3.0,
		IndexCompactFragmentation:   3.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numKeys = 24
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte{'k', '-', byte('A' + i)}
		if err := commitKeyValue(db, keys[i], "g0"); err != nil {
			t.Fatal(err)
		}
	}
	for gen := 1; gen <= 6; gen++ {
		for _, key := range keys {
			if err := commitKeyValue(db, key, "g"); err != nil {
				t.Fatal(err)
			}
		}
	}

	logicalBefore := db.log.logicalUsedBytes()
	if err := db.RunWalMaintenance(); err != nil {
		t.Fatal(err)
	}
	logicalAfter := db.log.logicalUsedBytes()
	if logicalAfter >= logicalBefore {
		t.Fatalf("expected overwrite generations to reclaim WAL, before=%d after=%d allocated=%d",
			logicalBefore, logicalAfter, db.log.AllocatedBytesOnDisk())
	}
	// Live data is one generation; reclaimed log should be well under the
	// pre-maintenance span (six extra overwrite passes).
	if logicalAfter*2 >= logicalBefore {
		t.Fatalf("expected reclaim toward one live generation, before=%d after=%d", logicalBefore, logicalAfter)
	}
	for _, key := range keys {
		checkKey(t, db, string(key), "g")
	}
}

func TestOverwriteCommitLatency_StableAfterMaintenance(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:              1024,
		WalCopyForwardFragmentation: 3.0,
		IndexCompactFragmentation:   3.0,
		UnsafeDisableFsync:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numKeys = 64
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte{'k', byte('0' + i/10), byte('0' + i%10)}
		if err := commitKeyValue(db, keys[i], "base"); err != nil {
			t.Fatal(err)
		}
	}

	overwriteAll := func(tag string) error {
		for _, key := range keys {
			if err := commitKeyValue(db, key, tag); err != nil {
				return err
			}
		}
		return nil
	}

	start := time.Now()
	if err := overwriteAll("a"); err != nil {
		t.Fatal(err)
	}
	first := time.Since(start)

	for i := 0; i < 5; i++ {
		if err := overwriteAll("b"); err != nil {
			t.Fatal(err)
		}
	}
	logicalBefore := db.log.logicalUsedBytes()
	if err := db.RunWalMaintenance(); err != nil {
		t.Fatal(err)
	}
	logicalAfter := db.log.logicalUsedBytes()
	if logicalAfter >= logicalBefore {
		t.Fatalf("expected reclaim before latency check, before=%d after=%d", logicalBefore, logicalAfter)
	}

	start = time.Now()
	if err := overwriteAll("c"); err != nil {
		t.Fatal(err)
	}
	second := time.Since(start)

	limit := first * 2
	if limit < 20*time.Millisecond {
		limit = 20 * time.Millisecond
	}
	if second > limit {
		t.Fatalf("overwrite batch slowed after fragmentation: first=%s after=%s limit=%s", first, second, limit)
	}
	for _, key := range keys {
		checkKey(t, db, string(key), "c")
	}
}
