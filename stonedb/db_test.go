// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestDB_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("hello")
	val := []byte("world")
	tx := db.NewTransaction(true)
	if err := tx.Put(key, val); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	readTx := db.NewTransaction(false)
	got, err := readTx.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Expected %s, got %s", val, got)
	}
	readTx.Discard()

	delTx := db.NewTransaction(true)
	if err := delTx.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := delTx.Commit(); err != nil {
		t.Fatal(err)
	}

	readTx2 := db.NewTransaction(false)
	_, err = readTx2.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
	readTx2.Discard()
}

func TestDB_Promote_Integration(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	tx1 := db.NewTransaction(true)
	tx1.Put([]byte("t1_key"), []byte("val1"))
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := db.Promote(); err != nil {
		t.Fatalf("Promote failed: %v", err)
	}

	tx2 := db.NewTransaction(true)
	tx2.Put([]byte("t2_key"), []byte("val2"))
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	rtx := db.NewTransaction(false)
	v1, _ := rtx.Get([]byte("t1_key"))
	if string(v1) != "val1" {
		t.Error("Lost data from Timeline 0")
	}
	v2, _ := rtx.Get([]byte("t2_key"))
	if string(v2) != "val2" {
		t.Error("Missing data from Timeline 1")
	}
	rtx.Discard()

	db.Close()

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if db2.CurrentTimeline() != 1 {
		t.Errorf("Expected timeline 1, got %d", db2.CurrentTimeline())
	}

	rtx2 := db2.NewTransaction(false)
	defer rtx2.Discard()
	v1, err = rtx2.Get([]byte("t1_key"))
	if err != nil || string(v1) != "val1" {
		t.Errorf("t1_key lost after reopen: err=%v val=%q", err, v1)
	}
	v2, err = rtx2.Get([]byte("t2_key"))
	if err != nil || string(v2) != "val2" {
		t.Errorf("t2_key lost after reopen: err=%v val=%q", err, v2)
	}
	count, err := db2.KeyCount()
	if err != nil || count != 2 {
		t.Errorf("KeyCount after reopen: want 2, got %d err=%v", count, err)
	}
}

func TestDB_TransactionIsolation(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()

	txW := db.NewTransaction(true)
	txW.Put([]byte("iso_key"), []byte("draft"))
	txR := db.NewTransaction(false)
	_, err := txR.Get([]byte("iso_key"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected uncommitted write invisible, got %v", err)
	}
	txW.Commit()
	txR2 := db.NewTransaction(false)
	val, err := txR2.Get([]byte("iso_key"))
	if err != nil || string(val) != "draft" {
		t.Errorf("Expected committed value visible, got %v %q", err, val)
	}
}

func TestDB_WriteConflict(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()

	tx1 := db.NewTransaction(true)
	tx2 := db.NewTransaction(true)
	if err := tx1.Put([]byte("hot"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Put([]byte("hot"), []byte("b")); err != ErrWriteConflict {
		t.Fatalf("Expected write conflict, got %v", err)
	}
}

func TestDB_IdempotentClose(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
}

func TestDB_EphemeralIndex_CloseSkipsUnmap(t *testing.T) {
	dir := t.TempDir()
	indexDir := filepath.Join(dir, "index")

	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("ephemeral"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(indexDir); err != nil {
		t.Fatalf("index dir should exist while db is open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(indexDir); err != nil {
		t.Fatalf("index dir may remain after close (fast shutdown): %v", err)
	}

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	count, err := db2.KeyCount()
	if err != nil || count != 1 {
		t.Fatalf("KeyCount after reopen: want 1, got %d err=%v", count, err)
	}
	tx2 := db2.NewTransaction(false)
	val, err := tx2.Get([]byte("ephemeral"))
	tx2.Discard()
	if err != nil || string(val) != "value" {
		t.Fatalf("GET after reopen: err=%v val=%q", err, val)
	}
}

func TestOpen_CancelDuringReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Promote(); err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	const n = 50000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("cancel-replay-%d", i))
		if err := tx.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := OpenContext(ctx, dir, Options{})
		errCh <- err
	}()
	cancel()

	err = <-errCh
	if err == nil {
		t.Fatal("expected cancel during replay")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDB_FastClose_SkipsIndexUnmap(t *testing.T) {
	dir := t.TempDir()
	indexDir := filepath.Join(dir, "index")

	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("fast-close-%d", i))
		if err := tx.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("close took too long: %v", elapsed)
	}
	if _, err := os.Stat(indexDir); err != nil {
		t.Fatalf("index dir should remain after fast close: %v", err)
	}
}

func TestDB_Checkpoint_Empty(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()
	if err := db.Checkpoint(); err != nil {
		t.Error(err)
	}
}

func TestDB_ApplyRecord(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const xid = uint64(100)
	recs := []WALRecord{
		{Type: WALRecordBegin, XID: xid, OpID: 500},
		{Type: WALRecordSet, XID: xid, OpID: 501, Key: []byte("replica_k1"), Value: []byte("val1")},
		{Type: WALRecordSet, XID: xid, OpID: 502, Key: []byte("replica_k2"), Value: []byte("val2")},
		{Type: WALRecordDelete, XID: xid, OpID: 503, Key: []byte("replica_k3")},
		{Type: WALRecordCommit, XID: xid, OpID: 504},
	}
	for _, r := range recs {
		if err := db.ApplyRecord(r); err != nil {
			t.Fatalf("ApplyRecord failed: %v", err)
		}
	}

	tx := db.NewTransaction(false)
	defer tx.Discard()
	val, err := tx.Get([]byte("replica_k1"))
	if err != nil || !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("Get k1: %v %q", err, val)
	}
	_, err = tx.Get([]byte("replica_k3"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected k3 deleted, got %v", err)
	}

	foundWAL := false
	err = db.ScanWAL(500, func(scanned []WALRecord) error {
		for _, r := range scanned {
			if r.Type == WALRecordSet && string(r.Key) == "replica_k1" {
				foundWAL = true
			}
		}
		return nil
	})
	if err != nil || !foundWAL {
		t.Errorf("ScanWAL: err=%v found=%v", err, foundWAL)
	}

	count, _ := db.KeyCount()
	if count != 2 {
		t.Errorf("Expected KeyCount 2, got %d", count)
	}
}

func TestDataLog_AppendAndScan(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	payload := encodeWALRecord(WALRecord{Type: WALRecordSet, XID: 999, OpID: 10, Key: []byte("k"), Value: []byte("v")})
	off, err := log.AppendReplicatedRecord(payload, true)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	err = log.Scan(10, func(recs []WALRecord) error {
		for _, r := range recs {
			if r.XID == 999 {
				found = true
			}
		}
		return nil
	})
	if err != nil || !found {
		t.Fatalf("scan: err=%v found=%v off=%d", err, found, off)
	}
}

func TestDB_KeyCount(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	count, _ := db.KeyCount()
	if count != 0 {
		t.Fatalf("Expected 0 keys, got %d", count)
	}

	tx := db.NewTransaction(true)
	tx.Put([]byte("k1"), []byte("v1"))
	tx.Put([]byte("k2"), []byte("v2"))
	tx.Commit()

	count, _ = db.KeyCount()
	if count != 2 {
		t.Errorf("Expected 2 keys, got %d", count)
	}
}

func TestOpen_ErrorPaths(t *testing.T) {
	if os.Geteuid() != 0 {
		dir := t.TempDir()
		os.Chmod(dir, 0o400)
		_, err := Open(filepath.Join(dir, "nested"), Options{})
		if err == nil {
			t.Error("Expected error opening in read-only directory")
		}
	}

	dir2 := t.TempDir()
	os.WriteFile(filepath.Join(dir2, logFileName), []byte("not-a-dir"), 0o644)
	_, err := Open(dir2, Options{})
	if err == nil {
		t.Error("Expected error when data.log path conflicts")
	}
}

func TestBackgroundChecksum_Coverage(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{ChecksumInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	db.Close()
}

func TestScanWAL_AfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}
	db, _ := Open(dir, opts)
	for i := 0; i < 20; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		tx.Commit()
	}
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	count := 0
	err = db2.ScanWAL(1, func(recs []WALRecord) error {
		count += len(recs)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Error("expected records after reopen replay")
	}
}

func TestApplyRecord_StaleBytes(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()

	tx := db.NewTransaction(true)
	tx.Put([]byte("key1"), []byte("old"))
	tx.Commit()

	recs := []WALRecord{
		{Type: WALRecordBegin, XID: 100, OpID: 200},
		{Type: WALRecordSet, XID: 100, OpID: 201, Key: []byte("key1"), Value: []byte("new")},
		{Type: WALRecordCommit, XID: 100, OpID: 202},
	}
	for _, r := range recs {
		if err := db.ApplyRecord(r); err != nil {
			t.Fatal(err)
		}
	}

	if db.TotalGarbageBytes() == 0 {
		t.Error("expected stale bytes after overwrite")
	}
}

func TestDB_RunAutoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{AutoCheckpointInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	tx.Put([]byte("key"), []byte("val"))
	tx.Commit()
	currentOp := atomic.LoadUint64(&db.operationID)
	time.Sleep(150 * time.Millisecond)
	if atomic.LoadUint64(&db.lastCkptOpID) < currentOp {
		t.Error("AutoCheckpoint did not update lastCkptOpID")
	}
}

func TestDB_RunAutoVacuum(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		CompactionInterval:   50 * time.Millisecond,
		CompactionMinGarbage: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 30; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k-%d", i)), []byte("v"))
		tx.Commit()
	}
	tx := db.NewTransaction(true)
	tx.Delete([]byte("k-0"))
	tx.Commit()

	time.Sleep(200 * time.Millisecond)
}
