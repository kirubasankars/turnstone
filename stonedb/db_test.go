// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package stonedb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDB_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 1. Put
	key := []byte("hello")
	val := []byte("world")
	tx := db.NewTransaction(true)
	if err := tx.Put(key, val); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 2. Get
	readTx := db.NewTransaction(false)
	got, err := readTx.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Expected %s, got %s", val, got)
	}
	readTx.Discard()

	// 3. Delete
	delTx := db.NewTransaction(true)
	if err := delTx.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := delTx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 4. Get (Not Found)
	readTx2 := db.NewTransaction(false)
	_, err = readTx2.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
	readTx2.Discard()
}

func TestDB_TransactionIsolation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("key")

	// Tx1 writes "v1"
	tx1 := db.NewTransaction(true)
	tx1.Put(key, []byte("v1"))
	tx1.Commit()

	// Tx2 starts (snapshot at "v1")
	tx2 := db.NewTransaction(false)

	// Tx3 writes "v2"
	tx3 := db.NewTransaction(true)
	tx3.Put(key, []byte("v2"))
	tx3.Commit()

	// Tx2 should still see "v1"
	val, err := tx2.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Errorf("Tx2 saw %s, expected v1", val)
	}
	tx2.Discard()

	// New Tx4 should see "v2"
	tx4 := db.NewTransaction(false)
	val, err = tx4.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v2" {
		t.Errorf("Tx4 saw %s, expected v2", val)
	}
	tx4.Discard()
}

func TestDB_WriteConflict(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("conflict")

	// Init
	tx := db.NewTransaction(true)
	tx.Put(key, []byte("init"))
	tx.Commit()

	// TxA reads
	txA := db.NewTransaction(true)
	_, _ = txA.Get(key)

	// TxB writes and commits
	txB := db.NewTransaction(true)
	txB.Put(key, []byte("updated"))
	if err := txB.Commit(); err != nil {
		t.Fatal(err)
	}

	// TxA tries to commit -> Should fail because it read old data
	txA.Put(key, []byte("overwrite"))
	err = txA.Commit()
	if err != ErrWriteConflict {
		t.Errorf("Expected ErrWriteConflict, got %v", err)
	}
}

func TestTransaction_Misuse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Write on Read-Only Tx
	roTx := db.NewTransaction(false)
	if err := roTx.Put([]byte("k"), []byte("v")); err == nil {
		t.Error("Expected error writing to RO tx")
	}
	if err := roTx.Delete([]byte("k")); err == nil {
		t.Error("Expected error deleting in RO tx")
	}
	if err := roTx.Commit(); err != nil {
		t.Errorf("Commit on RO tx should be no-op/nil, got %v", err)
	}
	roTx.Discard()

	// 2. Commit twice / Commit finished
	tx := db.NewTransaction(true)
	tx.Put([]byte("k"), []byte("v"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != ErrTxnFinished {
		t.Errorf("Expected ErrTxnFinished on second commit, got %v", err)
	}

	// 3. Get Deleted Key
	delTx := db.NewTransaction(true)
	delTx.Delete([]byte("k"))
	delTx.Commit()

	getTx := db.NewTransaction(false)
	_, err = getTx.Get([]byte("k"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key, got %v", err)
	}
	getTx.Discard()
}

func TestTransaction_MetaErrors(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()

	// Inject corrupt meta into index
	key := []byte("bad_meta")
	encKey := encodeIndexKey(key, ^uint64(0)) // TS max
	db.ldb.Put(encKey, []byte("short"), nil)

	tx := db.NewTransaction(false)
	_, err := tx.Get(key)
	if err == nil {
		t.Error("Expected error for corrupt meta")
	}
}

func TestDB_IdempotentClose(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close
	if err := db.Close(); err != nil {
		// Just ensure it doesn't crash
	}
}

func TestDB_Checkpoint_Empty(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	defer db.Close()

	// Checkpoint on empty DB
	if err := db.Checkpoint(); err != nil {
		t.Error(err)
	}
}

// TestDB_ApplyBatch verifies that ApplyBatch correctly writes data to all subsystems
// and advances the internal clocks without generating new IDs.
func TestDB_ApplyBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Simulate a batch from a leader or external source
	// TxID: 100, StartOpID: 500
	entries := []ValueLogEntry{
		{
			Key:           []byte("replica_k1"),
			Value:         []byte("val1"),
			TransactionID: 100,
			OperationID:   500,
			IsDelete:      false,
		},
		{
			Key:           []byte("replica_k2"),
			Value:         []byte("val2"),
			TransactionID: 100,
			OperationID:   501,
			IsDelete:      false,
		},
		{
			Key:           []byte("replica_k3"),
			Value:         nil,
			TransactionID: 100,
			OperationID:   502,
			IsDelete:      true, // Tombstone
		},
	}

	// Apply the batch
	if err := db.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// 1. Verify Clocks advanced
	if db.transactionID < 100 {
		t.Errorf("TransactionID not advanced. Got %d, want >= 100", db.transactionID)
	}
	if db.operationID < 502 {
		t.Errorf("OperationID not advanced. Got %d, want >= 502", db.operationID)
	}

	// 2. Verify Data Visibility via Standard Get
	tx := db.NewTransaction(false)
	defer tx.Discard()

	// k1 present
	val, err := tx.Get([]byte("replica_k1"))
	if err != nil {
		t.Errorf("Get k1 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("k1 mismatch. Got %s, want val1", val)
	}

	// k3 deleted (should be not found)
	_, err = tx.Get([]byte("replica_k3"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected k3 to be deleted, got %v", err)
	}

	// 3. Verify WAL Persistence
	// Scan from the beginning. We expect to find these entries.
	foundWAL := false
	err = db.ScanWAL(500, func(scanned []ValueLogEntry) error {
		for _, e := range scanned {
			if string(e.Key) == "replica_k1" && e.TransactionID == 100 {
				foundWAL = true
			}
		}
		return nil
	})
	if err != nil {
		t.Errorf("ScanWAL failed: %v", err)
	}
	if !foundWAL {
		t.Error("ApplyBatch data not found in WAL")
	}
}

// TestVLog_AppendEntries verifies low-level VLog appending and reading.
func TestVLog_AppendEntries(t *testing.T) {
	dir := t.TempDir()
	vl, err := OpenValueLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer vl.Close()

	entries := []ValueLogEntry{
		{Key: []byte("k1"), Value: []byte("v1"), TransactionID: 1, OperationID: 1},
		{Key: []byte("k2"), Value: []byte("v2"), TransactionID: 1, OperationID: 2},
	}

	// Test Append
	fileID, offset, err := vl.AppendEntries(entries)
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	if fileID != vl.currentFid {
		t.Errorf("Expected fileID %d, got %d", vl.currentFid, fileID)
	}

	// Verify reading back using returned offset
	// The offset points to the start of the batch (first entry).
	// Header(29) + Key(2) + Value(2) = 33 bytes for first entry?
	// Let's rely on ReadValue logic which handles headers.
	// ReadValue(fileID, offset, valLen)

	val, err := vl.ReadValue(fileID, offset, 2) // v1 length is 2
	if err != nil {
		t.Fatalf("ReadValue failed: %v", err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Errorf("Read back mismatch. Got %s", val)
	}
}

// TestWAL_AppendBatch_Direct verifies low-level WAL appending.
func TestWAL_AppendBatch_Direct(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWriteAheadLog(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Manually construct a batch payload
	var walBuf bytes.Buffer
	// Header: TxID(8) + StartOpID(8) + Count(4)
	binary.Write(&walBuf, binary.BigEndian, uint64(999)) // TxID
	binary.Write(&walBuf, binary.BigEndian, uint64(10))  // StartOpID
	binary.Write(&walBuf, binary.BigEndian, uint32(1))   // Count

	// Entry: KeyLen(4) + Key + ValLen(4) + Val + Type(1)
	key := []byte("wal_test")
	val := []byte("wal_val")
	binary.Write(&walBuf, binary.BigEndian, uint32(len(key)))
	walBuf.Write(key)
	binary.Write(&walBuf, binary.BigEndian, uint32(len(val)))
	walBuf.Write(val)
	walBuf.WriteByte(0) // Not delete

	// Append directly
	if err := wal.AppendBatch(walBuf.Bytes()); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	// Verify via Scan
	found := false
	err = wal.Scan(WALLocation{FileStartOffset: 0, RelativeOffset: 0}, func(entries []ValueLogEntry) error {
		for _, e := range entries {
			if e.TransactionID == 999 && string(e.Key) == "wal_test" {
				found = true
			}
		}
		return nil
	})
	if err != nil {
		t.Errorf("WAL Scan failed: %v", err)
	}
	if !found {
		t.Error("Did not find appended batch in WAL")
	}
}
