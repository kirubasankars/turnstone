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
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestValueLog_ReadCorruption(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write value
	tx := db.NewTransaction(true)
	tx.Put([]byte("corrupt_me"), []byte("value"))
	tx.Commit()

	// Force flush
	db.Checkpoint()
	db.Close()

	// Locate and corrupt the vlog file
	matches, _ := filepath.Glob(filepath.Join(dir, "vlog", "*.vlog"))
	if len(matches) == 0 {
		t.Fatal("No vlog files found")
	}
	target := matches[0]

	// Corrupt a byte in the middle (payload)
	f, err := os.OpenFile(target, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 40); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen
	// Recover() should fail due to corruption in older file
	db2, err := Open(dir, Options{})
	if err == nil {
		db2.Close()
		t.Error("Expected error opening DB with corrupted older VLog file, got nil")
	}
}

func TestValueLog_DeleteActiveFileError(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Try to delete the current active VLog file (should fail)
	activeFid := db.valueLog.currentFid
	if err := db.valueLog.DeleteFile(activeFid); err == nil {
		t.Error("Expected error deleting active VLog file")
	}
}

func TestValueLog_LRU(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Manually lower max open files for testing
	db.valueLog.mu.Lock()
	db.valueLog.maxOpenFiles = 1
	db.valueLog.mu.Unlock()

	// Create 3 files: 0 (active), 1, 2
	for i := 0; i < 2; i++ {
		// Write something so rotation actually happens
		tx := db.NewTransaction(true)
		tx.Put([]byte("k"), []byte("v"))
		tx.Commit()
		db.Checkpoint() // Rotate
	}

	// Files 0, 1 should exist (rotated). 2 is active.

	// Force handle creation to check LRU logic
	_, _ = db.valueLog.getFileHandle(0) // Cache: {0}
	_, _ = db.valueLog.getFileHandle(1) // Cache: {1}, Evicts 0

	db.valueLog.mu.RLock()
	if _, ok := db.valueLog.fileCache[0]; ok {
		t.Error("File 0 should have been evicted")
	}
	if _, ok := db.valueLog.fileCache[1]; !ok {
		t.Error("File 1 should be cached")
	}
	db.valueLog.mu.RUnlock()

	// Access 0 again
	_, _ = db.valueLog.getFileHandle(0) // Cache: {0}, Evicts 1
	db.valueLog.mu.RLock()
	if _, ok := db.valueLog.fileCache[0]; !ok {
		t.Error("File 0 should be cached again")
	}
	if _, ok := db.valueLog.fileCache[1]; ok {
		t.Error("File 1 should have been evicted")
	}
	db.valueLog.mu.RUnlock()
}

func TestValueLog_Stream_CheckSumError(t *testing.T) {
	// Manually construct a VLog entry with bad checksum
	key := []byte("k")
	val := []byte("v")

	var buf bytes.Buffer
	// Header: CRC(4) + KeyLen(4) + ValLen(4) + Tx(8) + Op(8) + Type(1)
	binary.Write(&buf, binary.BigEndian, uint32(0xDEADBEEF)) // BAD CRC
	binary.Write(&buf, binary.BigEndian, uint32(len(key)))
	binary.Write(&buf, binary.BigEndian, uint32(len(val)))
	binary.Write(&buf, binary.BigEndian, uint64(1)) // Tx
	binary.Write(&buf, binary.BigEndian, uint64(1)) // Op
	buf.WriteByte(0)                                // Type
	buf.Write(key)
	buf.Write(val)

	vl := &ValueLog{}
	_, _, _, err := vl.stream(&buf, func(offset int64, e ValueLogEntry, m EntryMeta) error {
		return nil
	})

	if err != ErrChecksum {
		t.Errorf("Expected ErrChecksum, got %v", err)
	}
}

func TestValueLog_Stream_EOF(t *testing.T) {
	vl := &ValueLog{}
	buf := bytes.NewReader([]byte{0x00, 0x01}) // Short header
	_, _, _, err := vl.stream(buf, nil)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected UnexpectedEOF, got %v", err)
	}
}

func TestVLog_ReadValue_FileError(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})

	tx := db.NewTransaction(true)
	tx.Put([]byte("A"), []byte("B"))
	tx.Commit()

	// Manually close and remove file 0
	db.valueLog.Close()
	os.Remove(filepath.Join(dir, "vlog", "0000.vlog"))

	// Force VLog to try and open a non-existent file ID
	db.mu.Lock()
	_, err := db.valueLog.ReadValue(999, 0, 10)
	db.mu.Unlock()

	if err == nil {
		t.Error("Expected error reading missing file")
	}
}
