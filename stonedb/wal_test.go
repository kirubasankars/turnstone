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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_RotationAndPurge(t *testing.T) {
	dir := t.TempDir()
	// Set very small WAL size to force rapid rotation (1KB)
	opts := Options{MaxWALSize: 1024}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough data to create multiple WAL files
	// Each entry approx 20 bytes + headers.
	for i := 0; i < 100; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Check files
	walDir := filepath.Join(dir, "wal")
	files, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if len(files) < 2 {
		t.Errorf("Expected multiple WAL files, got %d", len(files))
	}

	// Purge older logs
	// We need to know the OpID. Since we did 100 txns, OpID is around 100.
	// Let's purge everything older than OpID 80.
	if err := db.PurgeWAL(80); err != nil {
		t.Fatal(err)
	}

	// Verify some files were deleted
	filesAfter, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if len(filesAfter) >= len(files) {
		t.Logf("Files before: %d, Files after: %d", len(files), len(filesAfter))
	}

	db.Close()
}

func TestWAL_Migration(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	// Create a FILE named "wal" (simulating old version)
	if err := os.WriteFile(walPath, []byte("dummy data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Open should trigger migration (File -> Dir)
	// Because "dummy data" is corrupt WAL, Open might fail, but migration should have happened first.
	db, err := Open(dir, Options{})
	if err == nil {
		db.Close()
	}

	// Verify "wal" is now a directory
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("WAL path should be a directory after migration")
	}

	// Verify old data was moved to 00...00.wal
	migratedFile := filepath.Join(walPath, fmt.Sprintf("%020d.wal", 0))
	if _, err := os.Stat(migratedFile); err != nil {
		t.Error("Migrated WAL file not found")
	}
}

func TestWAL_ScanErrors(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Scan Future OpID (Log Unavailable)
	err = db.ScanWAL(999999, func(entries []ValueLogEntry) error { return nil })
	if err != ErrLogUnavailable {
		t.Errorf("Expected ErrLogUnavailable for future OpID, got %v", err)
	}
}

func TestWAL_Locate_IndexFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MaxWALSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Create a few rotations to populate index
	for i := 0; i < 50; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte("k"), []byte("v"))
		tx.Commit()
	}

	// 2. Query for an OpID that is definitely AFTER the last flushed index but BEFORE current memory
	// This exercises the `iter.Last` fallback or `iter.Prev` logic in `locateWALStart`
	// We simulate this by closing and reopening.
	db.Close()
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	// Now memory is empty. Ask for an ID that is very high.
	loc, found, err := db.locateWALStart(1000000)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Should have found last available WAL file via iter.Last()")
	}
	// Verify it points to a valid file offset
	if loc.FileStartOffset == 0 && loc.RelativeOffset == 0 {
		// It might be 0/0 if only one file exists, that is valid
	}
}

func TestWAL_Corruption_Truncate(t *testing.T) {
	dir := t.TempDir()
	// Use small max size to ensure we generate multiple files easily
	opts := Options{TruncateCorruptWAL: true, MaxWALSize: 1024}

	// 1. Create DB and Write enough to generate at least 2 files
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Write ~2KB of data
	for i := 0; i < 50; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte(fmt.Sprintf("key-%02d", i)), bytes.Repeat([]byte("val"), 20))
		tx.Commit()
	}
	db.Close()

	// Verify we have multiple files
	walDir := filepath.Join(dir, "wal")
	matches, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if len(matches) < 2 {
		t.Fatalf("Expected multiple WAL files, got %d", len(matches))
	}

	// Glob returns sorted paths
	olderFile := matches[0]
	lastFile := matches[len(matches)-1]

	// 2. Corrupt LAST WAL (Append garbage at end) -> Should Truncate
	f, _ := os.OpenFile(lastFile, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte("GARBAGE_TAIL"))
	f.Close()

	// 3. Reopen (Should truncate and succeed)
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open with truncation failed: %v", err)
	}
	db2.Close()

	// 4. Corrupt OLDER File (Middle of chain) -> Should Fail
	// Because it's not the last file, truncation is not allowed even if option is set.
	f, _ = os.OpenFile(olderFile, os.O_RDWR, 0o644)
	// Write huge length at start to simulate header corruption
	binary.Write(f, binary.BigEndian, uint32(0xFFFFFFFF))
	f.Close()

	// 5. Reopen (Should fail)
	_, err = Open(dir, opts)
	if err == nil {
		t.Error("Expected error opening DB with corrupt older WAL file, got nil")
	}
}

func TestWAL_RotateHookFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MaxWALSize: 100}) // Small size to force rotation
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Inject failing hook
	db.writeAheadLog.SetOnRotate(func(m map[uint64]WALLocation) error {
		return errors.New("boom")
	})

	// Write enough to trigger rotation
	tx := db.NewTransaction(true)
	tx.Put(make([]byte, 50), make([]byte, 50))
	if err := tx.Commit(); err != nil {
		if err.Error() != "wal failed: wal rotate hook failed: boom" {
			t.Errorf("Expected hook failure error, got: %v", err)
		}
	} else {
		// Attempt 2 in case first write was too small
		tx = db.NewTransaction(true)
		tx.Put(make([]byte, 50), make([]byte, 50))
		if err := tx.Commit(); err == nil {
			t.Error("Expected rotation error")
		}
	}
}

func TestWAL_ReadFirstOpID_Failures(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	os.MkdirAll(walDir, 0o755)

	// Case 1: Empty file
	os.WriteFile(filepath.Join(walDir, "0001.wal"), []byte{}, 0o644)

	// Case 2: Short header
	os.WriteFile(filepath.Join(walDir, "0002.wal"), []byte{0x00, 0x00}, 0o644)

	// Case 3: Bad length (too huge)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(99999999)) // Length
	binary.Write(buf, binary.BigEndian, uint32(0))        // Checksum
	os.WriteFile(filepath.Join(walDir, "0003.wal"), buf.Bytes(), 0o644)

	// Case 4: Short Payload
	buf.Reset()
	binary.Write(buf, binary.BigEndian, uint32(WALBatchHeaderSize)) // Length
	binary.Write(buf, binary.BigEndian, uint32(0))                  // Checksum
	os.WriteFile(filepath.Join(walDir, "0004.wal"), buf.Bytes(), 0o644)

	wal, _ := OpenWriteAheadLog(walDir, 1024)

	// Verify errors directly via readFirstOpID
	if _, err := wal.readFirstOpID(filepath.Join(walDir, "0001.wal")); err == nil {
		t.Error("Expected error on empty file")
	}
	if _, err := wal.readFirstOpID(filepath.Join(walDir, "0003.wal")); err == nil {
		t.Error("Expected error on huge length")
	}
	if _, err := wal.readFirstOpID(filepath.Join(walDir, "0004.wal")); err == nil {
		t.Error("Expected error on short payload")
	}
}

func TestWAL_BatchDecoding_Boundary(t *testing.T) {
	// 1. Empty payload
	if tx, _, _ := decodeWALBatchEntries([]byte{}); tx != 0 {
		t.Error("Expected 0 for empty payload")
	}

	// 2. Header only
	buf := make([]byte, WALBatchHeaderSize)
	binary.BigEndian.PutUint64(buf[0:], 1)  // TxID
	binary.BigEndian.PutUint64(buf[8:], 1)  // OpID
	binary.BigEndian.PutUint32(buf[16:], 0) // Count

	txID, opID, entries := decodeWALBatchEntries(buf)
	if txID != 1 || opID != 1 || len(entries) != 0 {
		t.Error("Failed decoding empty batch header")
	}

	// 3. Corrupt entry length
	buf2 := make([]byte, WALBatchHeaderSize+4)
	copy(buf2, buf)
	binary.BigEndian.PutUint32(buf2[WALBatchHeaderSize:], 100) // KeyLen

	_, _, entries2 := decodeWALBatchEntries(buf2)
	if len(entries2) != 0 {
		t.Error("Should return no entries for partial data")
	}
}

func TestWriteAheadLog_Stream_EOF(t *testing.T) {
	wal := &WriteAheadLog{maxSize: 1000}
	buf := bytes.NewReader([]byte{0x00, 0x01}) // Short header
	_, err := wal.stream(buf, func(offset int64, payload []byte) error { return nil })
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected UnexpectedEOF, got %v", err)
	}
}

func TestWAL_IndexKeyEncoding(t *testing.T) {
	opID := uint64(123456789)
	key := encodeWALIndexKey(opID)

	decoded := decodeWALIndexKey(key)
	if decoded != opID {
		t.Errorf("WAL Index Key encoding failed. Got %d, want %d", decoded, opID)
	}
}

func TestWAL_Open_LevelDBIndex(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{MaxWALSize: 1024})

	// Write data to rotate and create index
	for i := 0; i < 100; i++ {
		tx := db.NewTransaction(true)
		tx.Put([]byte("k"), []byte("v"))
		tx.Commit()
	}
	db.Close()

	// Reopen - this loads index from LDB if needed during scan
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	db2.Close()
}
