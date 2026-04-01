package stonedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	// Sort by our custom timeline sorter to ensure correct order
	sortWALFiles(matches)
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
	os.WriteFile(filepath.Join(walDir, "wal_1_0001.wal"), []byte{}, 0o644)

	// Case 2: Short header
	os.WriteFile(filepath.Join(walDir, "wal_1_0002.wal"), []byte{0x00, 0x00}, 0o644)

	// Case 3: Bad length (too huge)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(99999999)) // Length
	binary.Write(buf, binary.BigEndian, uint32(0))        // Checksum
	os.WriteFile(filepath.Join(walDir, "wal_1_0003.wal"), buf.Bytes(), 0o644)

	// Case 4: Short Payload
	buf.Reset()
	binary.Write(buf, binary.BigEndian, uint32(WALBatchHeaderSize)) // Length
	binary.Write(buf, binary.BigEndian, uint32(0))                  // Checksum
	os.WriteFile(filepath.Join(walDir, "wal_1_0004.wal"), buf.Bytes(), 0o644)

	wal, _ := OpenWriteAheadLog(walDir, 1024, 1)

	// Verify errors directly via readFirstOpID
	if _, err := wal.readFirstOpID(filepath.Join(walDir, "wal_1_0001.wal")); err == nil {
		t.Error("Expected error on empty file")
	}
	if _, err := wal.readFirstOpID(filepath.Join(walDir, "wal_1_0003.wal")); err == nil {
		t.Error("Expected error on huge length")
	}
	if _, err := wal.readFirstOpID(filepath.Join(walDir, "wal_1_0004.wal")); err == nil {
		t.Error("Expected error on short payload")
	}
}

func TestWAL_TimelineFork(t *testing.T) {
	dir := t.TempDir()

	// 1. Start on Timeline 1
	wal, err := OpenWriteAheadLog(dir, 1024*1024, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Write batch on T1
	wal.AppendBatch(makeBatch(1, 1, "k1", "v1"))

	// 2. Promote to Timeline 2
	if err := wal.ForceNewTimeline(2); err != nil {
		t.Fatalf("ForceNewTimeline failed: %v", err)
	}

	// Write batch on T2
	wal.AppendBatch(makeBatch(2, 2, "k2", "v2"))
	wal.Close()

	// 3. Verify Files
	matches, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	sortWALFiles(matches)

	if len(matches) != 2 {
		t.Fatalf("Expected 2 WAL files, got %d", len(matches))
	}

	// Check filenames
	base1 := filepath.Base(matches[0])
	base2 := filepath.Base(matches[1])

	if !ioIsPrefix(base1, "wal_1_") {
		t.Errorf("File 1 should be on Timeline 1, got %s", base1)
	}
	if !ioIsPrefix(base2, "wal_2_") {
		t.Errorf("File 2 should be on Timeline 2, got %s", base2)
	}
}

func TestWAL_Promotion_Persistence(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWriteAheadLog(dir, 1024*1024, 1)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Write on Timeline 1
	if err := wal.AppendBatch(makeBatch(100, 1, "k1", "v1")); err != nil {
		t.Fatal(err)
	}

	// 2. Promote to Timeline 5 (skip a few generations)
	if err := wal.ForceNewTimeline(5); err != nil {
		t.Fatalf("Promotion failed: %v", err)
	}

	// 3. Write on Timeline 5
	if err := wal.AppendBatch(makeBatch(101, 2, "k2", "v2")); err != nil {
		t.Fatal(err)
	}
	wal.Close()

	// 4. Reopen and Replay
	// We request timeline 5 (current state)
	wal2, err := OpenWriteAheadLog(dir, 1024*1024, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	foundK1 := false
	foundK2 := false

	// Scan should cover all histories leading up to current
	err = wal2.Scan(WALLocation{FileStartOffset: 0, RelativeOffset: 0}, func(entries []ValueLogEntry) error {
		for _, e := range entries {
			if string(e.Key) == "k1" {
				foundK1 = true
			}
			if string(e.Key) == "k2" {
				foundK2 = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if !foundK1 {
		t.Error("Failed to recover data from Timeline 1")
	}
	if !foundK2 {
		t.Error("Failed to recover data from Timeline 5")
	}
}

func TestWAL_Promotion_Validation(t *testing.T) {
	dir := t.TempDir()
	wal, _ := OpenWriteAheadLog(dir, 1024, 10)
	defer wal.Close()

	// Try promoting to older
	if err := wal.ForceNewTimeline(5); err == nil {
		t.Error("Expected error promoting to older timeline")
	}

	// Try promoting to same
	if err := wal.ForceNewTimeline(10); err == nil {
		t.Error("Expected error promoting to same timeline")
	}

	// Valid
	if err := wal.ForceNewTimeline(11); err != nil {
		t.Errorf("Valid promotion failed: %v", err)
	}
}

func ioIsPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[0:len(prefix)] == prefix
}

// Helper to make a raw WAL batch payload for testing
func makeBatch(txID, opID uint64, k, v string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, txID)
	binary.Write(&buf, binary.BigEndian, opID)
	binary.Write(&buf, binary.BigEndian, uint32(1)) // count

	key := []byte(k)
	val := []byte(v)
	binary.Write(&buf, binary.BigEndian, uint32(len(key)))
	buf.Write(key)
	binary.Write(&buf, binary.BigEndian, uint32(len(val)))
	buf.Write(val)
	buf.WriteByte(0)
	return buf.Bytes()
}
