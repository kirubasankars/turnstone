// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWal_MigratesLegacyDataLog(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, logFileName)
	if err := os.WriteFile(legacyPath, []byte{0, 0, 0, 0}, 0o644); err != nil {
		t.Fatal(err)
	}

	log, err := OpenDataLog(dir, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatal("expected legacy data.log migrated away")
	}
	manifestPath := filepath.Join(dir, walDirName, walManifestName)
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("expected manifest at %s: %v", manifestPath, err)
	}
}

func TestWal_GlobalLSNAcrossSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	const segSize = 256
	log, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	frameLen := int(frameSize(len(payload)))

	var firstOff int64 = -1
	for i := 0; i < 20; i++ {
		off, err := log.AppendEncoded(payload, false)
		if err != nil {
			t.Fatal(err)
		}
		if firstOff < 0 {
			firstOff = off
		}
	}
	if log.SegmentCount() < 2 {
		t.Fatalf("expected segment rotation with small segment size, got %d segments", log.SegmentCount())
	}

	val, err := log.ReadValueAt(firstOff, 1)
	if err != nil || string(val) != "v" {
		t.Fatalf("read from first segment after rotation: err=%v val=%q", err, val)
	}
	if lastOff := log.WriteOffset(); lastOff <= firstOff+int64(frameLen) {
		t.Fatalf("expected global LSN to grow across segments, first=%d last=%d", firstOff, lastOff)
	}
}

func TestWal_ReadLogRangeCrossesSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	const segSize = 128
	log, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("value")})
	for i := 0; i < 15; i++ {
		if _, err := log.AppendEncoded(payload, false); err != nil {
			t.Fatal(err)
		}
	}
	head := log.WriteOffset()
	data, end, err := log.ReadLogRange(0, head)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || end <= 0 {
		t.Fatalf("expected cross-segment read, len=%d end=%d", len(data), end)
	}
	frames, err := validateFrames(data)
	if err != nil {
		t.Fatalf("validate cross-segment batch: %v", err)
	}
	if len(frames) == 0 {
		t.Fatal("expected frames from cross-segment read")
	}
	log.Close()

	db, err := Open(dir, Options{WalSegmentSize: segSize})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.log.SegmentCount() < 2 {
		t.Fatalf("expected persisted segments after reopen, got %d", db.log.SegmentCount())
	}
}

func TestWal_ReplayPreservesGlobalOffsets(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 200}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var storedOffset int64
	db.index.ForEachKey(func(_ []byte, chain []indexVersion) {
		if len(chain) > 0 {
			storedOffset = chain[0].offset
		}
	})
	db.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, err := db2.log.ReadValueAt(storedOffset, 1)
	if err != nil || string(val) != "v" {
		t.Fatalf("replay offset read failed: err=%v val=%q offset=%d", err, val, storedOffset)
	}
}

func TestWal_IsFrameBoundaryAtSegmentBase(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("a"), Value: []byte("1")})
	for i := 0; i < 10; i++ {
		if _, err := log.AppendEncoded(payload, false); err != nil {
			t.Fatal(err)
		}
	}
	if log.SegmentCount() < 2 {
		t.Fatal("expected rotation")
	}
	// Second segment base should be a valid frame boundary.
	base := log.segments[1].baseLSN
	if !log.IsFrameBoundary(base) {
		t.Fatalf("expected frame boundary at segment base %d", base)
	}
}
