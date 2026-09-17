// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// How Turnstone handles WAL damage (see also engine/README.md):
//
//   * Active-segment tail (short frame, bad CRC, garbage, oversized length):
//     TruncateCorruptTail rewinds the logical end to the last valid frame and
//     keeps the prefix. The preallocated file is not shrunk.
//   * Same errors without TruncateCorruptTail: Open fails.
//   * Sealed-segment damage: Open fails. History is not rewritten.
//   * Missing segment / unreadable manifest: Open fails.
//   * Recycled unused bytes: ignored (footer used=0).
//   * Live GET / VerifyChecksums: report checksum/corrupt errors; no silent repair.

func TestCorruption_TailGarbage_TruncatesAndAllowsNewWrites(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "keep", "v1")
	head := db.log.WriteOffset()
	base := db.log.segments[db.log.activeIndex].baseLSN
	path := db.log.activeSegmentPath()
	db.Close()

	writeWAL(t, path, head-base, []byte{0xDE, 0xAD, 0xBE, 0xEF})
	mustWriteFooterUsed(t, path, dbSegmentSize(opts), head-base+4)

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "keep", "v1")
	if db2.log.WriteOffset() != head {
		t.Fatalf("logical head after tail repair = %d, want %d (not file size)", db2.log.WriteOffset(), head)
	}
	commitKV(t, db2, "after", "v2")
	mustGet(t, db2, "after", "v2")
	db2.Close()

	db3 := mustOpen(t, dir, opts)
	defer db3.Close()
	mustGet(t, db3, "keep", "v1")
	mustGet(t, db3, "after", "v2")
}

func TestCorruption_TailGarbage_WithoutTruncateFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "keep", "v1")
	head := db.log.WriteOffset()
	base := db.log.segments[db.log.activeIndex].baseLSN
	path := db.log.activeSegmentPath()
	db.Close()

	writeWAL(t, path, head-base, []byte{0xFF, 0xFF, 0xFF, 0xFF})
	mustWriteFooterUsed(t, path, dbSegmentSize(opts), head-base+4)

	if _, err := Open(dir, opts); err == nil {
		t.Fatal("expected Open to fail on tail garbage when TruncateCorruptTail is off")
	}
}

func TestCorruption_LastFrameCRC_DropsUncommittedTail(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "a", "1")
	commitKV(t, db, "b", "2")
	used := db.log.WriteOffset() - db.log.segments[db.log.activeIndex].baseLSN
	path := db.log.activeSegmentPath()
	db.Close()

	flipWAL(t, path, used-1)

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "a", "1")
	mustMissing(t, db2, "b")
}

func TestCorruption_MidActiveSegmentCRC_DropsSuffix(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "first", "1")
	afterFirst := db.log.WriteOffset()
	commitKV(t, db, "second", "2")
	commitKV(t, db, "third", "3")
	path := db.log.activeSegmentPath()
	base := db.log.segments[db.log.activeIndex].baseLSN
	db.Close()

	// Corrupt the first byte of the second transaction's frames.
	flipWAL(t, path, afterFirst-base)

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "first", "1")
	mustMissing(t, db2, "second")
	mustMissing(t, db2, "third")
}

func TestCorruption_SealedSegmentCRC_FailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 256, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	for i := 0; i < 20; i++ {
		commitKV(t, db, "k", "v")
	}
	if db.log.SegmentCount() < 2 {
		t.Fatal("expected a sealed segment")
	}
	sealed := db.log.segments[0].path
	db.Close()

	flipWAL(t, sealed, 0)

	if _, err := Open(dir, opts); err == nil {
		t.Fatal("expected Open to fail when a sealed segment is corrupt")
	}
}

func TestCorruption_FooterMagic_RecoversWithTruncate(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "keep", "ok")
	path := db.log.activeSegmentPath()
	segSize := db.log.segmentSize
	db.Close()

	writeWAL(t, path, segSize-walSegFooterSize, make([]byte, walSegFooterSize))

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "keep", "ok")
}

func TestCorruption_FooterUsedTooLarge_TruncatesAtValidPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "keep", "ok")
	path := db.log.activeSegmentPath()
	segSize := db.log.segmentSize
	db.Close()

	mustWriteFooterUsed(t, path, segSize, segSize-walSegFooterSize)

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "keep", "ok")
}

func TestCorruption_PartialLastFrame(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "keep", "ok")
	head := db.log.WriteOffset()
	base := db.log.segments[db.log.activeIndex].baseLSN
	path := db.log.activeSegmentPath()
	db.Close()

	// Half a frame header: length present, payload missing.
	hdr := make([]byte, 6)
	binary.BigEndian.PutUint32(hdr[0:], 100)
	writeWAL(t, path, head-base, hdr)
	mustWriteFooterUsed(t, path, dbSegmentSize(opts), head-base+6)

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "keep", "ok")
}

func TestCorruption_HugeFrameLength(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "keep", "ok")
	head := db.log.WriteOffset()
	base := db.log.segments[db.log.activeIndex].baseLSN
	path := db.log.activeSegmentPath()
	db.Close()

	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[0:], 1<<29)
	writeWAL(t, path, head-base, hdr)
	mustWriteFooterUsed(t, path, dbSegmentSize(opts), head-base+8)

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustGet(t, db2, "keep", "ok")
}

func TestCorruption_MissingSegmentFile(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "k", "v")
	path := db.log.activeSegmentPath()
	db.Close()

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, opts); err == nil {
		t.Fatal("expected Open to fail when the active segment file is missing")
	}
}

func TestCorruption_CorruptManifest(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, TruncateCorruptTail: true}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "k", "v")
	db.Close()

	manifest := filepath.Join(dir, walDirName, walManifestName)
	if err := os.WriteFile(manifest, []byte("not-json"), fileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, opts); err == nil {
		t.Fatal("expected Open to fail on a corrupt WAL manifest")
	}

	// Valid JSON but empty segment list.
	empty, _ := json.Marshal(map[string]any{"version": walManifestVersion, "segments": []any{}})
	if err := os.WriteFile(manifest, empty, fileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, opts); err == nil {
		t.Fatal("expected Open to fail when the manifest has no segments")
	}
}

func TestCorruption_RecycleIgnoresStaleBytes(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		WalSegmentSize:          256,
		TruncateCorruptTail:     true,
		ValueCacheBytes:         -1,
		IndexCompactOnRetention: indexCompactDisabled(),
	}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "stale", "secret")
	for i := 0; i < 24; i++ {
		commitKV(t, db, "hot", "x")
	}
	if db.log.SegmentCount() < 2 {
		t.Fatal("expected rotation")
	}
	tx := db.NewTransaction(true)
	if err := tx.Delete([]byte("stale")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		commitKV(t, db, "hot", "y")
	}
	floor := db.log.segments[0].endLSN
	if _, err := db.CompactIndex(db.BuildIndexGCContext()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.log.deleteSegmentsThrough(floor); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		commitKV(t, db, "fresh", "ok")
	}
	db.Close()

	db2 := mustOpen(t, dir, opts)
	defer db2.Close()
	mustMissing(t, db2, "stale")
	mustGet(t, db2, "fresh", "ok")
}

func TestCorruption_ReadValueAtDetectsChecksum(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	commitKV(t, db, "ck", "payload-data")
	var off int64
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		if string(key) == "ck" && len(chain) > 0 {
			off = chain[0].offset
		}
	})
	path := db.log.activeSegmentPath()
	base := db.log.segments[db.log.activeIndex].baseLSN
	db.Close()

	// Flip a byte inside the SET payload so CRC fails on the next read.
	flipWAL(t, path, off-base+LogFrameHeaderSize)

	db2 := mustOpen(t, dir, Options{WalSegmentSize: 4096, ValueCacheBytes: -1, TruncateCorruptTail: true})
	defer db2.Close()

	// Replay already truncated the damaged frame, so the key is gone.
	mustMissing(t, db2, "ck")
}

func TestCorruption_VerifyChecksumsOnLiveDB(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	defer db.Close()
	commitKV(t, db, "ck", "payload-data")
	if err := db.VerifyChecksums(); err != nil {
		t.Fatalf("clean WAL should verify: %v", err)
	}

	var off int64
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		if string(key) == "ck" && len(chain) > 0 {
			off = chain[0].offset
		}
	})
	base := db.log.segments[db.log.activeIndex].baseLSN
	flipWAL(t, db.log.activeSegmentPath(), off-base+4) // flip stored CRC

	if err := db.VerifyChecksums(); err == nil {
		t.Fatal("expected VerifyChecksums to report the flipped CRC")
	}
}

func TestCorruption_ReadValueAtOnOpenDB(t *testing.T) {
	dir := t.TempDir()
	opts := Options{WalSegmentSize: 4096, ValueCacheBytes: -1}

	db := mustOpen(t, dir, opts)
	defer db.Close()
	commitKV(t, db, "ck", "abcdefgh")

	var off int64
	var vlen uint32
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		if string(key) == "ck" && len(chain) > 0 {
			off = chain[0].offset
			vlen = chain[0].valueLen
		}
	})
	base := db.log.segments[db.log.activeIndex].baseLSN
	// Value starts after frame header + type + xid + keylen + key + vallen.
	valueOff := off - base + LogFrameHeaderSize + LogRecordHeaderSize + 4 + int64(len("ck")) + 4
	flipWAL(t, db.log.activeSegmentPath(), valueOff)

	_, err := db.log.ReadValueAt(off, vlen)
	if err == nil {
		t.Fatal("expected ReadValueAt to fail after the value bytes were flipped")
	}
	if !errors.Is(err, ErrChecksum) && !errors.Is(err, ErrCorruptData) {
		// decodeValueAt may return ErrCorruptData if CRC still matches by chance;
		// the flipped value almost always trips CRC first.
		t.Fatalf("expected checksum or corrupt error, got %v", err)
	}
}

func TestCorruption_ApplyLogRangeRejectsBadCRC(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, Options{WalSegmentSize: 4096})
	defer db.Close()
	commitKV(t, db, "k", "v")

	raw, _, err := db.ReadLogRange(0, 1<<20)
	if err != nil || len(raw) < 8 {
		t.Fatalf("ReadLogRange: %v len=%d", err, len(raw))
	}
	raw[4] ^= 0xFF
	if _, err := db.ApplyLogRange(raw); err == nil {
		t.Fatal("expected ApplyLogRange to reject a CRC-flipped batch")
	}
}

func mustOpen(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func commitKV(t *testing.T, db *DB, k, v string) {
	t.Helper()
	tx := db.NewTransaction(true)
	if err := tx.Put([]byte(k), []byte(v)); err != nil {
		tx.Discard()
		t.Fatalf("put %s: %v", k, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s: %v", k, err)
	}
}

func mustGet(t *testing.T, db *DB, k, want string) {
	t.Helper()
	tx := db.NewTransaction(false)
	defer tx.Discard()
	got, err := tx.Get([]byte(k))
	if err != nil || string(got) != want {
		t.Fatalf("Get(%s) = %q, %v; want %q", k, got, err, want)
	}
}

func mustMissing(t *testing.T, db *DB, k string) {
	t.Helper()
	tx := db.NewTransaction(false)
	defer tx.Discard()
	if _, err := tx.Get([]byte(k)); err != ErrKeyNotFound {
		t.Fatalf("Get(%s) = %v; want ErrKeyNotFound", k, err)
	}
}

func writeWAL(t *testing.T, path string, off int64, p []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, fileMode)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(p, off); err != nil {
		t.Fatal(err)
	}
}

func flipWAL(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, fileMode)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

func mustWriteFooterUsed(t *testing.T, path string, segmentSize, used int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, fileMode)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := writeSegmentFooter(f, segmentSize, used); err != nil {
		t.Fatal(err)
	}
}

func dbSegmentSize(opts Options) int64 {
	return normalizeWalSegmentSize(opts.WalSegmentSize)
}
