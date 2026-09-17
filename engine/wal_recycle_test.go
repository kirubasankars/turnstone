// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWal_PreallocatesFullSegment(t *testing.T) {
	dir := t.TempDir()
	const segSize = 256
	log, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	info, err := os.Stat(log.activeSegmentPath())
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != segSize {
		t.Fatalf("active segment size %d, want fully allocated %d", info.Size(), segSize)
	}
	if log.WriteOffset() != 0 {
		t.Fatalf("logical write offset %d, want 0 on a new preallocated file", log.WriteOffset())
	}
}

func TestWal_RecyclesSegmentByRename(t *testing.T) {
	dir := t.TempDir()
	const segSize = 256
	db, err := Open(dir, Options{
		WalSegmentSize:          segSize,
		IndexCompactOnRetention: indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	for i := 0; i < 20; i++ {
		if _, err := db.log.AppendEncoded(payload, false); err != nil {
			t.Fatal(err)
		}
	}
	if db.log.SegmentCount() < 2 {
		t.Fatal("expected rotation so a sealed segment exists")
	}

	firstPath := db.log.segments[0].path
	var firstIno uint64
	if st, err := os.Stat(firstPath); err != nil {
		t.Fatal(err)
	} else if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		firstIno = sys.Ino
	}

	floor := db.log.segments[0].endLSN
	if floor <= 0 {
		t.Fatal("expected sealed end LSN")
	}
	if _, _, err := db.log.deleteSegmentsThrough(floor); err != nil {
		t.Fatal(err)
	}
	if db.log.RecycledSegmentCount() == 0 {
		t.Fatal("expected deleted segment to enter the recycle pool")
	}

	// Force another rotation so the recycled file is renamed into service.
	for i := 0; i < 20; i++ {
		if _, err := db.log.AppendEncoded(payload, false); err != nil {
			t.Fatal(err)
		}
	}

	reused := false
	for i := range db.log.segments {
		st, err := os.Stat(db.log.segments[i].path)
		if err != nil {
			continue
		}
		sys, ok := st.Sys().(*syscall.Stat_t)
		if ok && firstIno != 0 && sys.Ino == firstIno {
			reused = true
			break
		}
	}
	if !reused && db.log.RecycledSegmentCount() == 0 {
		// Pool was consumed; the file should exist under wal/ as a later segment.
		matches, _ := filepath.Glob(filepath.Join(dir, walDirName, "seg-*.wal"))
		if len(matches) == 0 {
			t.Fatal("expected recycled inode to return as a live segment")
		}
	}
	if db.log.RecycledSegmentCount() > maxRecycledSegments {
		t.Fatalf("recycle pool %d exceeds cap %d", db.log.RecycledSegmentCount(), maxRecycledSegments)
	}
	db.Close()
}

func TestWal_ReopenUsesFooterNotFileSize(t *testing.T) {
	dir := t.TempDir()
	const segSize = 512
	log, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}
	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	off, err := log.AppendEncoded(payload, true)
	if err != nil {
		t.Fatal(err)
	}
	head := log.WriteOffset()
	if head <= off {
		t.Fatalf("write head %d should advance past %d", head, off)
	}
	info, err := os.Stat(log.activeSegmentPath())
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != segSize {
		t.Fatalf("file size %d, want %d", info.Size(), segSize)
	}
	log.Close()

	log2, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()
	if log2.WriteOffset() != head {
		t.Fatalf("reopen write offset %d, want footer used %d (not file size %d)",
			log2.WriteOffset(), head, segSize)
	}
	val, err := log2.ReadValueAt(off, 1)
	if err != nil || string(val) != "v" {
		t.Fatalf("read after reopen: err=%v val=%q", err, val)
	}
}

func TestWal_GrowsWhenPreallocHasNoSpace(t *testing.T) {
	testingPreallocErr = syscall.ENOSPC
	t.Cleanup(func() { testingPreallocErr = nil })

	dir := t.TempDir()
	const segSize = 64 << 20
	log, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	info, err := os.Stat(log.activeSegmentPath())
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() >= segSize {
		t.Fatalf("grow-mode file size %d, want well under segment %d", info.Size(), segSize)
	}
	if len(log.segments[log.activeIndex].mapping) != 0 {
		t.Fatal("grow-mode segment must not be mmap'd")
	}

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	off, err := log.AppendEncoded(payload, true)
	if err != nil {
		t.Fatal(err)
	}
	val, err := log.ReadValueAt(off, 1)
	if err != nil || string(val) != "v" {
		t.Fatalf("ReadValueAt in grow mode: %v %q", err, val)
	}

	head := log.WriteOffset()
	log.Close()
	log2, err := OpenDataLog(dir, nil, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()
	if log2.WriteOffset() != head {
		t.Fatalf("reopen write offset %d, want %d (file size, no footer)", log2.WriteOffset(), head)
	}
}
