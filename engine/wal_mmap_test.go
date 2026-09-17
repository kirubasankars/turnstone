// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"context"
	"runtime"
	"testing"
)

func TestWAL_SegmentsAreMapped(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	seg := &log.segments[log.activeIndex]
	if runtime.GOOS == "windows" {
		if len(seg.mapping) != 0 {
			t.Fatal("expected no WAL mmap on Windows")
		}
		return
	}
	if len(seg.mapping) != int(log.segmentSize) {
		t.Fatalf("mapped %d bytes, want segment size %d", len(seg.mapping), log.segmentSize)
	}

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	off, err := log.AppendEncoded(payload, true)
	if err != nil {
		t.Fatal(err)
	}
	val, err := log.ReadValueAt(off, 1)
	if err != nil || string(val) != "v" {
		t.Fatalf("ReadValueAt via mmap: %v %q", err, val)
	}
}

func TestWAL_ReplayUsesMapping(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		payload := encodeRecord(Record{Type: RecordSet, XID: uint64(i + 1), Key: []byte("k"), Value: []byte("v")})
		if _, err := log.AppendEncoded(payload, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	log2, err := OpenDataLog(dir, nil, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()
	var n int
	if err := log2.Replay(context.Background(), false, func(rec Record, span recordSpan) {
		n++
		if rec.Type != RecordSet {
			t.Fatalf("unexpected record %v", rec.Type)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Fatalf("replayed %d records, want 8", n)
	}
}
