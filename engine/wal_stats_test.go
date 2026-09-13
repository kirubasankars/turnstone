// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import "testing"

func TestEstimatedWalFrameSize_MatchesEncodedSetAndDelete(t *testing.T) {
	key := []byte("hello")
	val := []byte("world")

	setPayload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: key, Value: val})
	if got := estimatedWalFrameSize(key, false, uint32(len(val))); got != frameSize(len(setPayload)) {
		t.Fatalf("set frame: got %d want %d", got, frameSize(len(setPayload)))
	}

	delPayload := encodeRecord(Record{Type: RecordDelete, XID: 1, Key: key})
	if got := estimatedWalFrameSize(key, true, 0); got != frameSize(len(delPayload)) {
		t.Fatalf("delete frame: got %d want %d", got, frameSize(len(delPayload)))
	}
}

func TestWalSegmentMetrics_LiveAndGarbagePerSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		WalSegmentSize:            256,
		WalCopyForwardOnRetention: walCopyForwardDisabled(),
		IndexCompactOnRetention:   indexCompactDisabled(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := commitKeyValue(db, []byte("keep"), "live-value"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if err := commitKeyValue(db, []byte("keep"), "stale"); err != nil {
			t.Fatal(err)
		}
	}
	if db.log.SegmentCount() < 2 {
		t.Fatalf("expected segment rotation, got %d", db.log.SegmentCount())
	}

	m := db.WalSegmentMetrics()
	if len(m.Segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(m.Segments))
	}
	if m.LiveBytes <= 0 {
		t.Fatal("expected wal live bytes")
	}
	if db.WalSegmentCount() != len(m.Segments) {
		t.Fatalf("segment count %d != infos %d", db.WalSegmentCount(), len(m.Segments))
	}

	var liveSum, garbageSum int64
	var foundLive, foundGarbage, foundActive, foundSealed bool
	for _, seg := range m.Segments {
		liveSum += seg.LiveBytes
		garbageSum += seg.GarbageBytes
		if seg.SizeBytes > 0 && seg.LiveBytes <= seg.SizeBytes && seg.LiveBytes+seg.GarbageBytes != seg.SizeBytes {
			t.Fatalf("size != live+garbage: %+v", seg)
		}
		if seg.LiveBytes > 0 {
			foundLive = true
		}
		if seg.GarbageBytes > 0 {
			foundGarbage = true
		}
		if seg.Active {
			foundActive = true
		} else {
			foundSealed = true
		}
	}
	if liveSum != m.LiveBytes {
		t.Fatalf("live sum %d != total %d", liveSum, m.LiveBytes)
	}
	if garbageSum != m.GarbageBytes {
		t.Fatalf("garbage sum %d != total %d", garbageSum, m.GarbageBytes)
	}
	if !foundLive || !foundGarbage || !foundActive || !foundSealed {
		t.Fatalf("expected live/garbage split across sealed and active, metrics=%+v", m)
	}

	arena, indexLive := db.IndexArenaStats()
	if arena == 0 || indexLive == 0 {
		t.Fatalf("expected index arena and live bytes, arena=%d live=%d", arena, indexLive)
	}
}
