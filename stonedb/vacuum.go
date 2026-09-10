// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"sync/atomic"
)

type holeSpan struct{ off, len int64 }

type deadVersion struct {
	key string
	ver indexVersion
}

// RunVacuum drops dead index versions and punches holes in stale log ranges.
// Vacuum runs per sealed log segment: a segment is eligible only when its own
// stale-byte tally meets minGarbageThreshold.
func (db *DB) RunVacuum() (bool, error) {
	if atomic.LoadInt32(&db.closed) == 1 {
		return false, nil
	}
	if !atomic.CompareAndSwapInt32(&db.vacuuming, 0, 1) {
		return false, nil
	}
	defer atomic.StoreInt32(&db.vacuuming, 0)

	if atomic.LoadInt32(&db.closed) == 1 {
		return false, nil
	}
	horizon := db.minActiveSnapshotXmax()
	dead := db.index.collectDeadVersions(horizon, db.clogStatus)
	if atomic.LoadInt32(&db.closed) == 1 {
		return false, nil
	}
	if len(dead) == 0 {
		return false, nil
	}

	deadBySeg := db.groupDeadBySegment(dead)
	if len(deadBySeg) == 0 {
		return false, nil
	}

	purgeFloor := atomic.LoadUint64(&db.scanWALFloor)
	writeOff := db.log.WriteOffset()
	blockSize, _ := fileBlockSize(db.dir)

	var didWork bool
	for segIdx, entries := range deadBySeg {
		if db.segments == nil || !db.segments.isSealed(segIdx) {
			continue
		}
		if db.segments.staleBytes(segIdx) < db.segmentGarbageThreshold() {
			continue
		}

		segStart, segEnd, ok := db.segments.bounds(segIdx, writeOff)
		if !ok {
			continue
		}

		var segStale int64
		var spans []holeSpan
		for _, d := range entries {
			if d.ver.offset < segStart || d.ver.offset >= segEnd {
				continue
			}
			size := recordSpanSize(len(d.key), int(d.ver.valueLen), WALRecordSet)
			if d.ver.tombstone {
				size = recordSpanSize(len(d.key), 0, WALRecordDelete)
			}
			segStale += size

			if d.ver.opID >= purgeFloor {
				continue
			}
			if db.index.HasLiveRefAtOffset(d.ver.offset) {
				continue
			}
			end := d.ver.offset + size
			if end > writeOff {
				continue
			}
			start, alignedEnd := alignRange(d.ver.offset, end, blockSize)
			if alignedEnd <= start {
				continue
			}
			spans = append(spans, holeSpan{start, alignedEnd - start})
		}

		if segStale == 0 {
			continue
		}

		db.index.RemoveDead(toRemoveSlice(entries))

		if len(spans) == 0 {
			db.segments.deductStale(segIdx, segStale)
			didWork = true
			continue
		}

		var punched int64
		for _, s := range mergeSpans(spans) {
			if err := db.log.PunchHole(s.off, s.len); err != nil {
				db.logger.Debug("Punch hole skipped", "off", s.off, "len", s.len, "err", err)
				continue
			}
			punched += s.len
		}
		if punched > 0 || segStale > 0 {
			db.segments.deductStale(segIdx, segStale)
			didWork = true
		}
	}

	return didWork, nil
}

func toRemoveSlice(entries []deadVersion) []struct {
	key string
	ver indexVersion
} {
	out := make([]struct {
		key string
		ver indexVersion
	}, len(entries))
	for i, e := range entries {
		out[i].key = e.key
		out[i].ver = e.ver
	}
	return out
}

func (db *DB) groupDeadBySegment(dead []struct {
	key string
	ver indexVersion
}) map[int][]deadVersion {
	out := make(map[int][]deadVersion)
	for _, d := range dead {
		idx := 0
		if db.segments != nil {
			idx = db.segments.segmentIndexForOffset(d.ver.offset)
		}
		out[idx] = append(out[idx], deadVersion{d.key, d.ver})
	}
	return out
}

func mergeSpans(spans []holeSpan) []holeSpan {
	if len(spans) == 0 {
		return nil
	}
	for i := 0; i < len(spans); i++ {
		for j := i + 1; j < len(spans); j++ {
			if spans[j].off < spans[i].off {
				spans[i], spans[j] = spans[j], spans[i]
			}
		}
	}
	out := []holeSpan{spans[0]}
	for i := 1; i < len(spans); i++ {
		last := &out[len(out)-1]
		if spans[i].off <= last.off+last.len {
			end := last.off + last.len
			if spans[i].off+spans[i].len > end {
				last.len = spans[i].off + spans[i].len - last.off
			}
		} else {
			out = append(out, spans[i])
		}
	}
	return out
}
