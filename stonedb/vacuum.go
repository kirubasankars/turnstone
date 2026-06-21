// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"sync/atomic"
)

type holeSpan struct{ off, len int64 }

// RunVacuum drops dead index versions and punches holes in stale log ranges.
func (db *DB) RunVacuum() (bool, error) {
	if !atomic.CompareAndSwapInt32(&db.vacuuming, 0, 1) {
		return false, nil
	}
	defer atomic.StoreInt32(&db.vacuuming, 0)

	horizon := db.minActiveSnapshotXmax()
	dead := db.index.collectDeadVersions(horizon, db.clogStatus)
	if len(dead) == 0 {
		return false, nil
	}

	var staleBytes int64
	for _, d := range dead {
		size := recordSpanSize(len(d.key), int(d.ver.valueLen), WALRecordSet)
		if d.ver.tombstone {
			size = recordSpanSize(len(d.key), 0, WALRecordDelete)
		}
		staleBytes += size
	}

	if staleBytes < db.minGarbageThreshold {
		return false, nil
	}

	db.index.RemoveDead(dead)

	purgeFloor := atomic.LoadUint64(&db.scanWALFloor)
	writeOff := db.log.WriteOffset()
	blockSize, _ := fileBlockSize(db.dir)

	var spans []holeSpan
	for _, d := range dead {
		if d.ver.opID >= purgeFloor {
			continue
		}
		if db.index.HasLiveRefAtOffset(d.ver.offset) {
			continue
		}
		sz := recordSpanSize(len(d.key), int(d.ver.valueLen), WALRecordSet)
		if d.ver.tombstone {
			sz = recordSpanSize(len(d.key), 0, WALRecordDelete)
		}
		end := d.ver.offset + sz
		if end > writeOff {
			continue
		}
		start, alignedEnd := alignRange(d.ver.offset, end, blockSize)
		if alignedEnd <= start {
			continue
		}
		spans = append(spans, holeSpan{start, alignedEnd - start})
	}

	if len(spans) == 0 {
		atomic.AddInt64(&db.staleBytes, staleBytes)
		return true, nil
	}

	for _, s := range mergeSpans(spans) {
		if err := db.log.PunchHole(s.off, s.len); err != nil {
			db.logger.Debug("Punch hole skipped", "off", s.off, "len", s.len, "err", err)
		}
	}

	atomic.AddInt64(&db.staleBytes, staleBytes)
	return true, nil
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
