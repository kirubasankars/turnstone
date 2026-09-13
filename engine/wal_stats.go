// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import "os"

// WalSegmentInfo is a point-in-time view of one WAL segment file.
type WalSegmentInfo struct {
	ID           uint32 `json:"id"`
	File         string `json:"file"`
	BaseLSN      int64  `json:"base_lsn"`
	EndLSN       int64  `json:"end_lsn"`
	SizeBytes    int64  `json:"size_bytes"`
	LiveBytes    int64  `json:"live_bytes"`
	GarbageBytes int64  `json:"garbage_bytes"`
	Active       bool   `json:"active"`
}

// WalSegmentMetrics is per-segment WAL size, estimated live bytes, and garbage.
type WalSegmentMetrics struct {
	Segments     []WalSegmentInfo
	LiveBytes    int64
	GarbageBytes int64
}

// SegmentInfos lists WAL files with on-disk size and LSN range. Live/garbage are zero.
func (l *DataLog) SegmentInfos() []WalSegmentInfo {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segmentInfosLocked()
}

func (l *DataLog) segmentInfosLocked() []WalSegmentInfo {
	out := make([]WalSegmentInfo, len(l.segments))
	for i, seg := range l.segments {
		end := seg.endLSN
		if end == 0 {
			end = l.writeOffset
		}
		var size int64
		if info, err := os.Stat(seg.path); err == nil {
			size = info.Size()
		}
		out[i] = WalSegmentInfo{
			ID:        seg.id,
			File:      seg.file,
			BaseLSN:   seg.baseLSN,
			EndLSN:    end,
			SizeBytes: size,
			Active:    i == l.activeIndex,
		}
	}
	return out
}

// WalSegmentCount returns the number of WAL segment files.
func (db *DB) WalSegmentCount() int {
	if db.log == nil {
		return 0
	}
	return db.log.SegmentCount()
}

// WalSegmentMetrics estimates live MVCC frames per WAL segment without reading frames.
func (db *DB) WalSegmentMetrics() WalSegmentMetrics {
	if db.log == nil {
		return WalSegmentMetrics{Segments: []WalSegmentInfo{}}
	}

	infos := db.log.SegmentInfos()
	if len(infos) == 0 {
		return WalSegmentMetrics{Segments: []WalSegmentInfo{}}
	}

	liveByIdx := make([]int64, len(infos))
	if db.index != nil {
		ctx := db.BuildIndexGCContext()
		seen := make(map[int64]struct{})
		db.index.ForEachKey(func(key []byte, chain []indexVersion) {
			kept := ctx.FilterVersionsForWalRetain(key, chain)
			for _, v := range kept {
				if _, ok := seen[v.offset]; ok {
					continue
				}
				seen[v.offset] = struct{}{}
				i := segmentIndexForInfo(infos, v.offset)
				if i < 0 {
					continue
				}
				liveByIdx[i] += estimatedWalFrameSize(key, v.tombstone, v.valueLen)
			}
		})
	}

	var liveTotal, garbageTotal int64
	for i := range infos {
		infos[i].LiveBytes = liveByIdx[i]
		garbage := infos[i].SizeBytes - liveByIdx[i]
		if garbage < 0 {
			garbage = 0
		}
		infos[i].GarbageBytes = garbage
		liveTotal += liveByIdx[i]
		garbageTotal += garbage
	}
	return WalSegmentMetrics{
		Segments:     infos,
		LiveBytes:    liveTotal,
		GarbageBytes: garbageTotal,
	}
}

func segmentIndexForInfo(segments []WalSegmentInfo, lsn int64) int {
	for i, seg := range segments {
		if lsn >= seg.BaseLSN && lsn < seg.EndLSN {
			return i
		}
	}
	return -1
}

// estimatedWalFrameSize is the on-disk SET/DELETE frame length for a version.
func estimatedWalFrameSize(key []byte, tombstone bool, valueLen uint32) int64 {
	bodyLen := 4 + len(key)
	if !tombstone {
		bodyLen += 4 + int(valueLen)
	}
	return int64(LogFrameHeaderSize + LogRecordHeaderSize + bodyLen)
}
