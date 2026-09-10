// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import "sync"

// DefaultSegmentTargetSize is the approximate byte span sealed per log segment.
const DefaultSegmentTargetSize = 10 * 1024 * 1024

type logSegment struct {
	id          int
	startOffset int64
	endOffset   int64 // exclusive; valid only when sealed
	staleBytes  int64
	sealed      bool
}

// segmentTracker partitions data.log into logical ~targetSize segments sealed only
// after group-commit fsync so no transaction spans a segment boundary.
type segmentTracker struct {
	mu         sync.Mutex
	targetSize int64
	segments   []logSegment
}

func newSegmentTracker(targetSize int64, writeOffset int64) *segmentTracker {
	if targetSize <= 0 {
		targetSize = DefaultSegmentTargetSize
	}
	st := &segmentTracker{targetSize: targetSize}
	st.segments = []logSegment{{
		id:          0,
		startOffset: 0,
		sealed:      false,
	}}
	_ = writeOffset
	return st
}

func (st *segmentTracker) segmentIndexForOffset(off int64) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.segmentIndexForOffsetLocked(off)
}

func (st *segmentTracker) segmentIndexForOffsetLocked(off int64) int {
	for i := len(st.segments) - 1; i >= 0; i-- {
		seg := st.segments[i]
		if off < seg.startOffset {
			continue
		}
		if !seg.sealed || off < seg.endOffset {
			return i
		}
	}
	return 0
}

func (st *segmentTracker) addStaleAtOffset(off int64, nbytes int64) {
	if nbytes <= 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	idx := st.segmentIndexForOffsetLocked(off)
	st.segments[idx].staleBytes += nbytes
}

func (st *segmentTracker) addStaleToActive(nbytes int64) {
	if nbytes <= 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.segments[len(st.segments)-1].staleBytes += nbytes
}

// maybeSealAfterCommit closes the active segment once it reaches targetSize.
// Call only after a group-commit fsync so segment ends on a transaction boundary.
func (st *segmentTracker) maybeSealAfterCommit(writeOffset int64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	last := &st.segments[len(st.segments)-1]
	if last.sealed {
		return
	}
	if writeOffset-last.startOffset < st.targetSize {
		return
	}
	last.endOffset = writeOffset
	last.sealed = true
	st.segments = append(st.segments, logSegment{
		id:          len(st.segments),
		startOffset: writeOffset,
		sealed:      false,
	})
}

func (st *segmentTracker) totalStaleBytes() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	var total int64
	for _, s := range st.segments {
		total += s.staleBytes
	}
	return total
}

func (st *segmentTracker) staleBytes(segIdx int) int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	if segIdx < 0 || segIdx >= len(st.segments) {
		return 0
	}
	return st.segments[segIdx].staleBytes
}

func (st *segmentTracker) deductStale(segIdx int, nbytes int64) {
	if nbytes <= 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if segIdx < 0 || segIdx >= len(st.segments) {
		return
	}
	st.segments[segIdx].staleBytes -= nbytes
	if st.segments[segIdx].staleBytes < 0 {
		st.segments[segIdx].staleBytes = 0
	}
}

func (st *segmentTracker) isSealed(segIdx int) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if segIdx < 0 || segIdx >= len(st.segments) {
		return false
	}
	return st.segments[segIdx].sealed
}

func (st *segmentTracker) bounds(segIdx int, writeOffset int64) (start, end int64, ok bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if segIdx < 0 || segIdx >= len(st.segments) {
		return 0, 0, false
	}
	seg := st.segments[segIdx]
	start = seg.startOffset
	if seg.sealed {
		end = seg.endOffset
	} else {
		end = writeOffset
	}
	return start, end, true
}

func (st *segmentTracker) segmentCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.segments)
}

func (st *segmentTracker) sealedCount() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	var n int
	for _, s := range st.segments {
		if s.sealed {
			n++
		}
	}
	return n
}
