// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"os"
)

// ReadFrameBytesAt returns the on-disk frame bytes (header + payload) at global LSN.
func (l *DataLog) ReadFrameBytesAt(lsn int64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.readFrameBytesAtLocked(lsn)
}

func (l *DataLog) readFrameBytesAtLocked(lsn int64) ([]byte, error) {
	seg, local, ok := l.resolveLSN(lsn)
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrInvalidLogOffset, lsn)
	}
	_, _, span, err := l.readFrameAtSeg(seg, local, l.segmentScanLimit(seg, fileSizeOf(seg.path)))
	if err != nil {
		return nil, err
	}
	frame := make([]byte, span.length)
	if err := l.readSegmentAt(seg, local, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// AllocatedBytesOnDisk returns the total on-disk size of all WAL segment files.
func (l *DataLog) AllocatedBytesOnDisk() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.allocatedBytesLocked()
}

func (l *DataLog) allocatedBytesLocked() int64 {
	var total int64
	for _, seg := range l.segments {
		info, err := os.Stat(seg.path)
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

type walCopyForwardOutcome struct {
	remap          map[int64]int64
	headBefore     int64
	bytesBefore    int64
	bytesAfter     int64
	segmentsPurged int
}

// appendCopyForwardFrames appends frame copies to a fresh active segment and
// returns the old→new offset map. Segment purge is deferred until after the
// index is remapped (see MaybeCopyForwardWal).
func (l *DataLog) appendCopyForwardFrames(oldOffsets []int64, frames [][]byte) (walCopyForwardOutcome, error) {
	if len(oldOffsets) != len(frames) {
		return walCopyForwardOutcome{}, fmt.Errorf("wal copy-forward: offset/frame count mismatch")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	out := walCopyForwardOutcome{
		remap:      make(map[int64]int64, len(oldOffsets)),
		headBefore: l.writeOffset,
	}
	out.bytesBefore = l.allocatedBytesLocked()

	if err := l.rotateSegmentLocked(); err != nil {
		return walCopyForwardOutcome{}, err
	}

	for i, frame := range frames {
		off := l.writeOffset
		seg := &l.segments[l.activeIndex]
		localOff := off - seg.baseLSN
		n, err := seg.writer.WriteAt(frame, localOff)
		if err != nil {
			return walCopyForwardOutcome{}, err
		}
		if int64(n) != int64(len(frame)) {
			return walCopyForwardOutcome{}, fmt.Errorf("wal copy-forward: short write")
		}
		l.buffers.applyWrite(seg.id, localOff, frame[:n])
		l.writeOffset += int64(n)
		out.remap[oldOffsets[i]] = off

		if l.writeOffset-seg.baseLSN >= l.usableSegmentSize() {
			if err := l.rotateSegmentLocked(); err != nil {
				return walCopyForwardOutcome{}, err
			}
		}
	}

	out.bytesAfter = l.allocatedBytesLocked()
	if err := l.persistManifestLocked(); err != nil {
		return walCopyForwardOutcome{}, err
	}
	return out, nil
}

// copyForwardSegmentDeleteThrough computes how far sealed segments may be purged
// after a copy-forward pass. Replication scan floor caps deletion; standalone DBs
// with no floor delete through headBefore.
func copyForwardSegmentDeleteThrough(minDeletableLSN, headBefore int64, scanFloor int64) int64 {
	deleteThrough := minDeletableLSN
	if deleteThrough <= 0 {
		if scanFloor > 0 {
			deleteThrough = scanFloor
		} else {
			return headBefore
		}
	}
	if deleteThrough > headBefore {
		deleteThrough = headBefore
	}
	return deleteThrough
}

// deleteSegmentsThroughLocked removes sealed segments whose exclusive end LSN is
// at or below maxEndLSN. The active segment is never deleted. Caller must hold l.mu.
func (l *DataLog) deleteSegmentsThroughLocked(maxEndLSN int64) (int, int64, error) {
	if maxEndLSN <= 0 {
		return 0, 0, nil
	}

	var kept []walSegment
	var doomed []walSegment
	var reclaimed int64

	for i, seg := range l.segments {
		if i == l.activeIndex {
			kept = append(kept, seg)
			continue
		}
		end := seg.endLSN
		if end == 0 {
			end = seg.baseLSN + l.logicalSizeFromFile(seg.path, fileSizeOf(seg.path))
		}
		if end > maxEndLSN {
			kept = append(kept, seg)
			continue
		}

		info, err := os.Stat(seg.path)
		if err != nil && !os.IsNotExist(err) {
			return 0, 0, err
		}
		if err == nil {
			reclaimed += info.Size()
		}
		doomed = append(doomed, l.segments[i])
	}

	if len(doomed) == 0 {
		return 0, 0, nil
	}

	oldSegs := l.segments
	oldActive := l.activeIndex
	l.segments = kept
	l.activeIndex = -1
	for i, seg := range l.segments {
		if seg.writer != nil {
			l.activeIndex = i
			break
		}
	}
	if l.activeIndex < 0 {
		l.segments = oldSegs
		l.activeIndex = oldActive
		return 0, 0, fmt.Errorf("wal: no active segment after delete")
	}
	if err := l.persistManifestLocked(); err != nil {
		l.segments = oldSegs
		l.activeIndex = oldActive
		return 0, 0, err
	}

	var firstErr error
	deleted := 0
	for i := range doomed {
		seg := &doomed[i]
		if l.buffers != nil {
			l.buffers.invalidateSegment(seg.id)
		}
		closeSegmentFiles(seg)
		if _, err := l.recycleOrRemove(seg.path); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		deleted++
	}
	return deleted, reclaimed, firstErr
}
