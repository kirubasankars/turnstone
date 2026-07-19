// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"math"
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
		return nil, fmt.Errorf("invalid log offset %d", lsn)
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	_, _, span, err := readFrameAtFile(f, local, stat.Size())
	if err != nil {
		return nil, err
	}
	frame := make([]byte, span.length)
	if _, err := f.ReadAt(frame, local); err != nil {
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

// copyForwardLiveFrames appends frame copies to a fresh active segment, remaps
// old global offsets to new ones, and deletes sealed segments at or below
// maxDeleteThrough (typically scan floor / replication retain horizon).
func (l *DataLog) copyForwardLiveFrames(oldOffsets []int64, frames [][]byte, maxDeleteThrough int64) (walCopyForwardOutcome, error) {
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
		l.writeOffset += int64(n)
		out.remap[oldOffsets[i]] = off

		if l.writeOffset-seg.baseLSN >= l.segmentSize {
			if err := l.rotateSegmentLocked(); err != nil {
				return walCopyForwardOutcome{}, err
			}
		}
	}

	segDeleteThrough := maxDeleteThrough
	if segDeleteThrough <= 0 || segDeleteThrough == math.MaxInt64 {
		segDeleteThrough = out.headBefore
	} else if segDeleteThrough > out.headBefore {
		segDeleteThrough = out.headBefore
	}
	deleted, _, err := l.deleteSegmentsThroughLocked(segDeleteThrough)
	if err != nil {
		return walCopyForwardOutcome{}, err
	}
	out.segmentsPurged = deleted
	out.bytesAfter = l.allocatedBytesLocked()

	if err := l.persistManifestLocked(); err != nil {
		return walCopyForwardOutcome{}, err
	}
	return out, nil
}

// deleteSegmentsThroughLocked is deleteSegmentsThrough with mu already held.
func (l *DataLog) deleteSegmentsThroughLocked(maxEndLSN int64) (int, int64, error) {
	if maxEndLSN <= 0 {
		return 0, 0, nil
	}

	var kept []walSegment
	var deleted int
	var reclaimed int64

	for i, seg := range l.segments {
		if i == l.activeIndex {
			kept = append(kept, seg)
			continue
		}
		end := seg.endLSN
		if end == 0 {
			info, err := os.Stat(seg.path)
			if err != nil {
				return deleted, reclaimed, err
			}
			end = seg.baseLSN + info.Size()
		}
		if end > maxEndLSN {
			kept = append(kept, seg)
			continue
		}

		info, err := os.Stat(seg.path)
		if err != nil && !os.IsNotExist(err) {
			return deleted, reclaimed, err
		}
		if err == nil {
			reclaimed += info.Size()
		}
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			return deleted, reclaimed, err
		}
		deleted++
	}

	if deleted == 0 {
		return 0, 0, nil
	}

	l.segments = kept
	l.activeIndex = -1
	for i, seg := range l.segments {
		if seg.writer != nil {
			l.activeIndex = i
			break
		}
	}
	if l.activeIndex < 0 {
		return deleted, reclaimed, fmt.Errorf("wal: no active segment after delete")
	}
	return deleted, reclaimed, nil
}
