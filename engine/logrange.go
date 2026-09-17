// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type logFrame struct {
	rec    Record
	length int64
}

// validateFrames ensures data is a concatenation of complete log frames.
// Partial frames are rejected so replication stays statement-safe.
func validateFrames(data []byte) ([]logFrame, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var frames []logFrame
	pos := 0
	for pos < len(data) {
		if pos+LogFrameHeaderSize > len(data) {
			return nil, ErrCorruptData
		}
		length := binary.BigEndian.Uint32(data[pos:])
		checksum := binary.BigEndian.Uint32(data[pos+4:])
		total := int(frameSize(int(length)))
		if pos+total > len(data) {
			return nil, ErrCorruptData
		}
		payload := data[pos+LogFrameHeaderSize : pos+LogFrameHeaderSize+int(length)]
		if crc32.Checksum(payload, Crc32Table) != checksum {
			return nil, ErrChecksum
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			return nil, err
		}
		frames = append(frames, logFrame{rec: rec, length: int64(total)})
		pos += total
	}
	return frames, nil
}

// ReadLogRange reads complete log frames from startOffset, returning at most
// maxBytes of raw on-disk frame bytes (headers included). The segment never
// splits a frame, so each boundary aligns to a statement (BEGIN/SET/DEL/COMMIT/ABORT).
func (l *DataLog) ReadLogRange(startOffset int64, maxBytes int64) ([]byte, int64, error) {
	if maxBytes <= 0 {
		return nil, startOffset, nil
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if startOffset >= l.writeOffset {
		return nil, startOffset, nil
	}

	startIdx := l.segmentIndexForLSN(startOffset)
	if startIdx < 0 {
		return nil, startOffset, ErrLogUnavailable
	}

	var out []byte
	pos := startOffset

	for i := startIdx; i < len(l.segments); i++ {
		seg := &l.segments[i]
		segEnd := seg.endLSN
		if segEnd == 0 {
			segEnd = l.writeOffset
		}
		if pos >= segEnd {
			continue
		}

		if _, err := os.Stat(seg.path); err != nil {
			if os.IsNotExist(err) {
				return nil, startOffset, ErrLogUnavailable
			}
			return nil, startOffset, err
		}

		fileSize := l.segmentScanLimit(seg, fileSizeOf(seg.path))
		localPos := pos - seg.baseLSN

		for localPos < fileSize && pos < segEnd {
			validEnd, _, span, rerr := l.readFrameAtSeg(seg, localPos, fileSize)
			if rerr != nil {
				if rerr == io.EOF {
					break
				}
				if os.IsNotExist(rerr) {
					return nil, startOffset, ErrLogUnavailable
				}
				return nil, startOffset, rerr
			}
			frameLen := span.length
			if len(out) > 0 && int64(len(out))+frameLen > maxBytes {
				return out, pos, nil
			}

			frame := make([]byte, frameLen)
			if err := l.readSegmentAt(seg, localPos, frame); err != nil {
				if os.IsNotExist(err) {
					return nil, startOffset, ErrLogUnavailable
				}
				return nil, startOffset, err
			}
			out = append(out, frame...)
			localPos = validEnd
			pos = seg.baseLSN + localPos
		}
	}

	return out, pos, nil
}

// AppendRawFrames appends a validated range of complete log frames.
func (l *DataLog) AppendRawFrames(data []byte, fsync bool) (int64, error) {
	frames, err := validateFrames(data)
	if err != nil {
		return 0, err
	}
	if len(frames) == 0 {
		return l.WriteOffset(), nil
	}

	l.mu.Lock()
	startOff := l.writeOffset
	pos := 0
	for _, fr := range frames {
		raw := data[pos : pos+int(fr.length)]
		pos += int(fr.length)
		if err := l.rotateIfNeededLocked(fr.length); err != nil {
			l.mu.Unlock()
			return 0, err
		}
		seg := &l.segments[l.activeIndex]
		localOff := l.writeOffset - seg.baseLSN
		n, err := seg.writer.WriteAt(raw, localOff)
		if err != nil {
			l.mu.Unlock()
			return 0, err
		}
		if int64(n) != fr.length {
			l.mu.Unlock()
			return 0, io.ErrShortWrite
		}
		l.writeOffset += fr.length
		if l.writeOffset-seg.baseLSN >= l.usableSegmentSize() {
			if err := l.rotateSegmentLocked(); err != nil {
				l.mu.Unlock()
				return 0, err
			}
		}
	}
	if fsync {
		l.strictSyncLocked()
	}
	l.mu.Unlock()
	return startOff, nil
}
