// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type logFrame struct {
	rec    WALRecord
	length int64
}

// validateLogSegment ensures data is a concatenation of complete WAL frames.
// Partial frames are rejected so replication stays statement-safe.
func validateLogSegment(data []byte) ([]logFrame, error) {
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
		rec, err := decodeWALRecord(payload)
		if err != nil {
			return nil, err
		}
		frames = append(frames, logFrame{rec: rec, length: int64(total)})
		pos += total
	}
	return frames, nil
}

// ReadLogSegment reads complete WAL frames from startOffset, returning at most
// maxBytes of raw on-disk frame bytes (headers included). The segment never
// splits a frame, so each boundary aligns to a statement (BEGIN/SET/DEL/COMMIT/ABORT).
func (l *DataLog) ReadLogSegment(startOffset int64, maxBytes int64) ([]byte, int64, error) {
	if maxBytes <= 0 {
		return nil, startOffset, nil
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	f, err := os.Open(l.path)
	if err != nil {
		return nil, startOffset, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, startOffset, err
	}
	fileSize := stat.Size()
	if startOffset >= fileSize {
		return nil, startOffset, nil
	}

	var out []byte
	pos := startOffset

	for pos < fileSize {
		validEnd, _, span, rerr := l.readFrameAt(f, pos, fileSize)
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return nil, startOffset, rerr
		}
		frameLen := span.length
		if len(out) > 0 && int64(len(out))+frameLen > maxBytes {
			break
		}

		frame := make([]byte, frameLen)
		if _, err := f.ReadAt(frame, pos); err != nil {
			return nil, startOffset, err
		}
		out = append(out, frame...)
		pos = validEnd
	}

	return out, pos, nil
}

// AppendRawSegment appends a validated segment of complete WAL frames.
func (l *DataLog) AppendRawSegment(data []byte, fsync bool) (int64, error) {
	frames, err := validateLogSegment(data)
	if err != nil {
		return 0, err
	}
	if len(frames) == 0 {
		return l.WriteOffset(), nil
	}

	l.mu.Lock()
	off := l.writeOffset
	n, err := l.file.WriteAt(data, off)
	if err != nil {
		l.mu.Unlock()
		return 0, err
	}
	if int64(n) != int64(len(data)) {
		l.mu.Unlock()
		return 0, io.ErrShortWrite
	}
	l.writeOffset += int64(n)
	l.mu.Unlock()

	if fsync {
		l.strictSync()
	}
	return off, nil
}
