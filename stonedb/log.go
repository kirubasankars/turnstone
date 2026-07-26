// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// DataLog is a single append-only log file holding all records and values.
type DataLog struct {
	path        string
	file        *os.File
	writeOffset int64
	mu          sync.RWMutex
	logger      *slog.Logger
}

func OpenDataLog(dir string, logger *slog.Logger) (*DataLog, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &DataLog{
		path:        path,
		file:        f,
		writeOffset: stat.Size(),
		logger:      logger,
	}, nil
}

func (l *DataLog) WriteOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.writeOffset
}

func (l *DataLog) strictSync() {
	err := l.file.Sync()
	if err != nil {
		if errors.Is(err, syscall.EINTR) {
			l.strictSync()
			return
		}
		l.logger.Error("CRITICAL: fsync failed", "err", err)
		panic(fmt.Sprintf("CRITICAL STORAGE FAILURE: %v", err))
	}
}

// AppendRecords appends frames and returns the byte offset of each frame start.
func (l *DataLog) AppendRecords(builders []func() []byte, sync bool) ([]int64, error) {
	if len(builders) == 0 {
		return nil, nil
	}

	l.mu.Lock()
	offsets := make([]int64, len(builders))

	for i, build := range builders {
		payload := build()
		off := l.writeOffset
		if err := l.writeFrameLocked(payload); err != nil {
			l.mu.Unlock()
			return nil, err
		}
		offsets[i] = off
	}
	l.mu.Unlock()

	if sync {
		l.strictSync()
	}
	return offsets, nil
}

func (l *DataLog) AppendReplicatedRecord(payload []byte, sync bool) (int64, error) {
	l.mu.Lock()
	off := l.writeOffset
	if err := l.writeFrameLocked(payload); err != nil {
		l.mu.Unlock()
		return 0, err
	}
	l.mu.Unlock()

	if sync {
		l.strictSync()
	}
	return off, nil
}

func (l *DataLog) writeFrameLocked(payload []byte) error {
	length := uint32(len(payload))
	checksum := crc32.Checksum(payload, Crc32Table)
	totalLen := LogFrameHeaderSize + int(length)

	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:], length)
	binary.BigEndian.PutUint32(buf[4:], checksum)
	copy(buf[8:], payload)

	n, err := l.file.WriteAt(buf, l.writeOffset)
	if err != nil {
		return err
	}
	l.writeOffset += int64(n)
	return nil
}

func (l *DataLog) ReadValueAt(offset int64, valLen uint32) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	header := make([]byte, LogFrameHeaderSize)
	if _, err := l.file.ReadAt(header, offset); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header[0:])
	payload := make([]byte, payloadLen)
	if _, err := l.file.ReadAt(payload, offset+LogFrameHeaderSize); err != nil {
		return nil, err
	}

	rec, err := decodeWALRecord(payload)
	if err != nil {
		return nil, err
	}
	if len(rec.Value) != int(valLen) {
		return nil, fmt.Errorf("value length mismatch at offset %d", offset)
	}
	return append([]byte(nil), rec.Value...), nil
}

const replayCancelCheckInterval = 1024

// Replay scans the entire log, invoking onRecord for each valid frame.
func (l *DataLog) Replay(ctx context.Context, truncateCorrupt bool, onRecord func(rec WALRecord, span recordSpan)) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}

	pos, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	var records uint64

	for pos < fileSize {
		if records%replayCancelCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		next, err := f.Seek(pos, seekData)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if next >= fileSize {
			break
		}
		pos = next

		validEnd, rec, span, rerr := l.readFrameAt(f, pos, fileSize)
		if rerr != nil {
			if (rerr == io.ErrUnexpectedEOF || rerr == ErrChecksum || rerr == ErrCorruptData) && truncateCorrupt {
				l.logger.Warn("Truncating corrupt log tail", "offset", pos, "err", rerr)
				if err := os.Truncate(l.path, pos); err != nil {
					return err
				}
				l.writeOffset = pos
				return ErrTruncated
			}
			if rerr == io.EOF {
				break
			}
			return fmt.Errorf("log corruption at offset %d: %w", pos, rerr)
		}

		if onRecord != nil {
			onRecord(rec, span)
		}
		records++
		pos = validEnd
	}
	return nil
}

func (l *DataLog) readFrameAt(f *os.File, offset, fileSize int64) (int64, WALRecord, recordSpan, error) {
	if offset+LogFrameHeaderSize > fileSize {
		return offset, WALRecord{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	header := make([]byte, LogFrameHeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		return offset, WALRecord{}, recordSpan{}, err
	}
	length := binary.BigEndian.Uint32(header[0:])
	checksum := binary.BigEndian.Uint32(header[4:])
	if length > 1<<30 {
		return offset, WALRecord{}, recordSpan{}, ErrCorruptData
	}
	total := frameSize(int(length))
	if offset+total > fileSize {
		return offset, WALRecord{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	payload := make([]byte, length)
	if _, err := f.ReadAt(payload, offset+LogFrameHeaderSize); err != nil {
		return offset, WALRecord{}, recordSpan{}, err
	}
	if crc32.Checksum(payload, Crc32Table) != checksum {
		return offset, WALRecord{}, recordSpan{}, ErrChecksum
	}
	rec, err := decodeWALRecord(payload)
	if err != nil {
		return offset, WALRecord{}, recordSpan{}, err
	}
	span := recordSpan{
		offset: offset,
		length: total,
	}
	return offset + total, rec, span, nil
}

// Scan streams records from startOffset onward (inclusive frame boundary).
func (l *DataLog) Scan(startOffset int64, fn func([]WALRecord) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	if startOffset >= fileSize {
		return nil
	}

	pos := startOffset
	for pos < fileSize {
		next, err := f.Seek(pos, seekData)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if next >= fileSize {
			break
		}
		pos = next

		validEnd, rec, _, rerr := l.readFrameAt(f, pos, fileSize)
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return rerr
		}
		if err := fn([]WALRecord{rec}); err != nil {
			return err
		}
		pos = validEnd
	}
	return nil
}

// IsFrameBoundary reports whether offset is a valid replication cursor:
// 0, the exclusive end of the log (WriteOffset), or the start of a complete frame.
func (l *DataLog) IsFrameBoundary(offset int64) bool {
	if offset == 0 {
		return true
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	if offset == l.writeOffset {
		return true
	}
	stat, err := l.file.Stat()
	if err != nil || offset < 0 || offset >= stat.Size() {
		return false
	}
	_, _, _, err = l.readFrameAt(l.file, offset, stat.Size())
	return err == nil
}

func (l *DataLog) LogicalSize() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	stat, err := l.file.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
}

func (l *DataLog) AllocatedSize() int64 {
	var st syscall.Stat_t
	if err := syscall.Stat(l.path, &st); err != nil {
		return 0
	}
	return st.Blocks * 512
}

func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
