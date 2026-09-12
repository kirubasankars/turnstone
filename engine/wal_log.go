// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

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

type walSegment struct {
	id      uint32
	file    string
	path    string
	baseLSN int64
	endLSN  int64 // exclusive global end; 0 means active/growing
	writer  *os.File
}

// DataLog is a segmented append-only WAL addressed by a global byte LSN.
type DataLog struct {
	dir          string
	walDir       string
	manifestPath string
	mu           sync.RWMutex
	logger       *slog.Logger
	segmentSize  int64
	segments     []walSegment
	activeIndex  int
	writeOffset  int64
}

func OpenDataLog(dir string, logger *slog.Logger, segmentSize int64) (*DataLog, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	segmentSize = normalizeWalSegmentSize(segmentSize)

	walDir := filepath.Join(dir, walDirName)
	manifestPath := filepath.Join(walDir, walManifestName)

	var (
		manifest *walManifest
		err      error
	)
	if _, err = os.Stat(manifestPath); err == nil {
		manifest, err = loadWalManifest(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("load wal manifest: %w", err)
		}
	} else if os.IsNotExist(err) {
		legacyPath := filepath.Join(dir, logFileName)
		if _, statErr := os.Stat(legacyPath); statErr == nil {
			manifest, err = migrateLegacyDataLog(dir, walDir, segmentSize)
			if err != nil {
				return nil, fmt.Errorf("migrate legacy log: %w", err)
			}
		} else if os.IsNotExist(statErr) {
			manifest, err = createFreshWalManifest(walDir, segmentSize)
			if err != nil {
				return nil, fmt.Errorf("create wal: %w", err)
			}
		} else {
			return nil, statErr
		}
	} else {
		return nil, err
	}

	if manifest.SegmentSize <= 0 {
		manifest.SegmentSize = segmentSize
	}

	l := &DataLog{
		dir:          dir,
		walDir:       walDir,
		manifestPath: manifestPath,
		logger:       logger,
		segmentSize:  manifest.SegmentSize,
	}
	if err := l.loadSegments(manifest); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DataLog) loadSegments(m *walManifest) error {
	l.segments = make([]walSegment, len(m.Segments))
	activeIdx := -1
	var head int64

	for i, seg := range m.Segments {
		path := filepath.Join(l.walDir, seg.File)
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("wal segment %s: %w", seg.File, err)
		}
		endLSN := seg.EndLSN
		if seg.ID == m.ActiveID {
			activeIdx = i
			endLSN = seg.BaseLSN + info.Size()
		} else if endLSN == 0 {
			endLSN = seg.BaseLSN + info.Size()
		}
		l.segments[i] = walSegment{
			id:      seg.ID,
			file:    seg.File,
			path:    path,
			baseLSN: seg.BaseLSN,
			endLSN:  endLSN,
		}
		if endLSN > head {
			head = endLSN
		}
	}
	if activeIdx < 0 {
		return fmt.Errorf("wal manifest active segment %d not found", m.ActiveID)
	}
	f, err := os.OpenFile(l.segments[activeIdx].path, os.O_RDWR, fileMode)
	if err != nil {
		return err
	}
	l.segments[activeIdx].writer = f
	l.segments[activeIdx].endLSN = 0
	l.activeIndex = activeIdx
	l.writeOffset = head
	return nil
}

func (l *DataLog) manifestSnapshot() *walManifest {
	m := &walManifest{
		Version:     walManifestVersion,
		SegmentSize: l.segmentSize,
		ActiveID:    l.segments[l.activeIndex].id,
		Segments:    make([]walManifestSegment, len(l.segments)),
	}
	for i, seg := range l.segments {
		entry := walManifestSegment{
			ID:      seg.id,
			File:    seg.file,
			BaseLSN: seg.baseLSN,
		}
		if i != l.activeIndex && seg.endLSN > seg.baseLSN {
			entry.EndLSN = seg.endLSN
		}
		m.Segments[i] = entry
	}
	return m
}

func (l *DataLog) persistManifestLocked() error {
	return saveWalManifest(l.manifestPath, l.manifestSnapshot())
}

func (l *DataLog) resolveLSN(lsn int64) (*walSegment, int64, bool) {
	if lsn < 0 || lsn >= l.writeOffset {
		return nil, 0, false
	}
	for i := len(l.segments) - 1; i >= 0; i-- {
		seg := &l.segments[i]
		if lsn < seg.baseLSN {
			continue
		}
		end := seg.endLSN
		if end == 0 {
			end = l.writeOffset
		}
		if lsn >= end {
			return nil, 0, false
		}
		return seg, lsn - seg.baseLSN, true
	}
	return nil, 0, false
}

func (l *DataLog) segmentIndexForLSN(lsn int64) int {
	for i, seg := range l.segments {
		end := seg.endLSN
		if end == 0 {
			end = l.writeOffset
		}
		if lsn >= seg.baseLSN && lsn < end {
			return i
		}
	}
	return -1
}

func (l *DataLog) WriteOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.writeOffset
}

func (l *DataLog) strictSyncLocked() {
	seg := &l.segments[l.activeIndex]
	if seg.writer == nil {
		return
	}
	err := seg.writer.Sync()
	if err != nil {
		if errors.Is(err, syscall.EINTR) {
			l.strictSyncLocked()
			return
		}
		l.logger.Error("CRITICAL: fsync failed", "err", err)
		panic(fmt.Sprintf("CRITICAL STORAGE FAILURE: %v", err))
	}
}

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
	if sync {
		l.strictSyncLocked()
	}
	l.mu.Unlock()
	return offsets, nil
}

func (l *DataLog) AppendEncoded(payload []byte, sync bool) (int64, error) {
	l.mu.Lock()
	off := l.writeOffset
	if err := l.writeFrameLocked(payload); err != nil {
		l.mu.Unlock()
		return 0, err
	}
	if sync {
		l.strictSyncLocked()
	}
	l.mu.Unlock()
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

	seg := &l.segments[l.activeIndex]
	localOff := l.writeOffset - seg.baseLSN
	n, err := seg.writer.WriteAt(buf, localOff)
	if err != nil {
		return err
	}
	l.writeOffset += int64(n)

	if l.writeOffset-seg.baseLSN >= l.segmentSize {
		return l.rotateSegmentLocked()
	}
	return nil
}

func (l *DataLog) rotateSegmentLocked() error {
	active := &l.segments[l.activeIndex]
	if err := active.writer.Sync(); err != nil {
		return err
	}
	active.endLSN = l.writeOffset
	if err := active.writer.Close(); err != nil {
		return err
	}
	active.writer = nil

	nextID := active.id + 1
	nextName := walSegmentFileName(nextID)
	nextPath := filepath.Join(l.walDir, nextName)
	f, err := os.OpenFile(nextPath, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, walSegment{
		id:      nextID,
		file:    nextName,
		path:    nextPath,
		baseLSN: l.writeOffset,
		writer:  f,
	})
	l.activeIndex = len(l.segments) - 1
	return l.persistManifestLocked()
}

func (l *DataLog) ReadValueAt(offset int64, valLen uint32) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	seg, local, ok := l.resolveLSN(offset)
	if !ok {
		return nil, fmt.Errorf("invalid log offset %d", offset)
	}
	header := make([]byte, LogFrameHeaderSize)
	if err := l.readAtSegmentFile(seg.path, header, local); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header[0:])
	payload := make([]byte, payloadLen)
	if err := l.readAtSegmentFile(seg.path, payload, local+LogFrameHeaderSize); err != nil {
		return nil, err
	}

	rec, err := decodeRecord(payload)
	if err != nil {
		return nil, err
	}
	if len(rec.Value) != int(valLen) {
		return nil, fmt.Errorf("value length mismatch at offset %d", offset)
	}
	return append([]byte(nil), rec.Value...), nil
}

func (l *DataLog) readAtSegmentFile(path string, buf []byte, off int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.ReadAt(buf, off)
	return err
}

const replayCancelCheckInterval = 1024

func (l *DataLog) Replay(ctx context.Context, truncateCorrupt bool, onRecord func(rec Record, span recordSpan)) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var records uint64
	for i, seg := range l.segments {
		isActive := i == l.activeIndex
		if err := l.replaySegmentFile(ctx, &seg, isActive, truncateCorrupt, &records, onRecord); err != nil {
			if errors.Is(err, ErrTruncated) && isActive {
				stat, statErr := seg.writer.Stat()
				if statErr == nil {
					l.writeOffset = seg.baseLSN + stat.Size()
				}
				if persistErr := l.persistManifestLocked(); persistErr != nil {
					return persistErr
				}
			}
			return err
		}
	}
	return nil
}

func (l *DataLog) replaySegmentFile(ctx context.Context, seg *walSegment, isActive bool, truncateCorrupt bool, records *uint64, onRecord func(rec Record, span recordSpan)) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		return nil
	}

	pos := int64(0)
	for pos < fileSize {
		if *records%replayCancelCheckInterval == 0 {
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

		validEnd, rec, span, rerr := readFrameAtFile(f, pos, fileSize)
		if rerr != nil {
			if (rerr == io.ErrUnexpectedEOF || rerr == ErrChecksum || rerr == ErrCorruptData) && truncateCorrupt && isActive {
				l.logger.Warn("Truncating corrupt wal tail", "segment", seg.file, "offset", seg.baseLSN+pos, "err", rerr)
				if err := os.Truncate(seg.path, pos); err != nil {
					return err
				}
				if seg.writer != nil {
					_ = seg.writer.Truncate(pos)
				}
				l.writeOffset = seg.baseLSN + pos
				return ErrTruncated
			}
			if rerr == io.EOF {
				break
			}
			return fmt.Errorf("log corruption at offset %d: %w", seg.baseLSN+pos, rerr)
		}
		span.offset = seg.baseLSN + span.offset
		if onRecord != nil {
			onRecord(rec, span)
		}
		*records++
		pos = validEnd
	}
	return nil
}

func readFrameAtFile(f *os.File, offset, fileSize int64) (int64, Record, recordSpan, error) {
	if offset+LogFrameHeaderSize > fileSize {
		return offset, Record{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	header := make([]byte, LogFrameHeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		return offset, Record{}, recordSpan{}, err
	}
	length := binary.BigEndian.Uint32(header[0:])
	checksum := binary.BigEndian.Uint32(header[4:])
	if length > 1<<30 {
		return offset, Record{}, recordSpan{}, ErrCorruptData
	}
	total := frameSize(int(length))
	if offset+total > fileSize {
		return offset, Record{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	payload := make([]byte, length)
	if _, err := f.ReadAt(payload, offset+LogFrameHeaderSize); err != nil {
		return offset, Record{}, recordSpan{}, err
	}
	if crc32.Checksum(payload, Crc32Table) != checksum {
		return offset, Record{}, recordSpan{}, ErrChecksum
	}
	rec, err := decodeRecord(payload)
	if err != nil {
		return offset, Record{}, recordSpan{}, err
	}
	span := recordSpan{
		offset: offset,
		length: total,
	}
	return offset + total, rec, span, nil
}

func (l *DataLog) Scan(startOffset int64, fn func([]Record) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	startIdx := l.segmentIndexForLSN(startOffset)
	if startOffset == l.writeOffset {
		return nil
	}
	if startIdx < 0 {
		if startOffset == 0 && len(l.segments) > 0 {
			startIdx = 0
		} else {
			return fmt.Errorf("invalid scan offset %d", startOffset)
		}
	}

	for i := startIdx; i < len(l.segments); i++ {
		seg := &l.segments[i]
		localStart := int64(0)
		if i == startIdx {
			localStart = startOffset - seg.baseLSN
		}
		end := seg.endLSN
		if end == 0 {
			end = l.writeOffset
		}
		if err := l.scanSegmentFile(seg, localStart, end-seg.baseLSN, fn); err != nil {
			return err
		}
	}
	return nil
}

func (l *DataLog) scanSegmentFile(seg *walSegment, localStart, localEnd int64, fn func([]Record) error) error {
	f, err := os.Open(seg.path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	if localEnd > fileSize {
		localEnd = fileSize
	}
	if localStart >= localEnd {
		return nil
	}

	pos := localStart
	for pos < localEnd {
		next, err := f.Seek(pos, seekData)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if next >= localEnd {
			break
		}
		pos = next

		validEnd, rec, _, rerr := readFrameAtFile(f, pos, fileSize)
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return rerr
		}
		if err := fn([]Record{rec}); err != nil {
			return err
		}
		pos = validEnd
	}
	return nil
}

func (l *DataLog) IsFrameBoundary(offset int64) bool {
	if offset == 0 {
		return true
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if offset == l.writeOffset {
		return true
	}
	seg, local, ok := l.resolveLSN(offset)
	if !ok {
		return false
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return false
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	_, _, _, err = readFrameAtFile(f, local, stat.Size())
	return err == nil
}

func (l *DataLog) LogicalSize() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.writeOffset
}

func (l *DataLog) AllocatedSize() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var total int64
	for _, seg := range l.segments {
		var st syscall.Stat_t
		if err := syscall.Stat(seg.path, &st); err != nil {
			continue
		}
		total += st.Blocks * 512
	}
	return total
}

func (l *DataLog) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for i := range l.segments {
		if l.segments[i].writer != nil {
			if err := l.segments[i].writer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			l.segments[i].writer = nil
		}
	}
	return firstErr
}

// closeActiveFileForTest simulates a crash by closing the active segment writer.
func (l *DataLog) closeActiveFileForTest() {
	l.mu.Lock()
	defer l.mu.Unlock()
	seg := &l.segments[l.activeIndex]
	if seg.writer != nil {
		_ = seg.writer.Close()
		seg.writer = nil
	}
}

// activeSegmentPath returns the path to the active wal segment (for tests).
func (l *DataLog) activeSegmentPath() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[l.activeIndex].path
}
