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
	"sync/atomic"
	"syscall"
)

type walSegment struct {
	id        uint32
	file      string
	path      string
	baseLSN   int64
	endLSN    int64 // exclusive global end; 0 means active/growing
	writer    *os.File
	reader    *os.File // sealed segments keep an FD so GET does not open/close per read
	mapping   []byte   // MAP_SHARED mmap of the preallocated file; nil if unavailable
	allocated bool     // file is segment-sized and has a TSF1 footer slot
}

// DataLog is a segmented append-only WAL addressed by a global byte LSN.
// Segment rotation does not remap existing index or replication offsets.
type DataLog struct {
	dir           string
	walDir        string
	manifestPath  string
	mu            sync.RWMutex
	inflightSyncs sync.WaitGroup
	logger        *slog.Logger
	segmentSize   int64
	segments      []walSegment
	activeIndex   int
	writeOffset   int64
	durableOffset int64 // last LSN known durable; updated without holding mu
	syncing       int32 // in-flight fdatasyncs that released mu
	recycle       []string
	recycleSeq    uint32
	buffers       *sharedBuffers
}

// testingBeforeSync, when set, runs after the WAL insert lock is released and
// before fdatasync. Tests use it to prove SET can append during COMMIT's flush.
var testingBeforeSync func()

// OpenDataLog opens or creates wal/manifest.json and segment files under dir/wal/.
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

	manifest, err := loadWalManifest(manifestPath)
	if os.IsNotExist(err) {
		manifest, err = createFreshWalManifest(walDir, segmentSize)
		if err != nil {
			return nil, fmt.Errorf("create wal: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("load wal manifest: %w", err)
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
	atomic.StoreInt64(&l.durableOffset, l.writeOffset)
	if err := l.loadRecyclePool(); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("wal recycle pool: %w", err)
	}
	return l, nil
}

func (l *DataLog) loadSegments(m *walManifest) error {
	l.segments = make([]walSegment, len(m.Segments))
	activeIdx := -1

	for i, seg := range m.Segments {
		path := filepath.Join(l.walDir, seg.File)
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("wal segment %s: %w", seg.File, err)
		}
		endLSN := seg.EndLSN
		if seg.ID == m.ActiveID {
			activeIdx = i
			endLSN = 0
		} else if endLSN == 0 {
			endLSN = seg.BaseLSN + l.logicalSizeFromFile(path, info.Size())
		}
		l.segments[i] = walSegment{
			id:        seg.ID,
			file:      seg.File,
			path:      path,
			baseLSN:   seg.BaseLSN,
			endLSN:    endLSN,
			allocated: info.Size() >= l.segmentSize,
		}
		if seg.ID != m.ActiveID {
			if rf, openErr := os.Open(path); openErr == nil {
				l.segments[i].reader = rf
				l.mapSegment(&l.segments[i])
			}
		}
	}
	if activeIdx < 0 {
		return fmt.Errorf("wal manifest active segment %d not found", m.ActiveID)
	}
	f, err := os.OpenFile(l.segments[activeIdx].path, os.O_RDWR, fileMode)
	if err != nil {
		return err
	}
	if err := l.ensureAllocated(f); err != nil {
		_ = f.Close()
		return err
	}
	used, ok := readSegmentFooter(f, l.segmentSize)
	if !ok {
		used = l.logicalSizeFromFile(l.segments[activeIdx].path, fileSizeOf(l.segments[activeIdx].path))
	}
	l.segments[activeIdx].writer = f
	l.segments[activeIdx].endLSN = 0
	l.segments[activeIdx].allocated = l.fileAllocated(f)
	l.mapSegment(&l.segments[activeIdx])
	l.activeIndex = activeIdx
	l.writeOffset = l.segments[activeIdx].baseLSN + used
	return nil
}

func (l *DataLog) fileAllocated(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	return err == nil && info.Size() >= l.segmentSize
}

func (l *DataLog) mapSegment(seg *walSegment) {
	if seg == nil || len(seg.mapping) > 0 {
		return
	}
	f := seg.writer
	if f == nil {
		f = seg.reader
	}
	if f == nil {
		return
	}
	if info, err := f.Stat(); err != nil || info.Size() < l.segmentSize {
		// Short/grow-mode files are not mapped: a MAP_SHARED of segmentSize
		// would SIGBUS on reads past EOF.
		return
	}
	mapped, err := mmapWALFile(f, l.segmentSize)
	if err != nil {
		l.logger.Warn("wal mmap failed", "segment", seg.file, "err", err)
		return
	}
	seg.mapping = mapped
}

func (l *DataLog) EnableSharedBuffers(bytes int64) {
	if bytes < 0 {
		l.buffers = nil
		return
	}
	if bytes == 0 {
		bytes = defaultSharedBuffersBytes
	}
	l.buffers = newSharedBuffers(bytes)
}

func fileSizeOf(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func (l *DataLog) logicalSizeFromFile(path string, size int64) int64 {
	if size >= l.segmentSize {
		f, err := os.Open(path)
		if err != nil {
			return 0
		}
		used, ok := readSegmentFooter(f, l.segmentSize)
		_ = f.Close()
		if ok {
			return used
		}
	}
	if size > l.usableSegmentSize() {
		return l.usableSegmentSize()
	}
	return size
}

func (l *DataLog) ensureAllocated(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= l.segmentSize {
		return nil
	}
	if err := preallocateFile(f, l.segmentSize); err != nil {
		if isNoSpace(err) {
			return nil
		}
		return err
	}
	return writeSegmentFooter(f, l.segmentSize, info.Size())
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

// DurableOffset is the exclusive end of WAL known to have been fdatasync'd.
// ReadLogRange and sync-replication quorum must not go past this; writeOffset
// can race ahead while a flush is in flight.
func (l *DataLog) DurableOffset() int64 {
	return atomic.LoadInt64(&l.durableOffset)
}

func (l *DataLog) publishDurable(off int64) {
	for {
		cur := atomic.LoadInt64(&l.durableOffset)
		if off <= cur {
			return
		}
		if atomic.CompareAndSwapInt64(&l.durableOffset, cur, off) {
			return
		}
	}
}

func (l *DataLog) persistHeadLocked() error {
	seg := &l.segments[l.activeIndex]
	if seg.writer == nil || !seg.allocated {
		return nil
	}
	return writeSegmentFooter(seg.writer, l.segmentSize, l.writeOffset-seg.baseLSN)
}

// beginSyncLocked records the write head and returns the active writer for
// fdatasync outside l.mu. The caller must invoke completeSync.
// completeSync must not take l.mu: rotate waits for inflightSyncs while
// holding that lock.
func (l *DataLog) beginSyncLocked() (*os.File, int64, error) {
	if err := l.persistHeadLocked(); err != nil {
		return nil, 0, err
	}
	flushed := l.writeOffset
	f := l.segments[l.activeIndex].writer
	if f == nil {
		return nil, flushed, nil
	}
	atomic.AddInt32(&l.syncing, 1)
	l.inflightSyncs.Add(1)
	return f, flushed, nil
}

func (l *DataLog) completeSync(f *os.File, flushed int64) {
	if f == nil {
		l.publishDurable(flushed)
		return
	}
	defer l.inflightSyncs.Done()
	defer atomic.AddInt32(&l.syncing, -1)
	if hook := testingBeforeSync; hook != nil {
		hook()
	}
	l.syncWriter(f)
	// Publish only the snapshotted head. Concurrent WriteAts during
	// fdatasync are not guaranteed durable when this returns.
	l.publishDurable(flushed)
}

func (l *DataLog) publishAppendLocked() {
	if atomic.LoadInt32(&l.syncing) != 0 {
		return
	}
	l.publishDurable(l.writeOffset)
}

func (l *DataLog) syncWriter(f *os.File) {
	if f == nil {
		return
	}
	err := syncFile(f)
	if err != nil {
		if errors.Is(err, syscall.EINTR) {
			l.syncWriter(f)
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
	var syncF *os.File
	var flushed int64
	if sync {
		var err error
		syncF, flushed, err = l.beginSyncLocked()
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
	} else {
		l.publishAppendLocked()
	}
	l.mu.Unlock()
	if sync {
		l.completeSync(syncF, flushed)
	}
	return offsets, nil
}

func (l *DataLog) AppendEncoded(payload []byte, sync bool) (int64, error) {
	l.mu.Lock()
	off := l.writeOffset
	if err := l.writeFrameLocked(payload); err != nil {
		l.mu.Unlock()
		return 0, err
	}
	var syncF *os.File
	var flushed int64
	if sync {
		var err error
		syncF, flushed, err = l.beginSyncLocked()
		if err != nil {
			l.mu.Unlock()
			return 0, err
		}
	} else {
		l.publishAppendLocked()
	}
	l.mu.Unlock()
	if sync {
		l.completeSync(syncF, flushed)
	}
	return off, nil
}

var frameBufPool = sync.Pool{
	New: func() any { return make([]byte, 0, 512) },
}

const maxPooledFrameCap = 64 << 10

func (l *DataLog) writeFrameLocked(payload []byte) error {
	length := uint32(len(payload))
	checksum := crc32.Checksum(payload, Crc32Table)
	totalLen := LogFrameHeaderSize + int(length)

	buf := frameBufPool.Get().([]byte)
	if cap(buf) < totalLen {
		buf = make([]byte, totalLen)
	} else {
		buf = buf[:totalLen]
	}
	binary.BigEndian.PutUint32(buf[0:], length)
	binary.BigEndian.PutUint32(buf[4:], checksum)
	copy(buf[8:], payload)

	if err := l.rotateIfNeededLocked(int64(totalLen)); err != nil {
		if cap(buf) <= maxPooledFrameCap {
			frameBufPool.Put(buf[:0])
		}
		return err
	}
	seg := &l.segments[l.activeIndex]
	localOff := l.writeOffset - seg.baseLSN
	n, err := seg.writer.WriteAt(buf, localOff)
	if err == nil {
		l.buffers.applyWrite(seg.id, localOff, buf[:n])
	}
	if cap(buf) <= maxPooledFrameCap {
		frameBufPool.Put(buf[:0])
	}
	if err != nil {
		return err
	}
	l.writeOffset += int64(n)

	if l.writeOffset-seg.baseLSN >= l.usableSegmentSize() {
		return l.rotateSegmentLocked()
	}
	return nil
}

func (l *DataLog) rotateSegmentLocked() error {
	// Close of the active FD must wait for any fdatasync that already
	// released l.mu; otherwise that flush would operate on a closed file.
	l.inflightSyncs.Wait()
	active := &l.segments[l.activeIndex]
	if active.allocated {
		if err := writeSegmentFooter(active.writer, l.segmentSize, l.writeOffset-active.baseLSN); err != nil {
			return err
		}
	}
	if err := syncFile(active.writer); err != nil {
		return err
	}
	l.publishDurable(l.writeOffset)
	active.endLSN = l.writeOffset
	if err := active.writer.Close(); err != nil {
		return err
	}
	active.writer = nil
	if rf, openErr := os.Open(active.path); openErr == nil {
		active.reader = rf
	}

	nextID := active.id + 1
	nextName := walSegmentFileName(nextID)
	nextPath := filepath.Join(l.walDir, nextName)
	f, reused, err := l.takeRecycledSegment(nextPath)
	if err != nil {
		return err
	}
	if !reused {
		f, err = createAllocatedWALFile(nextPath, l.segmentSize)
		if err != nil {
			return err
		}
	}

	l.segments = append(l.segments, walSegment{
		id:        nextID,
		file:      nextName,
		path:      nextPath,
		baseLSN:   l.writeOffset,
		writer:    f,
		allocated: l.fileAllocated(f),
	})
	l.activeIndex = len(l.segments) - 1
	l.mapSegment(&l.segments[l.activeIndex])
	return l.persistManifestLocked()
}

func (l *DataLog) ReadValueAt(offset int64, valLen uint32) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.readValueAtLocked(offset, valLen)
}

// ReadValueAtCached is ReadValueAt through shared_buffers, then mmap/pread.
func (l *DataLog) ReadValueAtCached(offset int64, valLen uint32) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.buffers != nil {
		val, err := l.readValueViaBuffersLocked(offset, valLen)
		if err == nil {
			return val, nil
		}
	}
	return l.readValueAtLocked(offset, valLen)
}

func (l *DataLog) readValueAtLocked(offset int64, valLen uint32) ([]byte, error) {
	seg, local, ok := l.resolveLSN(offset)
	if !ok {
		return nil, fmt.Errorf("invalid log offset %d", offset)
	}
	var header [LogFrameHeaderSize]byte
	if err := l.readSegmentAt(seg, local, header[:]); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header[0:])
	if payloadLen > 1<<30 {
		return nil, ErrCorruptData
	}
	checksum := binary.BigEndian.Uint32(header[4:])
	payload := make([]byte, payloadLen)
	if err := l.readSegmentAt(seg, local+LogFrameHeaderSize, payload); err != nil {
		return nil, err
	}
	if crc32.Checksum(payload, Crc32Table) != checksum {
		return nil, ErrChecksum
	}
	return decodeValueAt(payload, valLen)
}

func (l *DataLog) readValueViaBuffersLocked(offset int64, valLen uint32) ([]byte, error) {
	seg, local, ok := l.resolveLSN(offset)
	if !ok {
		return nil, fmt.Errorf("invalid log offset %d", offset)
	}
	var pins []int
	defer func() {
		for _, i := range pins {
			l.buffers.unpin(i)
		}
	}()
	header, err := l.gatherFromBuffers(seg, local, LogFrameHeaderSize, &pins)
	if err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header[0:])
	if payloadLen > 1<<30 {
		return nil, ErrCorruptData
	}
	checksum := binary.BigEndian.Uint32(header[4:])
	payload, err := l.gatherFromBuffers(seg, local+LogFrameHeaderSize, int64(payloadLen), &pins)
	if err != nil {
		return nil, err
	}
	if crc32.Checksum(payload, Crc32Table) != checksum {
		return nil, ErrChecksum
	}
	return decodeValueAt(payload, valLen)
}

func (l *DataLog) gatherFromBuffers(seg *walSegment, local, n int64, pins *[]int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([]byte, n)
	copied := int64(0)
	for copied < n {
		pos := local + copied
		pageNo := uint32(pos / sharedBufferPageSize)
		pageOff := pos % sharedBufferPageSize
		tag := bufferTag{id: seg.id, page: pageNo}
		idx, page, err := l.buffers.pin(tag, func(dst []byte) error {
			return l.copyPageLocked(seg, int64(pageNo)*sharedBufferPageSize, dst)
		})
		if err != nil {
			return nil, err
		}
		*pins = append(*pins, idx)
		chunk := int64(len(page)) - pageOff
		if chunk > n-copied {
			chunk = n - copied
		}
		if chunk <= 0 {
			return nil, io.ErrUnexpectedEOF
		}
		copy(out[copied:], page[pageOff:pageOff+chunk])
		copied += chunk
	}
	return out, nil
}

func (l *DataLog) copyPageLocked(seg *walSegment, pageOff int64, dst []byte) error {
	if pageOff < 0 || len(dst) == 0 {
		return nil
	}
	n := int64(len(dst))
	if rem := l.segmentSize - pageOff; rem < n {
		if rem <= 0 {
			return io.ErrUnexpectedEOF
		}
		clear(dst[rem:])
		return l.readSegmentAt(seg, pageOff, dst[:rem])
	}
	return l.readSegmentAt(seg, pageOff, dst)
}

func (l *DataLog) readSegmentAt(seg *walSegment, local int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	n := int64(len(dst))
	if m := seg.mapping; len(m) > 0 {
		if local < 0 || local+n > int64(len(m)) {
			return io.ErrUnexpectedEOF
		}
		copy(dst, m[local:local+n])
		return nil
	}
	f, closeFn, err := segmentReadFile(seg)
	if err != nil {
		return err
	}
	if closeFn != nil {
		defer closeFn()
	}
	_, err = f.ReadAt(dst, local)
	return err
}

func segmentReadFile(seg *walSegment) (*os.File, func(), error) {
	if seg.writer != nil {
		return seg.writer, nil, nil
	}
	if seg.reader != nil {
		return seg.reader, nil, nil
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
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

func closeSegmentFiles(seg *walSegment) {
	if len(seg.mapping) > 0 {
		unmapWAL(seg.mapping)
		seg.mapping = nil
	}
	if seg.writer != nil {
		_ = seg.writer.Close()
		seg.writer = nil
	}
	if seg.reader != nil {
		_ = seg.reader.Close()
		seg.reader = nil
	}
}

const replayCancelCheckInterval = 1024

func (l *DataLog) Replay(ctx context.Context, truncateCorrupt bool, onRecord func(rec Record, span recordSpan)) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var records uint64
	for i := range l.segments {
		seg := &l.segments[i]
		isActive := i == l.activeIndex
		if err := l.replaySegmentFile(ctx, seg, isActive, truncateCorrupt, &records, onRecord); err != nil {
			if errors.Is(err, ErrTruncated) && isActive {
				// writeOffset is already the last valid frame (not the
				// preallocated file size). Persist that logical head.
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
	limit := l.segmentScanLimit(seg, fileSizeOf(seg.path))
	if limit <= 0 {
		return nil
	}
	if len(seg.mapping) > 0 {
		adviseWALRange(seg.mapping, 0, limit, walAdviseSequential())
		defer adviseWALRange(seg.mapping, 0, limit, walAdviseRandom())
	}

	pos := int64(0)
	for pos < limit {
		if *records%replayCancelCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		validEnd, rec, span, rerr := l.readFrameAtSeg(seg, pos, limit)
		if rerr != nil {
			if (rerr == io.ErrUnexpectedEOF || rerr == ErrChecksum || rerr == ErrCorruptData) && truncateCorrupt && isActive {
				l.logger.Warn("Truncating corrupt wal tail", "segment", seg.file, "offset", seg.baseLSN+pos, "err", rerr)
				if seg.writer != nil {
					_ = writeSegmentFooterIfAllocated(seg.writer, l.segmentSize, pos)
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

func (l *DataLog) readFrameAtSeg(seg *walSegment, offset, limit int64) (int64, Record, recordSpan, error) {
	if m := seg.mapping; len(m) > 0 {
		if limit > int64(len(m)) {
			limit = int64(len(m))
		}
		return readFrameAtMapping(m, offset, limit)
	}
	f, closeFn, err := segmentReadFile(seg)
	if err != nil {
		return offset, Record{}, recordSpan{}, err
	}
	if closeFn != nil {
		defer closeFn()
	}
	return readFrameAtFile(f, offset, limit)
}

func readFrameAtMapping(m []byte, offset, fileSize int64) (int64, Record, recordSpan, error) {
	if offset+LogFrameHeaderSize > fileSize || offset+LogFrameHeaderSize > int64(len(m)) {
		return offset, Record{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	length := binary.BigEndian.Uint32(m[offset:])
	checksum := binary.BigEndian.Uint32(m[offset+4:])
	if length > 1<<30 {
		return offset, Record{}, recordSpan{}, ErrCorruptData
	}
	total := frameSize(int(length))
	if offset+total > fileSize || offset+total > int64(len(m)) {
		return offset, Record{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	payload := m[offset+LogFrameHeaderSize : offset+LogFrameHeaderSize+int64(length)]
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
	limit := l.segmentScanLimit(seg, fileSizeOf(seg.path))
	if localEnd > limit {
		localEnd = limit
	}
	if localStart >= localEnd {
		return nil
	}

	pos := localStart
	for pos < localEnd {
		validEnd, rec, _, rerr := l.readFrameAtSeg(seg, pos, localEnd)
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
	_, _, _, err := l.readFrameAtSeg(seg, local, l.segmentScanLimit(seg, fileSizeOf(seg.path)))
	return err == nil
}

func (l *DataLog) segmentScanLimit(seg *walSegment, fileSize int64) int64 {
	used := l.segmentUsedBytes(seg)
	usable := l.usableSegmentSize()
	if used > usable {
		used = usable
	}
	if fileSize > 0 && used > fileSize {
		return fileSize
	}
	if used < 0 {
		return 0
	}
	return used
}

// LogicalSize is the retained WAL LSN span (write head minus oldest segment base).
// After segment delete this is smaller than WriteOffset.
func (l *DataLog) LogicalSize() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	size := l.writeOffset - l.segments[0].baseLSN
	if size < 0 {
		return 0
	}
	return size
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

// OldestSegmentBaseLSN returns the base LSN of the earliest retained segment.
func (l *DataLog) OldestSegmentBaseLSN() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseLSN
}

// InitLogAtLSN sets the empty log's origin to lsn so replicated/restored frames
// keep the primary's global byte addresses. The on-disk file still starts at
// offset 0; only the manifest BaseLSN and write head change.
func (l *DataLog) InitLogAtLSN(lsn int64) error {
	if lsn < 0 {
		return fmt.Errorf("init log at lsn: negative lsn %d", lsn)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writeOffset == lsn && len(l.segments) > 0 && l.segments[l.activeIndex].baseLSN == lsn {
		return nil
	}
	if l.writeOffset != 0 {
		return fmt.Errorf("init log at lsn: log is not empty (writeOffset=%d)", l.writeOffset)
	}
	if lsn == 0 {
		return nil
	}
	if l.activeIndex < 0 || len(l.segments) != 1 {
		return fmt.Errorf("init log at lsn: expected a single empty segment")
	}
	seg := &l.segments[l.activeIndex]
	if seg.baseLSN != 0 {
		return fmt.Errorf("init log at lsn: unexpected base lsn %d", seg.baseLSN)
	}
	oldBase := seg.baseLSN
	oldDurable := atomic.LoadInt64(&l.durableOffset)
	seg.baseLSN = lsn
	l.writeOffset = lsn
	atomic.StoreInt64(&l.durableOffset, lsn)
	if err := l.persistManifestLocked(); err != nil {
		seg.baseLSN = oldBase
		l.writeOffset = 0
		atomic.StoreInt64(&l.durableOffset, oldDurable)
		return err
	}
	return nil
}

// deleteSegmentsThrough removes sealed segments whose exclusive end LSN is at or
// below maxEndLSN. The active segment is never deleted.
func (l *DataLog) deleteSegmentsThrough(maxEndLSN int64) (int, int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	deleted, reclaimed, err := l.deleteSegmentsThroughLocked(maxEndLSN)
	if err != nil || deleted == 0 {
		return deleted, reclaimed, err
	}
	if err := l.persistManifestLocked(); err != nil {
		return deleted, reclaimed, err
	}
	return deleted, reclaimed, nil
}

func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inflightSyncs.Wait()
	var firstErr error
	for i := range l.segments {
		if len(l.segments[i].mapping) > 0 {
			unmapWAL(l.segments[i].mapping)
			l.segments[i].mapping = nil
		}
		if l.segments[i].writer != nil {
			if err := l.segments[i].writer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			l.segments[i].writer = nil
		}
		if l.segments[i].reader != nil {
			if err := l.segments[i].reader.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			l.segments[i].reader = nil
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
