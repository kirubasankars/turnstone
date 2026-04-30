package stonedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"errors"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const (
	// DefaultMaxWALSize is 1GB. Large enough to batch IO, small enough to manage.
	DefaultMaxWALSize = 1 * 1024 * 1024 * 1024
)

// WriteAheadLog handles append-only log files for crash recovery.
type WriteAheadLog struct {
	dir                string
	currentFile        *os.File
	currentStartOffset uint64 // Virtual offset of the start of the current file
	writeOffset        uint32 // Offset within the current file
	maxSize            uint32 // Size threshold for rotation
	mu                 sync.Mutex
	logger             *slog.Logger

	// Timeline Support
	timelineID uint64

	// Rotation Indexing
	batchIndex map[uint64]WALLocation
	onRotate   func(map[uint64]WALLocation) error
}

// OpenWriteAheadLog initializes the WAL subsystem.
func OpenWriteAheadLog(dir string, maxSize uint32, requestedTL uint64, logger *slog.Logger) (*WriteAheadLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if maxSize == 0 {
		maxSize = DefaultMaxWALSize
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return nil, err
	}

	var activePath string
	var currentStartOffset uint64
	var currentTL uint64 = requestedTL

	if len(matches) > 0 {
		// Sort to find the latest file
		sortWALFiles(matches)
		latest := matches[len(matches)-1]

		tl, off, err := parseWALFilename(latest)
		if err != nil {
			return nil, fmt.Errorf("failed to parse latest wal file %s: %w", latest, err)
		}

		switch {
		case tl > currentTL:
			// Consistency Check: the latest file on disk is from a HIGHER
			// timeline than requested. This means the metadata file is
			// stale or we are recovering on a node that was already
			// promoted. We respect the disk state.
			currentTL = tl
			activePath = latest
			currentStartOffset = off
		case tl < currentTL:
			// timeline.meta says we should already be on a newer timeline
			// than any file that actually exists on disk -- e.g. we
			// crashed after saveTimelineMeta() persisted the new timeline
			// but before ForceNewTimeline() created its first file.
			// Reusing the stale timeline's file here would silently label
			// every new record we append with the wrong (old) timeline,
			// which recovery's history-based orphan filtering (see
			// replayFile's cutoffOp check) depends on being accurate.
			// Start a fresh file for the requested timeline instead,
			// continuing the virtual offset sequence from where the old
			// file leaves off.
			latestStat, statErr := os.Stat(latest)
			if statErr != nil {
				return nil, fmt.Errorf("failed to stat latest wal file %s: %w", latest, statErr)
			}
			currentStartOffset = off + uint64(latestStat.Size())
			activePath = filepath.Join(dir, fmt.Sprintf("wal_%d_%020d.wal", currentTL, currentStartOffset))
		default:
			activePath = latest
			currentStartOffset = off
		}
	} else {
		// No files, start fresh on requested timeline
		currentStartOffset = 0
		activePath = filepath.Join(dir, fmt.Sprintf("wal_%d_%020d.wal", currentTL, 0))
	}

	// DEBUG: Low level file op
	logger.Debug("Opening WAL", "active_file", activePath, "timeline", currentTL)

	f, err := os.OpenFile(activePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &WriteAheadLog{
		dir:                dir,
		currentFile:        f,
		currentStartOffset: currentStartOffset,
		writeOffset:        uint32(stat.Size()),
		maxSize:            maxSize,
		timelineID:         currentTL,
		batchIndex:         make(map[uint64]WALLocation),
		logger:             logger,
	}, nil
}

func parseWALFilename(name string) (uint64, uint64, error) {
	base := filepath.Base(name)
	if strings.HasPrefix(base, "wal_") {
		var tl, off uint64
		body := strings.TrimSuffix(base, ".wal")
		parts := strings.Split(body, "_")
		if len(parts) == 3 {
			var err error
			if tl, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
				return 0, 0, err
			}
			if off, err = strconv.ParseUint(parts[2], 10, 64); err != nil {
				return 0, 0, err
			}
			return tl, off, nil
		}
		return 0, 0, fmt.Errorf("malformed wal filename: %s", base)
	}
	s := strings.TrimSuffix(base, ".wal")
	off, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return 0, off, nil
}

func sortWALFiles(paths []string) {
	sort.Slice(paths, func(i, j int) bool {
		tl1, off1, _ := parseWALFilename(paths[i])
		tl2, off2, _ := parseWALFilename(paths[j])
		if tl1 != tl2 {
			return tl1 < tl2
		}
		return off1 < off2
	})
}

// encodeWALRecord serializes a WALRecord to its on-disk payload:
// Type(1) + XID(8) + OpID(8) + type-specific body.
//   - Set:    KeyLen(4) + Key + ValLen(4) + Val
//   - Delete: KeyLen(4) + Key
//   - Begin/Commit/Abort: empty body
func encodeWALRecord(rec WALRecord) []byte {
	var bodyLen int
	switch rec.Type {
	case WALRecordSet:
		bodyLen = 4 + len(rec.Key) + 4 + len(rec.Value)
	case WALRecordDelete:
		bodyLen = 4 + len(rec.Key)
	}
	buf := make([]byte, WALRecordHeaderSize+bodyLen)
	buf[0] = byte(rec.Type)
	binary.BigEndian.PutUint64(buf[1:], rec.XID)
	binary.BigEndian.PutUint64(buf[9:], rec.OpID)

	switch rec.Type {
	case WALRecordSet:
		off := WALRecordHeaderSize
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
		off += 4
		copy(buf[off:], rec.Key)
		off += len(rec.Key)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Value)))
		off += 4
		copy(buf[off:], rec.Value)
	case WALRecordDelete:
		off := WALRecordHeaderSize
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
		off += 4
		copy(buf[off:], rec.Key)
	}
	return buf
}

// decodeWALRecord parses a single WAL frame payload back into a WALRecord.
func decodeWALRecord(payload []byte) (WALRecord, error) {
	if len(payload) < WALRecordHeaderSize {
		return WALRecord{}, ErrCorruptData
	}
	rec := WALRecord{
		Type: WALRecordType(payload[0]),
		XID:  binary.BigEndian.Uint64(payload[1:]),
		OpID: binary.BigEndian.Uint64(payload[9:]),
	}
	body := payload[WALRecordHeaderSize:]
	switch rec.Type {
	case WALRecordSet:
		if len(body) < 4 {
			return WALRecord{}, ErrCorruptData
		}
		klen := int(binary.BigEndian.Uint32(body[0:]))
		off := 4
		if off+klen+4 > len(body) {
			return WALRecord{}, ErrCorruptData
		}
		rec.Key = append([]byte{}, body[off:off+klen]...)
		off += klen
		vlen := int(binary.BigEndian.Uint32(body[off:]))
		off += 4
		if off+vlen > len(body) {
			return WALRecord{}, ErrCorruptData
		}
		rec.Value = append([]byte{}, body[off:off+vlen]...)
	case WALRecordDelete:
		if len(body) < 4 {
			return WALRecord{}, ErrCorruptData
		}
		klen := int(binary.BigEndian.Uint32(body[0:]))
		off := 4
		if off+klen > len(body) {
			return WALRecord{}, ErrCorruptData
		}
		rec.Key = append([]byte{}, body[off:off+klen]...)
	case WALRecordBegin, WALRecordCommit, WALRecordAbort:
		// no body
	default:
		return WALRecord{}, ErrCorruptData
	}
	return rec, nil
}

func (wal *WriteAheadLog) SetOnRotate(fn func(map[uint64]WALLocation) error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.onRotate = fn
}

func (wal *WriteAheadLog) FindInMemory(opID uint64) (WALLocation, bool) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	var bestOp uint64
	var found bool

	for op := range wal.batchIndex {
		if op <= opID {
			if !found || op > bestOp {
				bestOp = op
				found = true
						}
		}
	}

	if found {
		return wal.batchIndex[bestOp], true
	}
	return WALLocation{}, false
}

// strictSync enforces durability. If the disk fails, the process dies.
// This prevents "The False Success" where the OS marks dirty pages as clean
// after an error. It has no error return: every path either succeeds (nil
// Sync) or panics, and a previous `error` return type was pure dead code
// that misled every call site into unreachable "if err != nil" branches.
func (wal *WriteAheadLog) strictSync() {
	err := wal.currentFile.Sync()
	if err != nil {
		// 1. Transient errors (interrupted system call) can be retried.
		if errors.Is(err, syscall.EINTR) {
			wal.strictSync()
			return
		}

		// 2. CRITICAL HARDWARE FAILURE (EIO, EROFS, ENOSPC).
		// We cannot trust the OS page cache. We must crash immediately.
		wal.logger.Error("CRITICAL: fsync failed. Storage integrity compromised. Panicking.", "err", err)
		panic(fmt.Sprintf("CRITICAL STORAGE FAILURE: %v", err))
	}
}

// AppendRecordsWithOpIDs assigns an opID (via nextOpID) to each builder in order,
// writes the resulting frame, and optionally fsyncs once for the whole group.
// opID allocation and the WAL append happen inside the same critical section so
// physical file order always matches logical opID order.
func (wal *WriteAheadLog) AppendRecordsWithOpIDs(nextOpID func() uint64, builders []func(opID uint64) []byte, sync bool) ([]uint64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if len(builders) == 0 {
		return nil, nil
	}

	if wal.writeOffset >= wal.maxSize {
		if err := wal.rotate(); err != nil {
			return nil, err
		}
	}

	startOffset := wal.writeOffset
	opIDs := make([]uint64, len(builders))

	for i, build := range builders {
		opID := nextOpID()
		opIDs[i] = opID
		payload := build(opID)
		if err := wal.writeFrame(payload); err != nil {
			_ = wal.truncateTailLocked(int64(wal.writeOffset - startOffset))
			// writeFrame records a batchIndex entry for every record it
			// successfully writes *before* this failing one. Those bytes
			// were just truncated away above, so their batchIndex entries
			// are now stale: if left in place, a later rotation could
			// flush them into the durable WAL index, pointing a future
			// ScanWAL/replication reader at an offset that may since have
			// been overwritten by unrelated data.
			for j := 0; j < i; j++ {
				delete(wal.batchIndex, opIDs[j])
			}
			return nil, err
		}
	}

	if sync {
		wal.strictSync()
	}
	return opIDs, nil
}

// AppendReplicatedRecord writes a single pre-built record payload (opID already
// assigned by the leader) verbatim. Used by followers applying a replication stream.
func (wal *WriteAheadLog) AppendReplicatedRecord(payload []byte, sync bool) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.writeOffset >= wal.maxSize {
		if err := wal.rotate(); err != nil {
			return err
		}
	}

	if err := wal.writeFrame(payload); err != nil {
		return err
	}
	if sync {
		wal.strictSync()
	}
	return nil
}

// writeFrame writes a single frame atomically (header + payload).
// It assumes the lock is held.
func (wal *WriteAheadLog) writeFrame(payload []byte) error {
	length := uint32(len(payload))
	checksum := crc32.Checksum(payload, Crc32Table)
	totalLen := WALHeaderSize + int(length)

	// Combine Header and Payload into one buffer to reduce syscalls and partial writes
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:], length)
	binary.BigEndian.PutUint32(buf[4:], checksum)
	copy(buf[8:], payload)

	n, err := wal.currentFile.Write(buf)
	if err != nil {
		return err
	}

	// Only update offsets and index on success
	currentFileOffset := wal.writeOffset
	wal.writeOffset += uint32(n)

	if opID, ok := peekOpID(payload); ok {
		wal.batchIndex[opID] = WALLocation{
			FileStartOffset: wal.currentStartOffset,
			RelativeOffset:  currentFileOffset,
		}
	}

	return nil
}

// peekOpID extracts the OpID field (bytes [9:17]) from an encoded WAL record
// payload without doing a full decode.
func peekOpID(payload []byte) (uint64, bool) {
	if len(payload) < WALRecordHeaderSize {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[9:17]), true
}

func (wal *WriteAheadLog) truncateTailLocked(bytesToRemove int64) error {
	currentSize := int64(wal.writeOffset)
	if currentSize < bytesToRemove {
		return fmt.Errorf("cannot rollback across file boundary (offset %d < remove %d)", currentSize, bytesToRemove)
	}

	newOffset := currentSize - bytesToRemove
	if err := wal.currentFile.Truncate(newOffset); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	if _, err := wal.currentFile.Seek(newOffset, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}
	wal.writeOffset = uint32(newOffset)
	// Rollback also requires sync to be durable
	wal.strictSync()
	return nil
}

func (wal *WriteAheadLog) Rotate() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.rotate()
}

func (wal *WriteAheadLog) rotate() error {
	// Optimization: Do not rotate empty files
	if wal.writeOffset == 0 {
		return nil
	}

	// Fsync before handing batchIndex to onRotate: onRotate persists these
	// offsets into LevelDB's durable WAL index (used by ScanWAL/replication
	// to locate records by opID), so a reader could be pointed at an offset
	// whose bytes were never actually flushed to disk if a crash happened
	// between an unsynced write and the (already-persisted) index entry for
	// it. Syncing first guarantees every offset onRotate is about to record
	// is already durable.
	wal.strictSync()
	if wal.onRotate != nil && len(wal.batchIndex) > 0 {
		if err := wal.onRotate(wal.batchIndex); err != nil {
			return fmt.Errorf("wal rotate hook failed: %w", err)
		}
	}
	wal.batchIndex = make(map[uint64]WALLocation)

	if err := wal.currentFile.Close(); err != nil {
		return err
	}

	wal.currentStartOffset += uint64(wal.writeOffset)
	path := filepath.Join(wal.dir, fmt.Sprintf("wal_%d_%020d.wal", wal.timelineID, wal.currentStartOffset))

	// INFO: File Lifecycle event
	wal.logger.Info("Rotating WAL", "new_file", filepath.Base(path))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	wal.currentFile = f
	wal.writeOffset = 0
	return nil
}

func (wal *WriteAheadLog) ForceNewTimeline(newTimelineID uint64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if newTimelineID <= wal.timelineID {
		return fmt.Errorf("new timeline %d must be greater than current %d", newTimelineID, wal.timelineID)
	}

	wal.strictSync()
	if err := wal.currentFile.Close(); err != nil {
		return err
	}

	if wal.onRotate != nil && len(wal.batchIndex) > 0 {
		if err := wal.onRotate(wal.batchIndex); err != nil {
			return fmt.Errorf("wal rotate hook failed: %w", err)
		}
	}
	wal.batchIndex = make(map[uint64]WALLocation)

	wal.timelineID = newTimelineID
	wal.currentStartOffset += uint64(wal.writeOffset)
	wal.writeOffset = 0

	path := filepath.Join(wal.dir, fmt.Sprintf("wal_%d_%020d.wal", wal.timelineID, wal.currentStartOffset))
	// INFO: Timeline Event
	wal.logger.Info("Forcing new timeline", "timeline", wal.timelineID, "file", filepath.Base(path))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	wal.currentFile = f
	return nil
}

// ReplaySinceTx replays every WAL record found on disk, in order. Records
// with OpID > minOpID are redone into the ValueLog (they may not have made it
// there before a crash); onReplay is invoked for every record regardless, so
// callers can reconstruct the commit log. It accepts a timeline history to
// ensure we don't replay "zombie" writes from abandoned timelines.
func (wal *WriteAheadLog) ReplaySinceTx(vl *ValueLog, minOpID uint64, history []TimelineHistoryItem, truncateCorrupt bool, onReplay func(WALRecord), onTruncate func() error) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
	if err != nil {
		return err
	}

	sortWALFiles(matches)

	for i, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		isLastFile := (i == len(matches)-1)

		err = wal.replayFile(f, path, vl, minOpID, history, truncateCorrupt, isLastFile, onReplay, onTruncate)
		f.Close()

		if err != nil {
			if err == ErrTruncated {
				wal.logger.Warn("Stopping WAL replay due to truncation", "file", filepath.Base(path))
				return nil
			}
			return err
		}
	}

	// Ensure the current file handle is positioned at the end after replay updates/truncations
	wal.currentFile.Seek(0, 2)
	return nil
}

func (wal *WriteAheadLog) Scan(startLoc WALLocation, fn func([]WALRecord) error) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
	if err != nil {
		return err
	}

	sortWALFiles(matches)

	if len(matches) == 0 {
		return ErrLogUnavailable
	}

	_, firstOffset, _ := parseWALFilename(matches[0])
	if startLoc.FileStartOffset < firstOffset {
		return ErrLogUnavailable
	}

	startIndex := -1
	for i, m := range matches {
		_, offset, _ := parseWALFilename(m)
		if offset == startLoc.FileStartOffset {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		for i, m := range matches {
			_, offset, _ := parseWALFilename(m)
			if offset > startLoc.FileStartOffset {
				break
			}
			startIndex = i
		}
	}

	if startIndex == -1 {
		startIndex = 0
	}

	for i := startIndex; i < len(matches); i++ {
		path := matches[i]
		_, fileStart, _ := parseWALFilename(path)

		if fileStart < startLoc.FileStartOffset {
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}

		seekOff := int64(0)
		if fileStart == startLoc.FileStartOffset {
			seekOff = int64(startLoc.RelativeOffset)
		}

		err = wal.scanFile(f, seekOff, fn)
		f.Close()
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

func (wal *WriteAheadLog) PurgeOlderThan(minOpID uint64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
	if err != nil {
		return err
	}

	sortWALFiles(matches)

	if len(matches) <= 1 {
		return nil
	}

	for i := 0; i < len(matches)-1; i++ {
		currentPath := matches[i]
		nextPath := matches[i+1]

		// FIX 1: Clean up empty garbage files first
		// This handles files accumulated from rapid restarts (Dev Mode)
		if stat, err := os.Stat(currentPath); err == nil && stat.Size() == 0 {
			wal.logger.Info("Purging empty WAL file", "file", filepath.Base(currentPath))
			if err := os.Remove(currentPath); err != nil {
				return err
			}
			continue
		}

		// FIX 2: Gracefully handle empty/unreadable NEXT file
		nextStartOpID, err := wal.readFirstOpID(nextPath)
		if err != nil {
			// If we can't read the next file header, check if it's empty
			if stat, sErr := os.Stat(nextPath); sErr == nil && stat.Size() == 0 {
				// If next is the LAST file, it is the active head (just created).
				// We cannot delete 'current' yet because 'next' has no ops to bound it.
				// Stop the purge loop cleanly without error.
				if i+1 == len(matches)-1 {
					wal.logger.Debug("Next WAL file is active and empty, stopping purge", "current", filepath.Base(currentPath), "next", filepath.Base(nextPath))
					break
				}
				// If next is NOT the last file, it is an empty intermediate file (garbage).
				// We continue the loop. The next iteration will pick it up as 'currentPath'
				// and delete it via FIX 1 above.
				wal.logger.Info("Next WAL file is empty (intermediate), skipping boundary check", "next", filepath.Base(nextPath))
				continue
			}

			// Genuine read error
			wal.logger.Warn("Cannot read next WAL file header, skipping purge check for current file", "current", filepath.Base(currentPath), "next", filepath.Base(nextPath), "err", err)
			continue
		}

		if nextStartOpID <= minOpID {
			// INFO: Garbage Collection
			wal.logger.Info("Purging WAL file (older than constraint)", "file", filepath.Base(currentPath), "constraint", minOpID)
			if err := os.Remove(currentPath); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

func (wal *WriteAheadLog) readFirstOpID(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	header := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return 0, err
	}

	length := binary.BigEndian.Uint32(header[0:])
	// Sanity Check: a record must be at least as big as its header
	if length < WALRecordHeaderSize {
		return 0, fmt.Errorf("invalid record length: %d", length)
	}

	buf := make([]byte, WALRecordHeaderSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, err
	}

	opID := binary.BigEndian.Uint64(buf[9:])
	return opID, nil
}

func (wal *WriteAheadLog) scanFile(f *os.File, startOffset int64, fn func([]WALRecord) error) error {
	if startOffset > 0 {
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		if startOffset >= stat.Size() {
			return nil
		}
		if _, err := f.Seek(startOffset, 0); err != nil {
			return err
		}
	}

	reader := bufio.NewReader(f)

	_, err := wal.stream(reader, func(offset int64, payload []byte) error {
		rec, decErr := decodeWALRecord(payload)
		if decErr != nil {
			return decErr
		}
		return fn([]WALRecord{rec})
	})
	return err
}

func (wal *WriteAheadLog) replayFile(f *os.File, path string, vl *ValueLog, minOpID uint64, history []TimelineHistoryItem, truncateCorrupt bool, isLastFile bool, onReplay func(WALRecord), onTruncate func() error) error {
	reader := bufio.NewReader(f)

	// FIX: Parse timeline from filename to support history pruning
	fileTL, _, _ := parseWALFilename(path)

	// FIX: Find the Cutoff OpID for this timeline from history
	var cutoffOp uint64 = math.MaxUint64
	for _, h := range history {
		if h.TLI == fileTL {
			cutoffOp = h.EndOp
			break
		}
	}

	validOffset, err := wal.stream(reader, func(offset int64, payload []byte) error {
		rec, decErr := decodeWALRecord(payload)
		if decErr != nil {
			return decErr
		}

		if filepath.Base(path) == filepath.Base(wal.currentFile.Name()) {
			wal.batchIndex[rec.OpID] = WALLocation{
				FileStartOffset: wal.currentStartOffset,
				RelativeOffset:  uint32(offset),
			}
		}

		// FIX: Check if this record is orphaned (belongs to a dead branch of history)
		if rec.OpID > cutoffOp {
			wal.logger.Warn("Skipping orphaned WAL record (exceeds timeline history)",
				"file", filepath.Base(path),
				"file_tl", fileTL,
				"op_id", rec.OpID,
				"cutoff", cutoffOp)
			return nil
		}

		if rec.OpID > minOpID && (rec.Type == WALRecordSet || rec.Type == WALRecordDelete) {
			entry := ValueLogEntry{Key: rec.Key, Value: rec.Value, TransactionID: rec.XID, OperationID: rec.OpID, IsDelete: rec.Type == WALRecordDelete}
			if _, _, err := vl.AppendEntries([]ValueLogEntry{entry}); err != nil {
				return err
			}
		}
		if onReplay != nil {
			onReplay(rec)
		}
		return nil
	})

	if err != nil {
		if err == io.ErrUnexpectedEOF || err == ErrChecksum || err == ErrCorruptData {
			if truncateCorrupt && isLastFile {
				// A torn/garbage tail on the current (last) WAL file most
				// likely means we crashed (or a caller injected garbage)
				// mid-write. Only the last file can safely be truncated:
				// anything before it is a sealed, previously-fsynced file
				// that must never be rewritten. Truncate back to the last
				// known-good frame boundary and let the caller (onTruncate)
				// discard any index state that can no longer be trusted.
				wal.logger.Warn("WAL corruption detected at tail of last file. Truncating.",
					"file", filepath.Base(path),
					"offset", validOffset,
					"err", err)
				if truncErr := os.Truncate(path, validOffset); truncErr != nil {
					return fmt.Errorf("failed to truncate corrupt WAL tail %s: %w", path, truncErr)
				}
				if filepath.Base(path) == filepath.Base(wal.currentFile.Name()) {
					// Keep in-memory accounting in sync with the file we
					// just shrank; the fd stays open (O_APPEND resolves the
					// new EOF on the next write automatically).
					wal.writeOffset = uint32(validOffset)
				}
				if onTruncate != nil {
					if cbErr := onTruncate(); cbErr != nil {
						return fmt.Errorf("onTruncate callback failed for %s: %w", path, cbErr)
					}
				}
				return ErrTruncated
			}
			// Prioritize Durability: Do not truncate. Treat as fatal corruption.
			// This forces manual intervention (admin decision) rather than silent data loss.
			// Exception: Standard EOF is handled by stream return value.
			wal.logger.Error("WAL corruption detected. Refusing to truncate automatically.",
				"file", filepath.Base(path),
				"offset", validOffset,
				"err", err)
			return fmt.Errorf("FATAL: WAL corruption at offset %d in %s: %w. Manual intervention required", validOffset, path, err)
		} else if err != io.EOF {
			return err
		}
	}

	return nil
}

func (wal *WriteAheadLog) stream(r io.Reader, onPayload func(offset int64, payload []byte) error) (int64, error) {
	validOffset := int64(0)
	// Safety limit for a single frame read to prevent OOM on corrupt headers
	const maxReadSize = 1 * 1024 * 1024 * 1024 // 1GB

	for {
		header := make([]byte, WALHeaderSize)
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF {
				return validOffset, io.EOF
			}
			return validOffset, io.ErrUnexpectedEOF
		}

		length := binary.BigEndian.Uint32(header[0:])
		checksum := binary.BigEndian.Uint32(header[4:])

		if length > maxReadSize {
			return validOffset, ErrCorruptData
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return validOffset, io.ErrUnexpectedEOF
		}

		if crc32.Checksum(payload, Crc32Table) != checksum {
			return validOffset, ErrChecksum
		}

		if err := onPayload(validOffset, payload); err != nil {
			return validOffset, err
		}

		validOffset += int64(WALHeaderSize) + int64(length)
	}
}

func (wal *WriteAheadLog) Close() error {
	return wal.currentFile.Close()
}
