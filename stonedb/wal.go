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

		// Consistency Check:
		// If the latest file on disk is from a HIGHER timeline than requested,
		// it means the metadata file is stale or we are recovering on a node
		// that was already promoted. We respect the disk state.
		if tl > currentTL {
			currentTL = tl
		}

		activePath = latest
		currentStartOffset = off
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
// This prevents "The False Success" where the OS marks dirty pages as clean after an error.
func (wal *WriteAheadLog) strictSync() error {
	err := wal.currentFile.Sync()
	if err != nil {
		// 1. Transient errors (interrupted system call) can be retried.
		if errors.Is(err, syscall.EINTR) {
			return wal.strictSync()
		}

		// 2. CRITICAL HARDWARE FAILURE (EIO, EROFS, ENOSPC).
		// We cannot trust the OS page cache. We must crash immediately.
		wal.logger.Error("CRITICAL: fsync failed. Storage integrity compromised. Panicking.", "err", err)
		panic(fmt.Sprintf("CRITICAL STORAGE FAILURE: %v", err))
	}
	return nil
}

func (wal *WriteAheadLog) AppendBatch(payload []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Check rotation threshold
	if wal.writeOffset >= wal.maxSize {
		if err := wal.rotate(); err != nil {
			return err
		}
	}

	if err := wal.writeFrame(payload); err != nil {
		return err
	}

	return wal.strictSync()
}

func (wal *WriteAheadLog) AppendBatches(payloads [][]byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Check rotation threshold before writing the group
	if wal.writeOffset >= wal.maxSize {
		if err := wal.rotate(); err != nil {
			return err
		}
	}

	// Track start offset for internal rollback if needed within this loop
	startOffset := wal.writeOffset

	for _, payload := range payloads {
		if err := wal.writeFrame(payload); err != nil {
			// If a write fails mid-batch, we attempt to clean up internally first.
			_ = wal.truncateTailLocked(int64(wal.writeOffset - startOffset))
			return err
		}
	}

	// Sync only once for the whole group
	return wal.strictSync()
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

	if len(payload) >= 16 {
		startOpID := binary.BigEndian.Uint64(payload[8:])
		wal.batchIndex[startOpID] = WALLocation{
			FileStartOffset: wal.currentStartOffset,
			RelativeOffset:  currentFileOffset,
		}
	}

	return nil
}

// TruncateTail removes the last N bytes from the active WAL file.
// Used by the DB to rollback if the VLog write fails.
func (wal *WriteAheadLog) TruncateTail(bytesToRemove int64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.truncateTailLocked(bytesToRemove)
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
	if err := wal.strictSync(); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
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

	if wal.onRotate != nil && len(wal.batchIndex) > 0 {
		if err := wal.onRotate(wal.batchIndex); err != nil {
			return fmt.Errorf("wal rotate hook failed: %w", err)
		}
	}
	wal.batchIndex = make(map[uint64]WALLocation)

	if err := wal.strictSync(); err != nil {
		return err
	}
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

	if err := wal.strictSync(); err != nil {
		return err
	}
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

// ReplaySinceTx replays WAL entries from the given txID.
// It accepts a timeline history to ensure we don't replay "zombie" writes from abandoned timelines.
func (wal *WriteAheadLog) ReplaySinceTx(vl *ValueLog, minTxID uint64, history []TimelineHistoryItem, truncateCorrupt bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
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

		err = wal.replayFile(f, path, vl, minTxID, history, truncateCorrupt, isLastFile, onReplay, onTruncate)
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

func (wal *WriteAheadLog) Scan(startLoc WALLocation, fn func([]ValueLogEntry) error) error {
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
	// Sanity Check: a batch must be at least as big as its header
	if length < WALBatchHeaderSize {
		return 0, fmt.Errorf("invalid batch length: %d", length)
	}

	buf := make([]byte, 16)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, err
	}

	startOpID := binary.BigEndian.Uint64(buf[8:])
	return startOpID, nil
}

func (wal *WriteAheadLog) scanFile(f *os.File, startOffset int64, fn func([]ValueLogEntry) error) error {
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
		_, _, entries := decodeWALBatchEntries(payload)
		if len(entries) > 0 {
			return fn(entries)
		}
		return nil
	})
	return err
}

func (wal *WriteAheadLog) replayFile(f *os.File, path string, vl *ValueLog, minTxID uint64, history []TimelineHistoryItem, truncateCorrupt bool, isLastFile bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
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
		txID, startOpID, entries := decodeWALBatchEntries(payload)

		if filepath.Base(path) == filepath.Base(wal.currentFile.Name()) {
			wal.batchIndex[startOpID] = WALLocation{
				FileStartOffset: wal.currentStartOffset,
				RelativeOffset:  uint32(offset),
			}
		}

		// FIX: Check if this batch is orphaned (belongs to a dead branch of history)
		if startOpID > cutoffOp {
			wal.logger.Warn("Skipping orphaned WAL batch (exceeds timeline history)",
				"file", filepath.Base(path),
				"file_tl", fileTL,
				"op_id", startOpID,
				"cutoff", cutoffOp)
			return nil
		}

		if txID > minTxID {
			if _, _, err := vl.AppendEntries(entries); err != nil {
				return err
			}
			if onReplay != nil {
				onReplay(entries)
			}
		}
		return nil
	})

	if err != nil {
		if err == io.ErrUnexpectedEOF || err == ErrChecksum || err == ErrCorruptData {
			// Prioritize Durability: Do not truncate. Treat as fatal corruption.
			// This forces manual intervention (admin decision) rather than silent data loss.
			// Exception: Standard EOF is handled by stream return value.
			wal.logger.Error("WAL corruption detected. Refusing to truncate automatically.",
				"file", filepath.Base(path),
				"offset", validOffset,
				"err", err)
			return fmt.Errorf("FATAL: WAL corruption at offset %d in %s: %w. Manual intervention required.", validOffset, path, err)
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

func decodeWALBatchEntries(payload []byte) (uint64, uint64, []ValueLogEntry) {
	if len(payload) < WALBatchHeaderSize {
		return 0, 0, nil
	}
	txID := binary.BigEndian.Uint64(payload[0:])
	startOpID := binary.BigEndian.Uint64(payload[8:])

	var entries []ValueLogEntry
	offset := WALBatchHeaderSize
	opIdx := uint64(0)

	for offset < len(payload) {
		if offset+4 > len(payload) {
			break
		}
		keyLen := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4

		if offset+keyLen > len(payload) {
			break
		}
		key := payload[offset : offset+keyLen]
		offset += keyLen

		if offset+4 > len(payload) {
			break
		}
		valLen := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4

		if offset+valLen > len(payload) {
			break
		}
		val := payload[offset : offset+valLen]
		offset += valLen

		if offset+1 > len(payload) {
			break
		}
		isDelete := payload[offset] == 1
		offset += 1

		entries = append(entries, ValueLogEntry{
			Key:           append([]byte{}, key...),
			Value:         append([]byte{}, val...),
			TransactionID: txID,
			OperationID:   startOpID + opIdx,
			IsDelete:      isDelete,
		})
		opIdx++
	}
	return txID, startOpID, entries
}

func (wal *WriteAheadLog) Close() error {
	return wal.currentFile.Close()
}
