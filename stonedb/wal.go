package stonedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// WriteAheadLog handles append-only log files for crash recovery.
type WriteAheadLog struct {
	dir                string
	currentFile        *os.File
	currentStartOffset uint64 // Virtual offset of the start of the current file
	writeOffset        uint32 // Offset within the current file
	maxSize            uint32
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

	matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return nil, err
	}

	// NOTE: Removed default override of 0 -> 1 for timeline. 0 is valid.

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
	return 0, off, nil // Changed default legacy timeline from 1 to 0
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

func (wal *WriteAheadLog) AppendBatch(payload []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.writeOffset > wal.maxSize {
		if err := wal.rotate(); err != nil {
			return err
		}
	}

	if err := wal.writeFrame(payload); err != nil {
		return err
	}

	return wal.currentFile.Sync()
}

func (wal *WriteAheadLog) AppendBatches(payloads [][]byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Capture state to attempt rollback on failure if possible
	startFile := wal.currentFile
	startOffset := wal.writeOffset
	rotated := false

	for _, payload := range payloads {
		if wal.writeOffset > wal.maxSize {
			if err := wal.rotate(); err != nil {
				return err
			}
			rotated = true
		}

		if err := wal.writeFrame(payload); err != nil {
			// If we haven't rotated, we can safely truncate back to the start of this batch group.
			// This prevents "Phantom Data" where WAL has entries but client was told failure.
			if !rotated && wal.currentFile == startFile {
				wal.truncateSilent(int64(startOffset))
				wal.writeOffset = startOffset
			}
			return err
		}
	}

	return wal.currentFile.Sync()
}

func (wal *WriteAheadLog) writeFrame(payload []byte) error {
	startOffset := wal.writeOffset

	var header [8]byte
	length := uint32(len(payload))
	checksum := crc32.Checksum(payload, Crc32Table)

	binary.BigEndian.PutUint32(header[0:], length)
	binary.BigEndian.PutUint32(header[4:], checksum)

	// Critical Section: Write I/O
	// If any write fails, we must truncate back to startOffset to prevent corrupt/partial frames.
	n1, err := wal.currentFile.Write(header[:])
	if err != nil {
		wal.truncateSilent(int64(startOffset))
		return err
	}
	n2, err := wal.currentFile.Write(payload)
	if err != nil {
		wal.truncateSilent(int64(startOffset))
		return err
	}

	// Only update offsets and index on success
	wal.writeOffset += uint32(n1 + n2)

	if len(payload) >= 16 {
		startOpID := binary.BigEndian.Uint64(payload[8:])
		wal.batchIndex[startOpID] = WALLocation{
			FileStartOffset: wal.currentStartOffset,
			RelativeOffset:  startOffset,
		}
	}

	return nil
}

// truncateSilent attempts to revert the file to a specific size.
// Errors are ignored as this is best-effort cleanup during an error path.
func (wal *WriteAheadLog) truncateSilent(offset int64) {
	_ = wal.currentFile.Truncate(offset)
	_, _ = wal.currentFile.Seek(offset, 0)
}

func (wal *WriteAheadLog) rotate() error {
	if wal.onRotate != nil && len(wal.batchIndex) > 0 {
		if err := wal.onRotate(wal.batchIndex); err != nil {
			return fmt.Errorf("wal rotate hook failed: %w", err)
		}
	}
	wal.batchIndex = make(map[uint64]WALLocation)

	if err := wal.currentFile.Sync(); err != nil {
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

	if err := wal.currentFile.Sync(); err != nil {
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

func (wal *WriteAheadLog) ReplaySinceTx(vl *ValueLog, minTxID uint64, truncateCorrupt bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
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

		err = wal.replayFile(f, path, vl, minTxID, truncateCorrupt, isLastFile, onReplay, onTruncate)
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

func (wal *WriteAheadLog) PurgeExpired(retention time.Duration, safeOpID uint64) error {
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

	now := time.Now()

	for i := 0; i < len(matches)-1; i++ {
		currentPath := matches[i]
		nextPath := matches[i+1]

		info, err := os.Stat(currentPath)
		if err != nil {
			return err
		}
		if now.Sub(info.ModTime()) < retention {
			break
		}

		nextStartOpID, err := wal.readFirstOpID(nextPath)
		if err != nil {
			break
		}

		if nextStartOpID <= safeOpID {
			// INFO: Garbage Collection
			wal.logger.Info("Purging expired WAL file", "file", filepath.Base(currentPath))
			if err := os.Remove(currentPath); err != nil {
				return err
			}
		} else {
			break
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

		nextStartOpID, err := wal.readFirstOpID(nextPath)
		if err != nil {
			break
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
	if length < WALBatchHeaderSize || uint32(length) > wal.maxSize+1024*1024 {
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

func (wal *WriteAheadLog) replayFile(f *os.File, path string, vl *ValueLog, minTxID uint64, truncateCorrupt bool, isLastFile bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
	reader := bufio.NewReader(f)
	validOffset, err := wal.stream(reader, func(offset int64, payload []byte) error {
		txID, startOpID, entries := decodeWALBatchEntries(payload)

		if filepath.Base(path) == filepath.Base(wal.currentFile.Name()) {
			wal.batchIndex[startOpID] = WALLocation{
				FileStartOffset: wal.currentStartOffset,
				RelativeOffset:  uint32(offset),
			}
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
		needsTruncate := false
		if err == io.ErrUnexpectedEOF || err == ErrChecksum || err == ErrCorruptData {
			if truncateCorrupt && isLastFile {
				// WARN: Corruption handled
				wal.logger.Warn("WAL corruption detected in LAST file (truncating)", "file", filepath.Base(path), "offset", validOffset, "err", err)
				needsTruncate = true
			} else {
				msg := "WAL corruption"
				if !isLastFile {
					msg = "WAL corruption in middle file (truncation forbidden)"
				}
				// ERROR: Fatal corruption
				wal.logger.Error(msg, "file", filepath.Base(path), "offset", validOffset, "err", err)
				return fmt.Errorf("%s in %s at offset %d: %w", msg, path, validOffset, err)
			}
		} else if err != io.EOF {
			return err
		}

		if needsTruncate {
			fTrunc, err := os.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				return err
			}
			defer fTrunc.Close()
			if err := fTrunc.Truncate(validOffset); err != nil {
				return err
			}

			if path == wal.currentFile.Name() {
				wal.writeOffset = uint32(validOffset)
			}

			if onTruncate != nil {
				if err := onTruncate(); err != nil {
					return err
				}
			}
			return ErrTruncated
		}
	}

	return nil
}

func (wal *WriteAheadLog) stream(r io.Reader, onPayload func(offset int64, payload []byte) error) (int64, error) {
	validOffset := int64(0)
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

		if uint32(length) > wal.maxSize+1024*1024 {
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
