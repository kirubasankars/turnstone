// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package stonedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// WriteAheadLog handles append-only log files for crash recovery.
type WriteAheadLog struct {
	dir                string
	currentFile        *os.File
	currentStartOffset uint64 // Virtual offset of the start of the current file
	writeOffset        uint32 // Offset within the current file
	maxSize            uint32
	mu                 sync.Mutex

	// Rotation Indexing
	batchIndex map[uint64]WALLocation // Map BatchStartOpID -> Location (Active File)
	onRotate   func(map[uint64]WALLocation) error
}

func OpenWriteAheadLog(dir string, maxSize uint32) (*WriteAheadLog, error) {
	// Migration: Check if `dir` exists as a file (old single-file WAL)
	info, err := os.Stat(dir)
	if err == nil && !info.IsDir() {
		// Migrate: rename file to temp, mkdir, move file into dir as 00000000000000000000.wal
		tmpPath := dir + ".tmp"
		if err := os.Rename(dir, tmpPath); err != nil {
			return nil, fmt.Errorf("migrating WAL: rename failed: %w", err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("migrating WAL: mkdir failed: %w", err)
		}
		// Assuming start offset 0 for migrated file
		if err := os.Rename(tmpPath, filepath.Join(dir, fmt.Sprintf("%020d.wal", 0))); err != nil {
			return nil, fmt.Errorf("migrating WAL: move failed: %w", err)
		}
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	// Find max Offset to open the latest file
	matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return nil, err
	}

	var maxOffset uint64
	for _, m := range matches {
		base := filepath.Base(m)
		// strip extension
		name := strings.TrimSuffix(base, ".wal")
		if off, err := strconv.ParseUint(name, 10, 64); err == nil {
			if off > maxOffset {
				maxOffset = off
			}
		}
	}

	path := filepath.Join(dir, fmt.Sprintf("%020d.wal", maxOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
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
		currentStartOffset: maxOffset,
		writeOffset:        uint32(stat.Size()),
		maxSize:            maxSize,
		batchIndex:         make(map[uint64]WALLocation),
	}, nil
}

func (wal *WriteAheadLog) SetOnRotate(fn func(map[uint64]WALLocation) error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.onRotate = fn
}

func (wal *WriteAheadLog) FindInMemory(opID uint64) (WALLocation, bool) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Find the largest key <= opID
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

	// Check rotation
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

// AppendBatches writes multiple payloads to the WAL sequentially but performs only one Sync at the end.
// This is critical for Group Commit performance.
func (wal *WriteAheadLog) AppendBatches(payloads [][]byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, payload := range payloads {
		// Check rotation for each batch to ensure safety
		if wal.writeOffset > wal.maxSize {
			if err := wal.rotate(); err != nil {
				return err
			}
		}

		if err := wal.writeFrame(payload); err != nil {
			return err
		}
	}

	// Single Sync for the entire group
	return wal.currentFile.Sync()
}

// writeFrame writes a single Length+CRC+Payload frame to the current file.
// It assumes mu is held.
func (wal *WriteAheadLog) writeFrame(payload []byte) error {
	// Capture Index Information
	if len(payload) >= 16 {
		// Payload format: TxID(8) + StartOpID(8) + ...
		startOpID := binary.BigEndian.Uint64(payload[8:])
		wal.batchIndex[startOpID] = WALLocation{
			FileStartOffset: wal.currentStartOffset,
			RelativeOffset:  wal.writeOffset,
		}
	}

	var header [8]byte
	length := uint32(len(payload))
	// Use fast Castagnoli Table
	checksum := crc32.Checksum(payload, Crc32Table)

	binary.BigEndian.PutUint32(header[0:], length)
	binary.BigEndian.PutUint32(header[4:], checksum)

	n1, err := wal.currentFile.Write(header[:])
	if err != nil {
		return err
	}
	n2, err := wal.currentFile.Write(payload)
	if err != nil {
		return err
	}

	wal.writeOffset += uint32(n1 + n2)
	return nil
}

func (wal *WriteAheadLog) rotate() error {
	// Trigger the rotation hook to persist index
	if wal.onRotate != nil && len(wal.batchIndex) > 0 {
		if err := wal.onRotate(wal.batchIndex); err != nil {
			// Log error but proceed with rotation to avoid blocking system
			return fmt.Errorf("wal rotate hook failed: %w", err)
		}
	}
	// Reset memory index for next file
	wal.batchIndex = make(map[uint64]WALLocation)

	if err := wal.currentFile.Sync(); err != nil {
		return err
	}
	if err := wal.currentFile.Close(); err != nil {
		return err
	}

	// Calculate new offset: current base + current file size
	wal.currentStartOffset += uint64(wal.writeOffset)
	path := filepath.Join(wal.dir, fmt.Sprintf("%020d.wal", wal.currentStartOffset))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	wal.currentFile = f
	wal.writeOffset = 0
	return nil
}

// ReplaySinceTx scans ALL WAL files in numeric order and writes any batches newer than minTxID to the ValueLog.
// It supports truncation of corrupt WAL if truncateCorrupt is true.
func (wal *WriteAheadLog) ReplaySinceTx(vl *ValueLog, minTxID uint64, truncateCorrupt bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
	if err != nil {
		return err
	}

	sort.Slice(matches, func(i, j int) bool {
		ni := parseWALName(filepath.Base(matches[i]))
		nj := parseWALName(filepath.Base(matches[j]))
		return ni < nj
	})

	for i, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		// Only allow truncation if it is the last file in the sequence
		isLastFile := (i == len(matches)-1)

		err = wal.replayFile(f, path, vl, minTxID, truncateCorrupt, isLastFile, onReplay, onTruncate)
		f.Close()

		if err != nil {
			if err == ErrTruncated {
				// Stop replaying subsequent files to prevent causal history gaps
				fmt.Printf("Stopping WAL replay due to truncation in %s\n", path)
				return nil
			}
			return err
		}
	}

	wal.currentFile.Seek(0, 2)
	return nil
}

// Scan iterates WAL entries starting from a specific location (File+Offset).
func (wal *WriteAheadLog) Scan(startLoc WALLocation, fn func([]ValueLogEntry) error) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// 1. Identify all WAL files
	matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
	if err != nil {
		return err
	}

	// 2. Sort them
	sort.Slice(matches, func(i, j int) bool {
		ni := parseWALName(filepath.Base(matches[i]))
		nj := parseWALName(filepath.Base(matches[j]))
		return ni < nj
	})

	// Check if any log files exist
	if len(matches) == 0 {
		return ErrLogUnavailable
	}

	// Check if the requested start file is missing (older than the oldest available)
	firstAvailableOffset := parseWALName(filepath.Base(matches[0]))
	if startLoc.FileStartOffset < firstAvailableOffset {
		return ErrLogUnavailable
	}

	// 3. Find index of the file containing startLoc
	startIndex := -1
	for i, m := range matches {
		offset := parseWALName(filepath.Base(m))
		if offset == startLoc.FileStartOffset {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		// If exact file not found, find the one immediately preceding it
		// (e.g. if we compacted or user gave approximate ID)
		for i, m := range matches {
			offset := parseWALName(filepath.Base(m))
			if offset > startLoc.FileStartOffset {
				break
			}
			startIndex = i
		}
	}

	if startIndex == -1 {
		startIndex = 0
	}

	// 4. Iterate files from startIndex
	for i := startIndex; i < len(matches); i++ {
		path := matches[i]
		fileStart := parseWALName(filepath.Base(path))

		// IMPORTANT: Skip files that are strictly older than the requested start file
		// This prevents re-reading previous files if startIndex calculation was fuzzy
		if fileStart < startLoc.FileStartOffset {
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}

		// Calculate seek offset
		seekOff := int64(0)
		// Only seek if this is the specific start file
		if fileStart == startLoc.FileStartOffset {
			seekOff = int64(startLoc.RelativeOffset)
		}

		// Use a streamlined reader (similar to replayFile but simpler args)
		err = wal.scanFile(f, seekOff, fn)
		f.Close()
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

// PurgeOlderThan deletes WAL files where all operations are strictly less than minOpID.
func (wal *WriteAheadLog) PurgeOlderThan(minOpID uint64) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
	if err != nil {
		return err
	}

	// Sort files by offset (name)
	sort.Slice(matches, func(i, j int) bool {
		ni := parseWALName(filepath.Base(matches[i]))
		nj := parseWALName(filepath.Base(matches[j]))
		return ni < nj
	})

	if len(matches) <= 1 {
		return nil // Never delete the last active file
	}

	// Iterate up to the second to last file
	for i := 0; i < len(matches)-1; i++ {
		currentPath := matches[i]
		nextPath := matches[i+1]

		// To check if currentPath is fully obsolete, we check the START of nextPath.
		nextStartOpID, err := wal.readFirstOpID(nextPath)
		if err != nil {
			fmt.Printf("Purge stopped: cannot read header of %s: %v\n", filepath.Base(nextPath), err)
			break
		}

		if nextStartOpID <= minOpID {
			fmt.Printf("Purging WAL file %s (Next starts at %d <= %d)\n", filepath.Base(currentPath), nextStartOpID, minOpID)
			if err := os.Remove(currentPath); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// readFirstOpID reads the first batch header of a WAL file to determine its starting OperationID.
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
		return 0, fmt.Errorf("invalid batch length in header: %d", length)
	}

	buf := make([]byte, 16)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, err
	}

	startOpID := binary.BigEndian.Uint64(buf[8:])
	return startOpID, nil
}

// scanFile reads entries from a single WAL file starting at a given offset
func (wal *WriteAheadLog) scanFile(f *os.File, startOffset int64, fn func([]ValueLogEntry) error) error {
	if startOffset > 0 {
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		// Avoid seeking beyond EOF
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

func parseWALName(name string) uint64 {
	s := strings.TrimSuffix(name, ".wal")
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

// replayFile scans a WAL file and replays entries into the VLog. It handles corruption truncation.
func (wal *WriteAheadLog) replayFile(f *os.File, path string, vl *ValueLog, minTxID uint64, truncateCorrupt bool, isLastFile bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
	reader := bufio.NewReader(f)
	validOffset, err := wal.stream(reader, func(offset int64, payload []byte) error {
		txID, startOpID, entries := decodeWALBatchEntries(payload)

		// RECOVERY INDEXING:
		// If this is the active file, we MUST rebuild the in-memory index
		if filepath.Base(path) == filepath.Base(wal.currentFile.Name()) {
			wal.batchIndex[startOpID] = WALLocation{
				FileStartOffset: parseWALName(filepath.Base(path)),
				RelativeOffset:  uint32(offset),
			}
		}

		if txID > minTxID {
			fmt.Printf("Replaying WAL Batch TxID=%d from %s into ValueLog\n", txID, filepath.Base(path))
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
		// Handle truncation logic based on the error type returned by stream
		needsTruncate := false
		if err == io.ErrUnexpectedEOF || err == ErrChecksum || err == ErrCorruptData {
			// Only allow truncation if enabled AND it's the last file in the sequence.
			if truncateCorrupt && isLastFile {
				fmt.Printf("WAL corruption detected in LAST file %s at offset %d: %v. Truncating...\n", path, validOffset, err)
				needsTruncate = true
			} else {
				msg := "WAL corruption"
				if !isLastFile {
					msg = "WAL corruption in middle file (truncation forbidden)"
				}
				return fmt.Errorf("%s in %s at offset %d: %w", msg, path, validOffset, err)
			}
		} else if err != io.EOF {
			return err
		}

		if needsTruncate {
			// Re-open in RDWR to truncate
			fTrunc, err := os.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				return err
			}
			defer fTrunc.Close()
			if err := fTrunc.Truncate(validOffset); err != nil {
				return err
			}

			// If we truncated the *current* active file, update writeOffset
			if path == wal.currentFile.Name() {
				wal.writeOffset = uint32(validOffset)
			}

			// Trigger the callback to delete the index or handle other cleanup
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

// stream iterates over a WAL file reader, yielding valid payloads.
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

		// Sanity check
		if uint32(length) > wal.maxSize+1024*1024 {
			return validOffset, ErrCorruptData // Treat as corruption
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

// Helper to decode a WAL batch payload
func decodeWALBatchEntries(payload []byte) (uint64, uint64, []ValueLogEntry) {
	if len(payload) < WALBatchHeaderSize {
		return 0, 0, nil
	}
	txID := binary.BigEndian.Uint64(payload[0:])
	startOpID := binary.BigEndian.Uint64(payload[8:])
	// Count is at [16:20], implied by loop

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
			OperationID:   startOpID + opIdx, // FIXED: Removed +1 to match encoding
			IsDelete:      isDelete,
		})
		opIdx++
	}
	return txID, startOpID, entries
}

func (wal *WriteAheadLog) Close() error {
	return wal.currentFile.Close()
}
