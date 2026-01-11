package stonedb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// ValueLog manages storage of values on disk in append-only files.
type ValueLog struct {
	dir          string
	currentFile  *os.File
	currentFid   uint32
	writeOffset  uint32
	fileCache    map[uint32]*os.File
	lruOrder     []uint32 // Ordered list of fileIDs for eviction (newest at end)
	maxOpenFiles int
	mu           sync.RWMutex // Note: Lock() is used for LRU updates
}

func OpenValueLog(dir string) (*ValueLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.vlog"))
	if err != nil {
		return nil, err
	}

	maxFid := uint32(0)
	for _, m := range matches {
		var fid uint32
		if _, err := fmt.Sscanf(filepath.Base(m), "%04d.vlog", &fid); err == nil {
			if fid > maxFid {
				maxFid = fid
			}
		}
	}

	path := filepath.Join(dir, fmt.Sprintf("%04d.vlog", maxFid))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &ValueLog{
		dir:          dir,
		currentFile:  f,
		currentFid:   maxFid,
		writeOffset:  uint32(stat.Size()),
		fileCache:    make(map[uint32]*os.File),
		lruOrder:     make([]uint32, 0),
		maxOpenFiles: DefaultValueLogMaxOpenFiles,
	}, nil
}

// Recover scans all ValueLog files, finds max sequences, and truncates the last file if corrupt.
func (vl *ValueLog) Recover() (uint64, uint64, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(vl.dir, "*.vlog"))
	if err != nil {
		return 0, 0, err
	}
	sort.Strings(matches)

	maxTx := uint64(0)
	maxOp := uint64(0)

	for i, path := range matches {
		isLastFile := (i == len(matches)-1)

		f, err := os.OpenFile(path, os.O_RDWR, 0o644) // Open RDWR to allow truncation
		if err != nil {
			return 0, 0, err
		}

		// Use the shared stream iterator
		reader := bufio.NewReader(f)
		validOffset, mt, mo, err := vl.stream(reader, func(_ int64, _ ValueLogEntry, _ EntryMeta) error { return nil })
		f.Close()

		if mt > maxTx {
			maxTx = mt
		}
		if mo > maxOp {
			maxOp = mo
		}

		if err != nil {
			if err == ErrChecksum || err == io.ErrUnexpectedEOF {
				if isLastFile {
					fmt.Printf("ValueLog corruption detected in last file %s. Truncating to %d\n", path, validOffset)
					if err := os.Truncate(path, validOffset); err != nil {
						return 0, 0, err
					}
					// If we just truncated the current file, update write offset
					if filepath.Base(path) == filepath.Base(vl.currentFile.Name()) {
						vl.writeOffset = uint32(validOffset)
						// Need to re-seek current handle
						vl.currentFile.Seek(validOffset, 0)
					}
				} else {
					return 0, 0, fmt.Errorf("fatal corruption in older vlog file %s: %w", path, err)
				}
			} else if err != io.EOF {
				return 0, 0, err
			}
		}
	}
	return maxTx, maxOp, nil
}

func (vl *ValueLog) Rotate() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// If current file is empty, no need to rotate (safe optimization)
	if vl.writeOffset == 0 {
		return nil
	}

	if err := vl.currentFile.Sync(); err != nil {
		return err
	}
	if err := vl.currentFile.Close(); err != nil {
		return err
	}

	vl.currentFid++
	path := filepath.Join(vl.dir, fmt.Sprintf("%04d.vlog", vl.currentFid))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	vl.currentFile = f
	vl.writeOffset = 0
	return nil
}

// IterateFile scans a specific ValueLog file and invokes the callback for each valid entry.
func (vl *ValueLog) IterateFile(fileID uint32, fn func(ValueLogEntry, EntryMeta) error) error {
	vl.mu.RLock()
	// Check if file exists in dir
	path := filepath.Join(vl.dir, fmt.Sprintf("%04d.vlog", fileID))
	_, err := os.Stat(path)
	vl.mu.RUnlock()
	if err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	_, _, _, err = vl.stream(reader, func(offset int64, e ValueLogEntry, m EntryMeta) error {
		// Override FileID in case it's different from the one in stream (though stream doesn't set FileID)
		m.FileID = fileID
		return fn(e, m)
	})

	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// GetImmutableFileIDs returns a sorted list of all VLog file IDs except the current active one.
func (vl *ValueLog) GetImmutableFileIDs() ([]uint32, error) {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	matches, err := filepath.Glob(filepath.Join(vl.dir, "*.vlog"))
	if err != nil {
		return nil, err
	}

	var ids []uint32
	for _, m := range matches {
		var fid uint32
		if _, err := fmt.Sscanf(filepath.Base(m), "%04d.vlog", &fid); err == nil {
			if fid != vl.currentFid {
				ids = append(ids, fid)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// DeleteFile removes a specific ValueLog file from disk.
func (vl *ValueLog) DeleteFile(fileID uint32) error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// Cannot delete active file
	if fileID == vl.currentFid {
		return errors.New("cannot delete active file")
	}

	// Close cached handle and update LRU
	if f, ok := vl.fileCache[fileID]; ok {
		f.Close()
		delete(vl.fileCache, fileID)
		// Remove from LRU list
		for i, id := range vl.lruOrder {
			if id == fileID {
				vl.lruOrder = append(vl.lruOrder[:i], vl.lruOrder[i+1:]...)
				break
			}
		}
	}

	path := filepath.Join(vl.dir, fmt.Sprintf("%04d.vlog", fileID))
	return os.Remove(path)
}

// stream iterates over a ValueLog reader, parsing entries and invoking callback.
// Returns validOffset, maxTxID, maxOpID, and the first error encountered.
func (vl *ValueLog) stream(r io.Reader, onEntry func(offset int64, e ValueLogEntry, m EntryMeta) error) (int64, uint64, uint64, error) {
	validOffset := int64(0)
	maxTx := uint64(0)
	maxOp := uint64(0)

	for {
		header := make([]byte, ValueLogHeaderSize)
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF {
				return validOffset, maxTx, maxOp, io.EOF
			}
			return validOffset, maxTx, maxOp, io.ErrUnexpectedEOF
		}

		crcStored := binary.BigEndian.Uint32(header[0:])
		keyLen := binary.BigEndian.Uint32(header[4:])
		valLen := binary.BigEndian.Uint32(header[8:])
		txID := binary.BigEndian.Uint64(header[12:])
		opID := binary.BigEndian.Uint64(header[20:])
		typeByte := header[28]

		kvLen := int(keyLen + valLen)
		kvBuf := make([]byte, kvLen)
		if _, err := io.ReadFull(r, kvBuf); err != nil {
			return validOffset, maxTx, maxOp, io.ErrUnexpectedEOF
		}

		// Verify Checksum
		payload := make([]byte, ValueLogHeaderSize-4+uint32(kvLen))
		copy(payload, header[4:])
		copy(payload[25:], kvBuf)

		if crc32.Checksum(payload, Crc32Table) != crcStored {
			return validOffset, maxTx, maxOp, ErrChecksum
		}

		if txID > maxTx {
			maxTx = txID
		}
		if opID > maxOp {
			maxOp = opID
		}

		key := kvBuf[:keyLen]
		val := kvBuf[keyLen:]

		entry := ValueLogEntry{
			Key:           key,
			Value:         val,
			TransactionID: txID,
			OperationID:   opID,
			IsDelete:      typeByte == 1,
		}

		meta := EntryMeta{
			// FileID must be set by caller usually
			ValueOffset:   uint32(validOffset),
			ValueLen:      valLen,
			TransactionID: txID,
			OperationID:   opID,
			IsTombstone:   typeByte == 1,
		}

		if err := onEntry(validOffset, entry, meta); err != nil {
			return validOffset, maxTx, maxOp, err
		}

		validOffset += int64(ValueLogHeaderSize) + int64(kvLen)
	}
}

func (vl *ValueLog) AppendEntries(entries []ValueLogEntry) (uint32, uint32, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	startOffset := vl.writeOffset
	var buf bytes.Buffer

	for _, e := range entries {
		payloadBuf := new(bytes.Buffer)

		binary.Write(payloadBuf, binary.BigEndian, uint32(len(e.Key)))
		binary.Write(payloadBuf, binary.BigEndian, uint32(len(e.Value)))
		binary.Write(payloadBuf, binary.BigEndian, e.TransactionID)
		binary.Write(payloadBuf, binary.BigEndian, e.OperationID)

		if e.IsDelete {
			payloadBuf.WriteByte(1)
		} else {
			payloadBuf.WriteByte(0)
		}

		payloadBuf.Write(e.Key)
		payloadBuf.Write(e.Value)

		payload := payloadBuf.Bytes()
		crc := crc32.Checksum(payload, Crc32Table)

		binary.Write(&buf, binary.BigEndian, crc)
		buf.Write(payload)
	}

	n, err := vl.currentFile.Write(buf.Bytes())
	if err != nil {
		return 0, 0, err
	}

	vl.writeOffset += uint32(n)
	return vl.currentFid, startOffset, nil
}

func (vl *ValueLog) ReadValue(fileID uint32, offset uint32, valLen uint32) ([]byte, error) {
	f, err := vl.getFileHandle(fileID)
	if err != nil {
		return nil, err
	}

	header := make([]byte, ValueLogHeaderSize)
	if _, err := f.ReadAt(header, int64(offset)); err != nil {
		return nil, err
	}

	crcStored := binary.BigEndian.Uint32(header[0:])
	keyLen := binary.BigEndian.Uint32(header[4:])

	totalLen := ValueLogHeaderSize + keyLen + valLen
	data := make([]byte, totalLen)

	if _, err := f.ReadAt(data, int64(offset)); err != nil {
		return nil, err
	}

	payload := data[4:]
	if crc32.Checksum(payload, Crc32Table) != crcStored {
		return nil, ErrChecksum
	}

	valStart := ValueLogHeaderSize + keyLen
	return data[valStart : valStart+valLen], nil
}

func (vl *ValueLog) getFileHandle(fileID uint32) (*os.File, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// 1. Check Active File
	if fileID == vl.currentFid {
		return vl.currentFile, nil
	}

	// 2. Check Cache
	if f, ok := vl.fileCache[fileID]; ok {
		vl.moveToBack(fileID)
		return f, nil
	}

	// 3. Evict if needed
	if len(vl.fileCache) >= vl.maxOpenFiles {
		vl.evictOldest()
	}

	// 4. Open File
	path := filepath.Join(vl.dir, fmt.Sprintf("%04d.vlog", fileID))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	vl.fileCache[fileID] = f
	vl.lruOrder = append(vl.lruOrder, fileID)
	return f, nil
}

func (vl *ValueLog) moveToBack(fileID uint32) {
	// Simple slice manipulation to move accessed item to end
	if len(vl.lruOrder) > 0 && vl.lruOrder[len(vl.lruOrder)-1] == fileID {
		return // Already at back
	}
	for i, id := range vl.lruOrder {
		if id == fileID {
			vl.lruOrder = append(vl.lruOrder[:i], vl.lruOrder[i+1:]...)
			vl.lruOrder = append(vl.lruOrder, fileID)
			return
		}
	}
}

func (vl *ValueLog) evictOldest() {
	if len(vl.lruOrder) == 0 {
		return
	}
	oldestID := vl.lruOrder[0]
	vl.lruOrder = vl.lruOrder[1:]

	if f, ok := vl.fileCache[oldestID]; ok {
		f.Close()
		delete(vl.fileCache, oldestID)
	}
}

func (vl *ValueLog) Replay(maxTxID uint64, fn func(ValueLogEntry, EntryMeta) error) error {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	matches, err := filepath.Glob(filepath.Join(vl.dir, "*.vlog"))
	if err != nil {
		return err
	}
	sort.Strings(matches)

	for _, path := range matches {
		base := filepath.Base(path)
		var fileID uint32
		if _, err := fmt.Sscanf(base, "%04d.vlog", &fileID); err != nil {
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		reader := bufio.NewReader(f)
		_, _, _, err = vl.stream(reader, func(offset int64, e ValueLogEntry, meta EntryMeta) error {
			if e.TransactionID > maxTxID {
				// We still need to reconstruct EntryMeta properly with FileID for the callback
				meta.FileID = fileID
				return fn(e, meta)
			}
			return nil
		})

		if err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

func (vl *ValueLog) Truncate(offset uint32) error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// 1. Truncate the file physically
	if err := vl.currentFile.Truncate(int64(offset)); err != nil {
		return err
	}

	// 2. Reset the file pointer (crucial for next write)
	if _, err := vl.currentFile.Seek(int64(offset), 0); err != nil {
		return err
	}

	// 3. Reset internal offset state
	vl.writeOffset = offset
	return nil
}

func (vl *ValueLog) Close() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	if err := vl.currentFile.Close(); err != nil {
		return err
	}
	for _, f := range vl.fileCache {
		f.Close()
	}
	// Clear cache references
	vl.fileCache = make(map[uint32]*os.File)
	vl.lruOrder = nil
	return nil
}
