package stonedb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
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
)

// parseVLogFileID extracts the numeric file ID from a VLog file path. Unlike
// fmt.Sscanf(name, "%04d.vlog", ...), the width modifier in "%04d" caps how
// many digits Sscanf will consume (4), so it silently fails to parse (and
// therefore silently skips) any file ID >= 10000 once rotation runs long
// enough to produce 5+ digit names -- those files would then vanish from
// maxFid/GetImmutableFileIDs and be excluded from Recover/Replay entirely.
// strconv.ParseUint has no such width limit.
func parseVLogFileID(path string) (uint32, error) {
	base := filepath.Base(path)
	s := strings.TrimSuffix(base, ".vlog")
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(n), nil
}

// sortVLogFiles orders VLog file paths numerically by file ID. A plain
// lexicographic sort.Strings only happens to match numeric order while every
// file ID has the same digit count (i.e. below 10000, given "%04d.vlog"
// zero-padding) -- once a 5-digit ID appears, e.g. "10000.vlog" sorts before
// "9999.vlog" lexicographically, which is backwards.
func sortVLogFiles(paths []string) {
	sort.Slice(paths, func(i, j int) bool {
		idI, _ := parseVLogFileID(paths[i])
		idJ, _ := parseVLogFileID(paths[j])
		return idI < idJ
	})
}

// cachedVLogFile is a refcounted, evictable read handle on a VLog file.
// getFileHandle hands these out (incrementing refs) to callers doing a
// ReadAt; evictOldest/DeleteFile/Close mark pendingClose and only actually
// close the underlying *os.File once every outstanding reader has released
// it. Without this, a concurrent ReadAt could still be in flight against an
// *os.File that eviction/deletion/compaction closed out from under it.
type cachedVLogFile struct {
	f            *os.File
	mu           sync.Mutex
	refs         int
	pendingClose bool
}

func (c *cachedVLogFile) acquire() {
	c.mu.Lock()
	c.refs++
	c.mu.Unlock()
}

// release drops a reference. If the handle has since been evicted/deleted
// and this was the last outstanding reference, it is closed here instead of
// at eviction time.
func (c *cachedVLogFile) release() {
	c.mu.Lock()
	c.refs--
	shouldClose := c.pendingClose && c.refs <= 0
	c.mu.Unlock()
	if shouldClose {
		c.f.Close()
	}
}

// markForClose flags the handle to be closed once its last active reader
// releases it (immediately, if there are no active readers right now).
func (c *cachedVLogFile) markForClose() {
	c.mu.Lock()
	c.pendingClose = true
	shouldClose := c.refs <= 0
	c.mu.Unlock()
	if shouldClose {
		c.f.Close()
	}
}

// ValueLog manages storage of values on disk in append-only files.
type ValueLog struct {
	dir          string
	currentFile  *os.File
	currentFid   uint32
	writeOffset  int64 // Updated to int64 for 64-bit offsets
	fileCache    map[uint32]*cachedVLogFile
	lruOrder     []uint32 // Ordered list of fileIDs for eviction (newest at end)
	maxOpenFiles int
	maxSize      int64 // Updated to int64
	mu           sync.RWMutex
	logger       *slog.Logger
}

func OpenValueLog(dir string, maxSize int64, logger *slog.Logger) (*ValueLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	// Safety check: ensure maxSize is reasonable (e.g. > 1MB)
	if maxSize <= 1024*1024 {
		maxSize = 200 * 1024 * 1024 // Default 200MB if invalid
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.vlog"))
	if err != nil {
		return nil, err
	}

	maxFid := uint32(0)
	for _, m := range matches {
		if fid, err := parseVLogFileID(m); err == nil {
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
		writeOffset:  stat.Size(), // int64
		fileCache:    make(map[uint32]*cachedVLogFile),
		lruOrder:     make([]uint32, 0),
		maxOpenFiles: DefaultValueLogMaxOpenFiles,
		maxSize:      maxSize,
		logger:       logger,
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
	sortVLogFiles(matches)

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
			// FIX: Handle ErrCorruptData (Garbage Header) as a truncate-able error if it's the last file.
			// This prevents crash loops when a header partial write results in huge length values.
			if err == ErrChecksum || err == io.ErrUnexpectedEOF || err == ErrCorruptData {
				if isLastFile {
					vl.logger.Warn("ValueLog corruption detected in last file. Truncating.", "file", path, "offset", validOffset, "err", err)
					if err := os.Truncate(path, validOffset); err != nil {
						return 0, 0, err
					}
					// If we just truncated the current file, update write offset
					if filepath.Base(path) == filepath.Base(vl.currentFile.Name()) {
						vl.writeOffset = validOffset
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

// CurrentFileID returns the ID of the currently active (writable) VLog file.
func (vl *ValueLog) CurrentFileID() uint32 {
	vl.mu.RLock()
	defer vl.mu.RUnlock()
	return vl.currentFid
}

// Sync fsyncs the currently active VLog file, making everything written to
// it so far durable.
func (vl *ValueLog) Sync() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.currentFile.Sync()
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
		if fid, err := parseVLogFileID(m); err == nil {
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

	// Mark the cached handle for close (deferred until any in-flight
	// ReadAt using it releases its reference) and update LRU.
	if c, ok := vl.fileCache[fileID]; ok {
		c.markForClose()
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
	vl.logger.Info("Deleting VLog file", "file_id", fileID)
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
			ValueOffset:   validOffset, // int64
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

// AppendEntries writes entries to the active ValueLog file.
// Returns FileID, StartOffset, Error.
func (vl *ValueLog) AppendEntries(entries []ValueLogEntry) (uint32, int64, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// Rotation Trigger: Max Size Check
	if vl.writeOffset > vl.maxSize {
		if err := vl.currentFile.Sync(); err != nil {
			return 0, 0, err
		}
		if err := vl.currentFile.Close(); err != nil {
			return 0, 0, err
		}
		vl.currentFid++
		path := filepath.Join(vl.dir, fmt.Sprintf("%04d.vlog", vl.currentFid))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return 0, 0, err
		}
		vl.currentFile = f
		vl.writeOffset = 0
	}

	startOffset := vl.writeOffset
	var buf bytes.Buffer

	for _, e := range entries {
		// Prepare Header (KeyLen + ValLen + TxID + OpID + Type) = 25 bytes
		var header [25]byte
		binary.BigEndian.PutUint32(header[0:], uint32(len(e.Key)))
		binary.BigEndian.PutUint32(header[4:], uint32(len(e.Value)))
		binary.BigEndian.PutUint64(header[8:], e.TransactionID)
		binary.BigEndian.PutUint64(header[16:], e.OperationID)
		if e.IsDelete {
			header[24] = 1
		} else {
			header[24] = 0 // Explicitly set 0 for clarity
		}

		// Calculate CRC without intermediate allocation
		crc := crc32.New(Crc32Table)
		crc.Write(header[:])
		crc.Write(e.Key)
		crc.Write(e.Value)
		sum := crc.Sum32()

		// Write [CRC][Header][Key][Value] to main buffer
		binary.Write(&buf, binary.BigEndian, sum)
		buf.Write(header[:])
		buf.Write(e.Key)
		buf.Write(e.Value)
	}

	n, err := vl.currentFile.Write(buf.Bytes())
	if n > 0 {
		// Advance writeOffset by whatever was actually written to the file
		// *before* checking err: os.File.Write can return n > 0 alongside a
		// non-nil error on a partial write. If we left writeOffset at its
		// old value here, the next AppendEntries call would use it as its
		// startOffset -- but the file's real EOF is now n bytes further
		// along (this partial write's leftover garbage), so the next
		// entry's header/data would be recorded in the index at an offset
		// that doesn't actually point at what was written, corrupting the
		// next read.
		vl.writeOffset += int64(n)
	}
	if err != nil {
		// Best-effort: truncate the partial write away so the file and our
		// tracked offset agree again, instead of leaving a torn record for
		// the next append (or a future Recover scan) to trip over.
		if truncErr := vl.currentFile.Truncate(startOffset); truncErr == nil {
			vl.currentFile.Seek(0, io.SeekEnd)
			vl.writeOffset = startOffset
		} else {
			vl.logger.Error("VLog partial write recovery: truncate failed, writeOffset may be desynced from file", "err", truncErr)
		}
		return 0, 0, err
	}

	return vl.currentFid, startOffset, nil
}

func (vl *ValueLog) ReadValue(fileID uint32, offset int64, valLen uint32) ([]byte, error) {
	c, err := vl.getFileHandle(fileID)
	if err != nil {
		return nil, err
	}
	defer c.release()
	f := c.f

	header := make([]byte, ValueLogHeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		return nil, err
	}

	crcStored := binary.BigEndian.Uint32(header[0:])
	keyLen := binary.BigEndian.Uint32(header[4:])

	totalLen := ValueLogHeaderSize + keyLen + valLen
	data := make([]byte, totalLen)

	if _, err := f.ReadAt(data, offset); err != nil {
		return nil, err
	}

	payload := data[4:]
	if crc32.Checksum(payload, Crc32Table) != crcStored {
		return nil, ErrChecksum
	}

	valStart := ValueLogHeaderSize + keyLen
	return data[valStart : valStart+valLen], nil
}

// getFileHandle returns a refcounted read handle for fileID, always through
// a dedicated read-only *os.File independent of vl.currentFile -- including
// for the currently-active file. This matters because vl.currentFile is the
// live writer fd that Rotate()/AppendEntries's size-triggered rotation can
// close out from under a caller at any time; handing that same fd out for
// reads would let a concurrent rotation close it mid-ReadAt. Every returned
// handle must be released via its release() method.
func (vl *ValueLog) getFileHandle(fileID uint32) (*cachedVLogFile, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	if c, ok := vl.fileCache[fileID]; ok {
		vl.moveToBack(fileID)
		c.acquire()
		return c, nil
	}

	if len(vl.fileCache) >= vl.maxOpenFiles {
		vl.evictOldest()
	}

	path := filepath.Join(vl.dir, fmt.Sprintf("%04d.vlog", fileID))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	c := &cachedVLogFile{f: f}
	c.acquire()
	vl.fileCache[fileID] = c
	vl.lruOrder = append(vl.lruOrder, fileID)
	return c, nil
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

	if c, ok := vl.fileCache[oldestID]; ok {
		c.markForClose()
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
	sortVLogFiles(matches)

	for _, path := range matches {
		fileID, err := parseVLogFileID(path)
		if err != nil {
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

func (vl *ValueLog) Close() error {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	if err := vl.currentFile.Close(); err != nil {
		return err
	}
	for _, c := range vl.fileCache {
		c.markForClose()
	}
	// Clear cache references
	vl.fileCache = make(map[uint32]*cachedVLogFile)
	vl.lruOrder = nil
	return nil
}
