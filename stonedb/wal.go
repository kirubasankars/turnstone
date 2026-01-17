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
    "time"
)

// WriteAheadLog handles append-only log files for crash recovery.
// It ensures ACID durability by persisting transaction data to disk before
// the in-memory index or ValueLog is updated.
//
// It supports:
// 1. Automatic File Rotation based on size.
// 2. Timeline Branching for Failover support (Postgres-style).
// 3. Time-based retention policies.
type WriteAheadLog struct {
    dir                string
    currentFile        *os.File
    currentStartOffset uint64 // Virtual offset of the start of the current file
    writeOffset        uint32 // Offset within the current file
    maxSize            uint32
    mu                 sync.Mutex

    // Timeline Support: The current generation ID of the history.
    // This increments when a replica is promoted to a leader to prevent
    // split-brain history corruption in backups.
    timelineID uint64

    // Rotation Indexing
    // Maps BatchStartOpID -> Location. Used to persist an index of where
    // batches are located on disk during rotation.
    batchIndex map[uint64]WALLocation
    onRotate   func(map[uint64]WALLocation) error
}

// parseWALFilename extracts (TimelineID, StartOffset) from a filename.
// Supports both legacy format ("{Offset}.wal") and new format ("wal_{TL}_{Offset}.wal").
func parseWALFilename(name string) (uint64, uint64, error) {
    base := filepath.Base(name)

    // New Format: wal_1_0000.wal
    if strings.HasPrefix(base, "wal_") {
        var tl, off uint64
        // Scan for wal_{TL}_{Offset}.wal
        // Using Sscanf is simple but strictly expects the pattern.
        // We handle the .wal suffix manually to be safe.
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

    // Legacy Format: 00000000.wal (Implies Timeline 1)
    s := strings.TrimSuffix(base, ".wal")
    off, err := strconv.ParseUint(s, 10, 64)
    if err != nil {
        return 0, 0, err
    }
    return 1, off, nil
}

// sortWALFiles sorts file paths by TimelineID (asc) then StartOffset (asc).
// This ensures that during recovery, we process history in the correct order,
// respecting forks in the timeline.
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

// OpenWriteAheadLog initializes the WAL subsystem.
// It scans the directory for existing logs to recover the state.
// requestedTL comes from timeline.meta. We use it to ensure new files
// are created on the correct timeline (e.g., after a restart or crash).
func OpenWriteAheadLog(dir string, maxSize uint32, requestedTL uint64) (*WriteAheadLog, error) {
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return nil, err
    }

    matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
    if err != nil {
        return nil, err
    }

    // Default to Timeline 1 if nothing specified
    if requestedTL == 0 {
        requestedTL = 1
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
        // that was already promoted. We respect the disk state to avoid overwriting history.
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
    }, nil
}

// SetOnRotate registers a callback that is invoked whenever the WAL rotates.
// This is typically used to persist the WAL index to LevelDB.
func (wal *WriteAheadLog) SetOnRotate(fn func(map[uint64]WALLocation) error) {
    wal.mu.Lock()
    defer wal.mu.Unlock()
    wal.onRotate = fn
}

// FindInMemory searches the current active file's memory index for a given OpID.
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

// AppendBatch writes a single binary payload (a transaction or batch of ops) to the WAL.
// It handles framing, CRC calculation, file rotation, and fsync.
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

// AppendBatches writes multiple payloads to the WAL sequentially.
// Optimization: It performs only ONE fsync() at the end, enabling Group Commit.
func (wal *WriteAheadLog) AppendBatches(payloads [][]byte) error {
    wal.mu.Lock()
    defer wal.mu.Unlock()

    for _, payload := range payloads {
        if wal.writeOffset > wal.maxSize {
            if err := wal.rotate(); err != nil {
                return err
            }
        }

        if err := wal.writeFrame(payload); err != nil {
            return err
        }
    }

    return wal.currentFile.Sync()
}

// writeFrame encapsulates the low-level serialization of a WAL frame:
// [Length(4)] [CRC(4)] [Payload]
func (wal *WriteAheadLog) writeFrame(payload []byte) error {
    if len(payload) >= 16 {
        startOpID := binary.BigEndian.Uint64(payload[8:])
        wal.batchIndex[startOpID] = WALLocation{
            FileStartOffset: wal.currentStartOffset,
            RelativeOffset:  wal.writeOffset,
        }
    }

    var header [8]byte
    length := uint32(len(payload))
    checksum := crc32.Checksum(payload, Crc32Table)

    binary.BigEndian.PutUint32(header[0:], length)
    binary.BigEndian.PutUint32(header[4:], checksum)

    n1, err := wal.currentFile.Write(header[:])
    if err != nil {
        wal.currentFile.Seek(int64(wal.writeOffset), 0)
        return err
    }
    n2, err := wal.currentFile.Write(payload)
    if err != nil {
        wal.currentFile.Seek(int64(wal.writeOffset), 0)
        return err
    }

    wal.writeOffset += uint32(n1 + n2)
    return nil
}

// rotate closes the current file and opens the next one in the sequence.
// It preserves the current TimelineID.
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
    // Construct filename using current TimelineID
    path := filepath.Join(wal.dir, fmt.Sprintf("wal_%d_%020d.wal", wal.timelineID, wal.currentStartOffset))

    f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
    if err != nil {
        return err
    }

    wal.currentFile = f
    wal.writeOffset = 0
    return nil
}

// ForceNewTimeline switches to a new Timeline ID immediately.
// This is the core mechanism for Failover/Promotion.
// It effectively "Forks" the history by closing the current file (regardless of size)
// and starting a new file stamped with `newTimelineID`.
func (wal *WriteAheadLog) ForceNewTimeline(newTimelineID uint64) error {
    wal.mu.Lock()
    defer wal.mu.Unlock()

    if newTimelineID <= wal.timelineID {
        return fmt.Errorf("new timeline %d must be greater than current %d", newTimelineID, wal.timelineID)
    }

    // 1. Flush & Close current file
    if err := wal.currentFile.Sync(); err != nil {
        return err
    }
    if err := wal.currentFile.Close(); err != nil {
        return err
    }

    // 2. Persist Index for the closed file
    if wal.onRotate != nil && len(wal.batchIndex) > 0 {
        if err := wal.onRotate(wal.batchIndex); err != nil {
            return fmt.Errorf("wal rotate hook failed: %w", err)
        }
    }
    wal.batchIndex = make(map[uint64]WALLocation)

    // 3. Switch State
    wal.timelineID = newTimelineID
    // Continuity: The new file starts logically where the old one ended
    wal.currentStartOffset += uint64(wal.writeOffset)
    wal.writeOffset = 0

    // 4. Create New File
    path := filepath.Join(wal.dir, fmt.Sprintf("wal_%d_%020d.wal", wal.timelineID, wal.currentStartOffset))
    f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
    if err != nil {
        return err
    }

    wal.currentFile = f
    return nil
}

// ReplaySinceTx scans ALL WAL files in numeric/timeline order and replays batches
// newer than minTxID into the ValueLog.
//
// It handles:
// - Timeline Forks (by sorting logic).
// - Corruption (by truncating the last file if `truncateCorrupt` is true).
func (wal *WriteAheadLog) ReplaySinceTx(vl *ValueLog, minTxID uint64, truncateCorrupt bool, onReplay func([]ValueLogEntry), onTruncate func() error) error {
    wal.mu.Lock()
    defer wal.mu.Unlock()

    matches, err := filepath.Glob(filepath.Join(wal.dir, "*.wal"))
    if err != nil {
        return err
    }

    // Updated sort logic for Timelines
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
                fmt.Printf("Stopping WAL replay due to truncation in %s\n", path)
                return nil
            }
            return err
        }
    }

    wal.currentFile.Seek(0, 2)
    return nil
}

// Scan iterates WAL entries starting from a specific logical location.
// Used by Replication to stream history to followers.
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

    // Parse first file to see if startLoc is valid
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

    // Fuzzy matching if exact file missing
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

// PurgeExpired deletes WAL files that are older than `retention` AND are fully checkpointed.
// It checks the *next* file's StartOpID to ensure the current file contains no data
// required by the checkpoint (`safeOpID`).
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
            if err := os.Remove(currentPath); err != nil {
                return err
            }
        } else {
            break
        }
    }
    return nil
}

// PurgeOlderThan deletes WAL files strictly older than minOpID.
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
        return 0, fmt.Errorf("invalid batch length: %d", length)
    }

    buf := make([]byte, 16)
    if _, err := io.ReadFull(f, buf); err != nil {
        return 0, err
    }

    startOpID := binary.BigEndian.Uint64(buf[8:])
    return startOpID, nil
}

// scanFile reads entries from a single WAL file starting at a given offset.
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
                // Note: using currentStartOffset is safe because replay runs on startup
                // where currentFile is the one being replayed (last in list)
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


