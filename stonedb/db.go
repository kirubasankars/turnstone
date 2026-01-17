package stonedb

import (
    "bytes"
    "encoding/binary"
    "encoding/json"
    "errors"
    "fmt"
    "math"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
    "github.com/syndtr/goleveldb/leveldb/util"
)

type commitRequest struct {
    tx   *Transaction
    resp chan error
}

type TimelineHistoryItem struct {
    TLI     uint64 `json:"tli"`
    StartOp uint64 `json:"start_op"`
    EndOp   uint64 `json:"end_op"`
}

type TimelineMeta struct {
    CurrentTimeline uint64                `json:"current_timeline"`
    History         []TimelineHistoryItem `json:"history"`
}

// DB is the main database struct.
type DB struct {
    dir                string
    ldb                *leveldb.DB
    writeAheadLog      *WriteAheadLog
    valueLog           *ValueLog
    deletedBytesByFile map[uint32]int64

    mu       sync.RWMutex
    commitMu sync.Mutex

    // Clocks
    transactionID uint64
    operationID   uint64
    keyCount      int64 // Persistent Key Count

    // Metrics (Atomic counters)
    metricsConflicts uint64

    // Transaction State
    activeTxnsMu   sync.Mutex
    activeTxns     map[*Transaction]uint64
    pendingDeletes []pendingFile

    // Background Tasks
    closeCh      chan struct{}
    wg           sync.WaitGroup
    lastCkptOpID uint64
    closed       int32

    // Group Commit Pipeline
    commitCh chan commitRequest

    // Config
    minGarbageThreshold    int64
    checksumInterval       time.Duration
    autoCheckpointInterval time.Duration
    compactionInterval     time.Duration
    walRetentionTime       time.Duration
    maxDiskUsagePercent    int   // Configured threshold
    isDiskFull             int32 // Atomic boolean (1=Full, 0=OK)

    // Timeline Meta
    timelineMeta TimelineMeta
}

// Open initializes the DB.
func Open(dir string, opts Options) (*DB, error) {
    if err := os.MkdirAll(dir, dirMode); err != nil {
        return nil, err
    }

    if opts.MaxWALSize == 0 {
        opts.MaxWALSize = 10 * 1024 * 1024
    }
    if opts.CompactionMinGarbage == 0 {
        opts.CompactionMinGarbage = 1024 * 1024
    }
    if opts.WALRetentionTime == 0 {
        opts.WALRetentionTime = 2 * time.Hour
    }
    if opts.AutoCheckpointInterval == 0 {
        opts.AutoCheckpointInterval = 60 * time.Second
    }
    if opts.CompactionInterval == 0 {
        opts.CompactionInterval = 20 * time.Second
    }

    meta, err := loadTimelineMeta(dir)
    if err != nil {
        return nil, fmt.Errorf("load timeline meta: %w", err)
    }

    wal, err := OpenWriteAheadLog(filepath.Join(dir, "wal"), opts.MaxWALSize, meta.CurrentTimeline)
    if err != nil {
        return nil, fmt.Errorf("open wal: %w", err)
    }

    vl, err := OpenValueLog(filepath.Join(dir, "vlog"))
    if err != nil {
        _ = wal.Close()
        return nil, fmt.Errorf("open vlog: %w", err)
    }

    db := &DB{
        dir:                    dir,
        writeAheadLog:          wal,
        valueLog:               vl,
        deletedBytesByFile:     make(map[uint32]int64),
        activeTxns:             make(map[*Transaction]uint64),
        closeCh:                make(chan struct{}),
        commitCh:               make(chan commitRequest, 500),
        minGarbageThreshold:    opts.CompactionMinGarbage,
        checksumInterval:       opts.ChecksumInterval,
        autoCheckpointInterval: opts.AutoCheckpointInterval,
        compactionInterval:     opts.CompactionInterval,
        walRetentionTime:       opts.WALRetentionTime,
        maxDiskUsagePercent:    opts.MaxDiskUsagePercent,
        timelineMeta:           meta,
    }

    if err := db.recoverValueLog(); err != nil {
        db.Close()
        return nil, fmt.Errorf("recover vlog: %w", err)
    }

    if err := db.syncWALToValueLog(opts.TruncateCorruptWAL); err != nil {
        db.Close()
        return nil, fmt.Errorf("sync wal: %w", err)
    }

    if err := db.openLevelDB(dir); err != nil {
        db.Close()
        return nil, err
    }

    db.writeAheadLog.SetOnRotate(db.onWALRotate)

    if err := db.loadDeletedBytesStats(); err != nil {
        fmt.Printf("Warning: failed to load garbage stats: %v\n", err)
    }
    if err := db.loadKeyCount(); err != nil {
        fmt.Printf("Warning: failed to load key count (recounting): %v\n", err)
    }

    db.startBackgroundTasks()

    return db, nil
}

func loadTimelineMeta(dir string) (TimelineMeta, error) {
    path := filepath.Join(dir, "timeline.meta")
    data, err := os.ReadFile(path)
    if os.IsNotExist(err) {
        return TimelineMeta{CurrentTimeline: 1}, nil
    }
    if err != nil {
        return TimelineMeta{}, err
    }
    var meta TimelineMeta
    if err := json.Unmarshal(data, &meta); err != nil {
        return TimelineMeta{}, err
    }
    if meta.CurrentTimeline == 0 {
        meta.CurrentTimeline = 1
    }
    return meta, nil
}

func (db *DB) saveTimelineMeta() error {
    path := filepath.Join(db.dir, "timeline.meta")
    data, err := json.MarshalIndent(db.timelineMeta, "", "  ")
    if err != nil {
        return err
    }
    tmp := path + ".tmp"
    if err := os.WriteFile(tmp, data, 0644); err != nil {
        return err
    }
    return os.Rename(tmp, path)
}

func (db *DB) Promote() error {
    db.mu.Lock()
    defer db.mu.Unlock()

    currentTL := db.timelineMeta.CurrentTimeline
    newTL := currentTL + 1
    lastOp := atomic.LoadUint64(&db.operationID)

    historyEntry := TimelineHistoryItem{
        TLI:     currentTL,
        StartOp: 0,
        EndOp:   lastOp,
    }
    if len(db.timelineMeta.History) > 0 {
        historyEntry.StartOp = db.timelineMeta.History[len(db.timelineMeta.History)-1].EndOp
    }

    db.timelineMeta.History = append(db.timelineMeta.History, historyEntry)
    db.timelineMeta.CurrentTimeline = newTL

    if err := db.saveTimelineMeta(); err != nil {
        return fmt.Errorf("failed to save timeline meta: %w", err)
    }

    if err := db.writeAheadLog.ForceNewTimeline(newTL); err != nil {
        return fmt.Errorf("failed to switch WAL timeline: %w", err)
    }

    fmt.Printf("Promoted to Timeline %d at OpID %d\n", newTL, lastOp)
    return nil
}

func (db *DB) ForceSetClocks(txID, opID uint64) {
    currentTx := atomic.LoadUint64(&db.transactionID)
    if txID > currentTx {
        atomic.StoreUint64(&db.transactionID, txID)
    }
    currentOp := atomic.LoadUint64(&db.operationID)
    if opID > currentOp {
        atomic.StoreUint64(&db.operationID, opID)
    }
}

func (db *DB) openLevelDB(dir string) error {
    indexPath := filepath.Join(dir, "index")
    ldbOpts := &opt.Options{
        BlockCacheCapacity: 64 * 1024 * 1024,
        Compression:        opt.SnappyCompression,
    }

    var err error
    db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)

    needsRebuild := false
    if err != nil {
        fmt.Printf("LevelDB open failed (%v), rebuilding...\n", err)
        needsRebuild = true
        os.RemoveAll(indexPath)
        db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)
        if err != nil {
            return fmt.Errorf("open fresh leveldb: %w", err)
        }
    } else if !db.isIndexConsistent() {
        fmt.Println("Index is stale, rebuilding...")
        needsRebuild = true
    }

    if needsRebuild {
        if err := db.RebuildIndexFromVLog(); err != nil {
            return fmt.Errorf("rebuild index: %w", err)
        }
    }
    return nil
}

func (db *DB) startBackgroundTasks() {
    db.lastCkptOpID = db.operationID
    waitCount := 3
    if db.checksumInterval > 0 {
        waitCount++
    }
    if db.maxDiskUsagePercent > 0 {
        waitCount++
    }
    db.wg.Add(waitCount)

    go db.runAutoCheckpoint()
    go db.runAutoCompaction()
    go db.runGroupCommits()

    if db.checksumInterval > 0 {
        go db.runBackgroundChecksum()
    }
    if db.maxDiskUsagePercent > 0 {
        go db.runDiskMonitor()
    }
}

func (db *DB) KeyCount() (int64, error) {
    return atomic.LoadInt64(&db.keyCount), nil
}

func (db *DB) scanKeyCount() (int64, error) {
    count := int64(0)
    if db.ldb == nil {
        return 0, nil
    }
    iter := db.ldb.NewIterator(nil, nil)
    defer iter.Release()

    var lastKey []byte

    for iter.Next() {
        if bytes.HasPrefix(iter.Key(), []byte("!sys!")) {
            continue
        }
        key, _, err := decodeIndexKey(iter.Key())
        if err != nil {
            continue
        }
        if !bytes.Equal(key, lastKey) {
            meta, err := decodeEntryMeta(iter.Value())
            if err == nil && !meta.IsTombstone {
                count++
            }
            lastKey = append([]byte(nil), key...)
        }
    }
    return count, nil
}

func (db *DB) StorageStats() (walCount int, walSize int64, vlogCount int, vlogSize int64) {
    walDir := filepath.Join(db.dir, "wal")
    if walEntries, err := os.ReadDir(walDir); err == nil {
        for _, e := range walEntries {
            if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
                walCount++
                if info, err := e.Info(); err == nil {
                    walSize += info.Size()
                }
            }
        }
    }
    vlogDir := filepath.Join(db.dir, "vlog")
    if vlogEntries, err := os.ReadDir(vlogDir); err == nil {
        for _, e := range vlogEntries {
            if !e.IsDir() && strings.HasSuffix(e.Name(), ".vlog") {
                vlogCount++
                if info, err := e.Info(); err == nil {
                    vlogSize += info.Size()
                }
            }
        }
    }
    return
}

func (db *DB) LastOpID() uint64 {
    return atomic.LoadUint64(&db.operationID)
}

func (db *DB) GetLastCheckpointOpID() uint64 {
    return atomic.LoadUint64(&db.lastCkptOpID)
}

func (db *DB) GetConflicts() uint64 {
    return atomic.LoadUint64(&db.metricsConflicts)
}

func (db *DB) ActiveTransactionCount() int {
    db.activeTxnsMu.Lock()
    defer db.activeTxnsMu.Unlock()
    return len(db.activeTxns)
}

func (db *DB) runGroupCommits() {
    defer db.wg.Done()
    var batch []commitRequest

    for {
        select {
        case <-db.closeCh:
            return
        case req := <-db.commitCh:
            batch = append(batch, req)
        }

    Loop:
        for len(batch) < 128 {
            select {
            case req := <-db.commitCh:
                batch = append(batch, req)
            default:
                break Loop
            }
        }
        db.processCommitBatch(batch)
        batch = batch[:0]
    }
}

func (db *DB) SetCompactionMinGarbage(minGarbage int64) {
    db.mu.Lock()
    defer db.mu.Unlock()
    db.minGarbageThreshold = minGarbage
}

func (db *DB) SetWALRetentionTime(d time.Duration) {
    db.mu.Lock()
    defer db.mu.Unlock()
    db.walRetentionTime = d
}

func (db *DB) runAutoCheckpoint() {
    defer db.wg.Done()
    ticker := time.NewTicker(db.autoCheckpointInterval)
    defer ticker.Stop()

    for {
        select {
        case <-db.closeCh:
            return
        case <-ticker.C:
            currentOp := atomic.LoadUint64(&db.operationID)
            lastOp := atomic.LoadUint64(&db.lastCkptOpID)

            if currentOp > lastOp {
                if err := db.Checkpoint(); err != nil {
                    if !strings.Contains(err.Error(), "closed") {
                        fmt.Printf("Auto-checkpoint failed: %v\n", err)
                    }
                } else {
                    checkpointID := atomic.LoadUint64(&db.lastCkptOpID)
                    db.mu.RLock()
                    retention := db.walRetentionTime
                    db.mu.RUnlock()
                    if retention > 0 {
                        if err := db.writeAheadLog.PurgeExpired(retention, checkpointID); err != nil {
                            fmt.Printf("WAL purge failed: %v\n", err)
                        }
                    }
                }
            }
        }
    }
}

func (db *DB) runAutoCompaction() {
    defer db.wg.Done()
    ticker := time.NewTicker(db.compactionInterval)
    defer ticker.Stop()

    for {
        select {
        case <-db.closeCh:
            return
        case <-ticker.C:
            for i := 0; i < 10; i++ {
                didWork, err := db.RunCompaction()
                if err != nil {
                    if !strings.Contains(err.Error(), "closed") {
                        fmt.Printf("Auto-compaction failed: %v\n", err)
                    }
                    break
                }
                if !didWork {
                    break
                }
            }
        }
    }
}

func (db *DB) runBackgroundChecksum() {
    defer db.wg.Done()
    if db.checksumInterval <= 0 {
        return
    }
    ticker := time.NewTicker(db.checksumInterval)
    defer ticker.Stop()

    for {
        select {
        case <-db.closeCh:
            return
        case <-ticker.C:
            if err := db.VerifyChecksums(); err != nil {
                if !strings.Contains(err.Error(), "closed") {
                    fmt.Printf("Checksum verification failed: %v\n", err)
                }
            }
        }
    }
}

func (db *DB) VerifyChecksums() error {
    fids, err := db.valueLog.GetImmutableFileIDs()
    if err != nil {
        return err
    }
    for _, fid := range fids {
        if isClosed(db.closeCh) {
            return nil
        }
        err := db.valueLog.IterateFile(fid, func(_ ValueLogEntry, _ EntryMeta) error {
            if isClosed(db.closeCh) {
                return errors.New("closed")
            }
            return nil
        })
        if err != nil && !strings.Contains(err.Error(), "closed") {
            fmt.Printf("Corrupt VLog %d: %v\n", fid, err)
        }
    }
    return nil
}

func isClosed(ch <-chan struct{}) bool {
    select {
    case <-ch:
        return true
    default:
        return false
    }
}

func (db *DB) NewTransaction(update bool) *Transaction {
    readTxID := atomic.LoadUint64(&db.transactionID)
    tx := &Transaction{
        db:         db,
        pendingOps: make(map[string]*PendingOp),
        readSet:    make(map[string]struct{}),
        readTxID:   readTxID,
        update:     update,
    }
    db.activeTxnsMu.Lock()
    db.activeTxns[tx] = readTxID
    db.activeTxnsMu.Unlock()
    return tx
}

func (db *DB) ScanWAL(startOpID uint64, fn func([]ValueLogEntry) error) error {
    loc, found, err := db.locateWALStart(startOpID)
    if err != nil {
        return err
    }
    if !found {
        return ErrLogUnavailable
    }
    return db.writeAheadLog.Scan(loc, func(entries []ValueLogEntry) error {
        var filtered []ValueLogEntry
        for _, e := range entries {
            if e.OperationID >= startOpID {
                filtered = append(filtered, e)
            }
        }
        if len(filtered) > 0 {
            return fn(filtered)
        }
        return nil
    })
}

func (db *DB) PurgeWAL(minOpID uint64) error {
    return db.writeAheadLog.PurgeOlderThan(minOpID)
}

func (db *DB) ApplyBatch(entries []ValueLogEntry) error {
    // For backward compatibility or single batch application.
    // Delegates to ApplyBatches for consistency.
    return db.ApplyBatches([][]ValueLogEntry{entries})
}

// ApplyBatches applies multiple transactions (batches of entries) atomically to disk.
// This enables Group Commit for replication streams.
func (db *DB) ApplyBatches(batches [][]ValueLogEntry) error {
    if len(batches) == 0 {
        return nil
    }

    db.commitMu.Lock()
    defer db.commitMu.Unlock()

    var walPayloads [][]byte
    var combinedVLog []ValueLogEntry
    var maxTxID uint64
    var maxOpID uint64

    // 1. Preparation & Serialization
    for _, entries := range batches {
        if len(entries) == 0 {
            continue
        }

        first := entries[0]
        txID := first.TransactionID
        startOpID := first.OperationID

        var walBuf bytes.Buffer
        binary.Write(&walBuf, binary.BigEndian, txID)
        binary.Write(&walBuf, binary.BigEndian, startOpID)
        binary.Write(&walBuf, binary.BigEndian, uint32(len(entries)))

        for _, e := range entries {
            binary.Write(&walBuf, binary.BigEndian, uint32(len(e.Key)))
            walBuf.Write(e.Key)
            binary.Write(&walBuf, binary.BigEndian, uint32(len(e.Value)))
            walBuf.Write(e.Value)
            val := byte(0)
            if e.IsDelete {
                val = 1
            }
            walBuf.WriteByte(val)

            if e.TransactionID > maxTxID {
                maxTxID = e.TransactionID
            }
            if e.OperationID > maxOpID {
                maxOpID = e.OperationID
            }
        }

        walPayloads = append(walPayloads, walBuf.Bytes())
        combinedVLog = append(combinedVLog, entries...)
    }

    if len(walPayloads) == 0 {
        return nil
    }

    // 2. Write WAL (One Fsync for all batches)
    if err := db.writeAheadLog.AppendBatches(walPayloads); err != nil {
        return fmt.Errorf("wal append batches: %w", err)
    }

    // 3. Write VLog (One Append for all entries)
    fileID, baseOffset, err := db.valueLog.AppendEntries(combinedVLog)
    if err != nil {
        // Note: Ideally we should rollback WAL here if possible, but this is a critical IO failure.
        return fmt.Errorf("vlog append batches: %w", err)
    }

    // 4. Update Index (Consolidated update)
    staleBytes, keyDelta := db.calculateBatchImpact(combinedVLog)

    if err := db.UpdateIndexForEntries(combinedVLog, fileID, baseOffset, staleBytes); err != nil {
        return fmt.Errorf("index update: %w", err)
    }

    // 5. Update Clocks
    atomic.AddInt64(&db.keyCount, keyDelta)

    currentTx := atomic.LoadUint64(&db.transactionID)
    if maxTxID > currentTx {
        atomic.StoreUint64(&db.transactionID, maxTxID)
    }
    currentOp := atomic.LoadUint64(&db.operationID)
    if maxOpID > currentOp {
        atomic.StoreUint64(&db.operationID, maxOpID)
    }

    return nil
}

func (db *DB) locateWALStart(targetOpID uint64) (WALLocation, bool, error) {
    if loc, ok := db.writeAheadLog.FindInMemory(targetOpID); ok {
        return loc, true, nil
    }
    if db.ldb == nil {
        return WALLocation{}, false, nil
    }
    iter := db.ldb.NewIterator(util.BytesPrefix(sysWALIndexPrefix), nil)
    defer iter.Release()
    seekKey := encodeWALIndexKey(targetOpID)
    if iter.Seek(seekKey) {
        key := iter.Key()
        if decodeWALIndexKey(key) == targetOpID {
            var loc WALLocation
            err := json.Unmarshal(iter.Value(), &loc)
            return loc, true, err
        }
        if iter.Prev() && bytes.HasPrefix(iter.Key(), sysWALIndexPrefix) {
            var loc WALLocation
            err := json.Unmarshal(iter.Value(), &loc)
            return loc, true, err
        }
    } else if iter.Last() && bytes.HasPrefix(iter.Key(), sysWALIndexPrefix) {
        var loc WALLocation
        err := json.Unmarshal(iter.Value(), &loc)
        return loc, true, err
    }
    return WALLocation{}, false, nil
}

func (db *DB) Close() error {
    if !atomic.CompareAndSwapInt32(&db.closed, 0, 1) {
        return nil
    }
    close(db.closeCh)
    db.wg.Wait()

    db.commitMu.Lock()
    defer db.commitMu.Unlock()

    if err := db.Checkpoint(); err != nil {
        if !strings.Contains(err.Error(), "closed") {
            fmt.Printf("Error checkpointing on close: %v\n", err)
        }
    }
    db.persistSequences()

    var errs []string
    if db.writeAheadLog != nil {
        if err := db.writeAheadLog.Close(); err != nil {
            errs = append(errs, err.Error())
        }
    }
    if db.valueLog != nil {
        if err := db.valueLog.Close(); err != nil {
            errs = append(errs, err.Error())
        }
    }
    if db.ldb != nil {
        if err := db.ldb.Close(); err != nil {
            errs = append(errs, err.Error())
        }
        db.ldb = nil
    }
    if len(errs) > 0 {
        return fmt.Errorf("close errors: %s", strings.Join(errs, "; "))
    }
    return nil
}

func (db *DB) Checkpoint() error {
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.ldb == nil {
        return nil
    }
    
    // FIX: Clean up ALL existing garbage stats first to prevent resurrection of deleted files
    // This ensures we only persist what is currently in memory
    iter := db.ldb.NewIterator(util.BytesPrefix(sysStaleBytesPrefix), nil)
    cleanupBatch := new(leveldb.Batch)
    for iter.Next() {
        cleanupBatch.Delete(iter.Key())
    }
    iter.Release()
    if cleanupBatch.Len() > 0 {
        if err := db.ldb.Write(cleanupBatch, nil); err != nil {
            return err
        }
    }

    batch := new(leveldb.Batch)
    for fileID, size := range db.deletedBytesByFile {
        k := make([]byte, len(sysStaleBytesPrefix)+4)
        copy(k, sysStaleBytesPrefix)
        binary.BigEndian.PutUint32(k[len(sysStaleBytesPrefix):], fileID)
        v := make([]byte, 8)
        binary.BigEndian.PutUint64(v, uint64(size))
        batch.Put(k, v)
    }
    if err := db.valueLog.Rotate(); err != nil {
        return err
    }
    atomic.StoreUint64(&db.lastCkptOpID, atomic.LoadUint64(&db.operationID))
    // Always sync sequences during checkpoint to be safe
    db.persistSequences()
    
    if batch.Len() == 0 {
        return nil
    }
    return db.ldb.Write(batch, &opt.WriteOptions{Sync: true})
}

func (db *DB) UpdateIndexForEntries(entries []ValueLogEntry, fileID uint32, baseOffset uint32, staleBytes map[uint32]int64) error {
    batch := new(leveldb.Batch)
    currentOffset := baseOffset
    for _, e := range entries {
        recSize := ValueLogHeaderSize + len(e.Key) + len(e.Value)
        meta := EntryMeta{
            FileID:        fileID,
            ValueOffset:   currentOffset,
            ValueLen:      uint32(len(e.Value)),
            TransactionID: e.TransactionID,
            OperationID:   e.OperationID,
            IsTombstone:   e.IsDelete,
        }
        batch.Put(encodeIndexKey(e.Key, e.TransactionID), meta.Encode())
        currentOffset += uint32(recSize)
    }
    if err := db.ldb.Write(batch, &opt.WriteOptions{Sync: false}); err != nil {
        return err
    }
    if len(staleBytes) > 0 {
        db.mu.Lock()
        for fid, delta := range staleBytes {
            db.deletedBytesByFile[fid] += delta
        }
        db.mu.Unlock()
    }
    return nil
}

func (db *DB) onWALRotate(index map[uint64]WALLocation) error {
    if len(index) == 0 || db.ldb == nil {
        return nil
    }
    batch := new(leveldb.Batch)
    var keys []uint64
    for k := range index {
        keys = append(keys, k)
    }
    sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

    for _, opID := range keys {
        locBytes, _ := json.Marshal(index[opID])
        batch.Put(encodeWALIndexKey(opID), locBytes)
    }
    return db.ldb.Write(batch, nil)
}

func (db *DB) calculateBatchImpact(entries []ValueLogEntry) (map[uint32]int64, int64) {
    staleBytes := make(map[uint32]int64)
    keyDelta := int64(0)
    batchState := make(map[string]bool)

    iter := db.ldb.NewIterator(nil, nil)
    defer iter.Release()

    for _, e := range entries {
        key := string(e.Key)
        exists := false

        if state, seen := batchState[key]; seen {
            exists = state
        } else {
            seekKey := encodeIndexKey(e.Key, math.MaxUint64)
            if iter.Seek(seekKey) {
                foundKey := iter.Key()
                uKey, _, err := decodeIndexKey(foundKey)
                if err == nil && bytes.Equal(uKey, e.Key) {
                    meta, err := decodeEntryMeta(iter.Value())
                    if err == nil {
                        size := int64(ValueLogHeaderSize) + int64(len(e.Key)) + int64(meta.ValueLen)
                        staleBytes[meta.FileID] += size
                        exists = !meta.IsTombstone
                    }
                }
            }
        }

        if e.IsDelete {
            if exists {
                keyDelta--
            }
            batchState[key] = false
        } else {
            if !exists {
                keyDelta++
            }
            batchState[key] = true
        }
    }
    return staleBytes, keyDelta
}

func (db *DB) runDiskMonitor() {
    defer db.wg.Done()
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    db.checkDisk()

    for {
        select {
        case <-db.closeCh:
            return
        case <-ticker.C:
            db.checkDisk()
        }
    }
}

func (db *DB) checkDisk() {
    usage, err := getDiskUsage(db.dir)
    if err != nil {
        fmt.Printf("Error checking disk usage: %v\n", err)
        return
    }

    if int(usage) > db.maxDiskUsagePercent {
        if atomic.CompareAndSwapInt32(&db.isDiskFull, 0, 1) {
            fmt.Printf("WARNING: Disk usage %.2f%% exceeds limit %d%%. Stopping writes.\n", usage, db.maxDiskUsagePercent)
        }
    } else {
        if atomic.CompareAndSwapInt32(&db.isDiskFull, 1, 0) {
            fmt.Printf("INFO: Disk usage %.2f%% within limit. Resuming writes.\n", usage)
        }
    }
}

func (db *DB) loadKeyCount() error {
    val, err := db.ldb.Get(sysKeyCountKey, nil)
    if err == nil && len(val) == 8 {
        db.keyCount = int64(binary.BigEndian.Uint64(val))
        return nil
    }
    if err == leveldb.ErrNotFound {
        fmt.Println("Key count not found, performing full scan...")
        count, err := db.scanKeyCount()
        if err != nil {
            return err
        }
        db.keyCount = count
        return nil
    }
    return err
}

func (db *DB) loadDeletedBytesStats() error {
    db.mu.Lock()
    defer db.mu.Unlock()

    iter := db.ldb.NewIterator(util.BytesPrefix(sysStaleBytesPrefix), nil)
    defer iter.Release()

    for iter.Next() {
        key := iter.Key()
        if len(key) < len(sysStaleBytesPrefix)+4 {
            continue
        }

        fileID := binary.BigEndian.Uint32(key[len(sysStaleBytesPrefix):])
        val := iter.Value()
        if len(val) != 8 {
            continue
        }
        size := int64(binary.BigEndian.Uint64(val))
        db.deletedBytesByFile[fileID] = size
    }
    return iter.Error()
}

func (db *DB) persistSequences() error {
    if db.ldb == nil {
        return nil
    }

    batch := new(leveldb.Batch)

    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, atomic.LoadUint64(&db.transactionID))
    batch.Put(sysTransactionIDKey, buf)

    buf2 := make([]byte, 8)
    binary.BigEndian.PutUint64(buf2, atomic.LoadUint64(&db.operationID))
    batch.Put(sysOperationIDKey, buf2)

    buf3 := make([]byte, 8)
    binary.BigEndian.PutUint64(buf3, uint64(atomic.LoadInt64(&db.keyCount)))
    batch.Put(sysKeyCountKey, buf3)

    return db.ldb.Write(batch, &opt.WriteOptions{Sync: true})
}


