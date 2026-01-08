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
	minGarbageThreshold int64
	checksumInterval    time.Duration
	walRetentionTime    time.Duration
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
	// Default WAL Retention to 2 hours if not set
	if opts.WALRetentionTime == 0 {
		opts.WALRetentionTime = 2 * time.Hour
	}

	// 1. Storage Components
	wal, err := OpenWriteAheadLog(filepath.Join(dir, "wal"), opts.MaxWALSize)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	vl, err := OpenValueLog(filepath.Join(dir, "vlog"))
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("open vlog: %w", err)
	}

	db := &DB{
		dir:                 dir,
		writeAheadLog:       wal,
		valueLog:            vl,
		deletedBytesByFile:  make(map[uint32]int64),
		activeTxns:          make(map[*Transaction]uint64),
		closeCh:             make(chan struct{}),
		commitCh:            make(chan commitRequest, 500),
		minGarbageThreshold: opts.CompactionMinGarbage,
		checksumInterval:    opts.ChecksumInterval,
		walRetentionTime:    opts.WALRetentionTime,
	}

	// 2. Recovery (VLog -> WAL -> Index)
	if err := db.recoverValueLog(); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover vlog: %w", err)
	}

	if err := db.syncWALToValueLog(opts.TruncateCorruptWAL); err != nil {
		db.Close()
		return nil, fmt.Errorf("sync wal: %w", err)
	}

	// 3. Index Init
	if err := db.openLevelDB(dir); err != nil {
		db.Close()
		return nil, err
	}

	// 4. Hook WAL Rotation
	db.writeAheadLog.SetOnRotate(db.onWALRotate)

	// 5. Load Metadata
	if err := db.loadDeletedBytesStats(); err != nil {
		fmt.Printf("Warning: failed to load garbage stats: %v\n", err)
	}

	// 6. Start Background Routines
	db.startBackgroundTasks()

	return db, nil
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
	db.wg.Add(waitCount)

	go db.runAutoCheckpoint()
	go db.runAutoCompaction()
	go db.runGroupCommits()

	if db.checksumInterval > 0 {
		go db.runBackgroundChecksum()
	}
}

// KeyCount returns approximate key count.
func (db *DB) KeyCount() (int64, error) {
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

		// Decode index key to get the logical key
		key, _, err := decodeIndexKey(iter.Key())
		if err != nil {
			continue
		}

		// Check if we have seen this key before.
		// Since LevelDB is sorted (Key ASC, Version DESC), the first time we see a key
		// it is the latest version.
		if !bytes.Equal(key, lastKey) {
			// It's a new key (or the first one). Check if it's alive.
			meta, err := decodeEntryMeta(iter.Value())
			if err == nil && !meta.IsTombstone {
				count++
			}

			// Update lastKey. Must copy because iter buffer is reused.
			lastKey = append([]byte(nil), key...)
		}
	}
	return count, nil
}

// StorageStats returns the number and size of WAL and VLog files.
func (db *DB) StorageStats() (walCount int, walSize int64, vlogCount int, vlogSize int64) {
	// Scan WAL dir
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

	// Scan VLog dir
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

// Metric Getters
func (db *DB) GetConflicts() uint64 {
	return atomic.LoadUint64(&db.metricsConflicts)
}

// ActiveTransactionCount returns number of active transactions.
func (db *DB) ActiveTransactionCount() int {
	db.activeTxnsMu.Lock()
	defer db.activeTxnsMu.Unlock()
	return len(db.activeTxns)
}

// runGroupCommits consumes the commit channel and processes batches.
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

		// Drain loop
	Loop:
		for len(batch) < 128 {
			select {
			case req := <-db.commitCh:
				batch = append(batch, req)
			default:
				break Loop
			}
		}

		// Process via the refactored committer pipeline
		db.processCommitBatch(batch)
		batch = batch[:0]
	}
}

// SetCompactionMinGarbage updates threshold.
func (db *DB) SetCompactionMinGarbage(minGarbage int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.minGarbageThreshold = minGarbage
}

// SetWALRetentionTime updates the WAL retention duration dynamically.
func (db *DB) SetWALRetentionTime(d time.Duration) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.walRetentionTime = d
}

// Background Task: Auto Checkpoint
func (db *DB) runAutoCheckpoint() {
	defer db.wg.Done()
	ticker := time.NewTicker(60 * time.Second)
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
					// Ignore expected errors during shutdown
					if !strings.Contains(err.Error(), "closed") {
						fmt.Printf("Auto-checkpoint failed: %v\n", err)
					}
				} else {
					// WAL Management:
					// Purge logs older than retention time AND fully checkpointed.
					// We get the current safe checkpoint ID which is lastCkptOpID
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

// Background Task: Auto Compaction
func (db *DB) runAutoCompaction() {
	defer db.wg.Done()
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			if err := db.RunCompaction(); err != nil {
				if !strings.Contains(err.Error(), "closed") {
					fmt.Printf("Auto-compaction failed: %v\n", err)
				}
			}
		}
	}
}

// Background Task: Checksum
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

// VerifyChecksums checks data integrity.
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

// Transaction & Operation Access
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

// ApplyBatch applies entries directly (for replication).
func (db *DB) ApplyBatch(entries []ValueLogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	// Direct write pipeline (simplified version of processCommitBatch for single batch)
	first := entries[0]
	txID := first.TransactionID
	startOpID := first.OperationID

	// 1. Serialize WAL
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
	}

	// 2. Persist
	if err := db.writeAheadLog.AppendBatch(walBuf.Bytes()); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	fileID, baseOffset, err := db.valueLog.AppendEntries(entries)
	if err != nil {
		return fmt.Errorf("vlog append: %w", err)
	}

	// 3. Update Index
	staleBytes := make(map[uint32]int64)
	db.calculateStaleBytesSimple(entries, staleBytes)

	if err := db.UpdateIndexForEntries(entries, fileID, baseOffset, staleBytes); err != nil {
		return fmt.Errorf("index update: %w", err)
	}

	// 4. Update Clocks
	maxTxID := uint64(0)
	maxOpID := uint64(0)
	for _, e := range entries {
		if e.TransactionID > maxTxID {
			maxTxID = e.TransactionID
		}
		if e.OperationID > maxOpID {
			maxOpID = e.OperationID
		}
	}

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

// locateWALStart finds WAL location.
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
	// Sort for deterministic write
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

// calculateStaleBytesSimple is a helper for ApplyBatch (direct batch).
func (db *DB) calculateStaleBytesSimple(entries []ValueLogEntry, staleBytes map[uint32]int64) {
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()
	for _, entry := range entries {
		seekKey := encodeIndexKey(entry.Key, math.MaxUint64)
		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, entry.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					size := int64(ValueLogHeaderSize) + int64(len(entry.Key)) + int64(meta.ValueLen)
					staleBytes[meta.FileID] += size
				}
			}
		}
	}
}
