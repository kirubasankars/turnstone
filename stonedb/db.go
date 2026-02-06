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

// DB is the main database struct (formerly Store)
type DB struct {
	dir                string
	ldb                *leveldb.DB
	writeAheadLog      *WriteAheadLog
	valueLog           *ValueLog
	deletedBytesByFile map[uint32]int64

	mu       sync.RWMutex
	commitMu sync.Mutex

	// Two clocks:
	transactionID uint64 // Clock for Transactions (Batch ID)
	operationID   uint64 // Clock for Every Operation (Global Op ID)

	// Transaction Lifecycle & Garbage Collection
	activeTxnsMu   sync.Mutex
	activeTxns     map[*Transaction]uint64 // Map active Tx -> ReadTxID (Restored for O(1) performance)
	pendingDeletes []pendingFile           // Files waiting for active txns to finish

	// Auto-Checkpoint & Background Tasks
	closeCh      chan struct{}
	wg           sync.WaitGroup
	lastCkptOpID uint64
	closed       int32 // Atomic flag to ensure idempotent Close

	// Group Commit
	commitCh chan commitRequest

	// Config
	minGarbageThreshold int64
	checksumInterval    time.Duration
}

// Open initializes the DB
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}

	if opts.MaxWALSize == 0 {
		opts.MaxWALSize = 10 * 1024 * 1024 // 10MB Default
	}

	if opts.CompactionMinGarbage == 0 {
		opts.CompactionMinGarbage = 1024 * 1024 // 1MB Default
	}

	// 1. Open WriteAheadLog (Directory based now)
	wal, err := OpenWriteAheadLog(filepath.Join(dir, "wal"), opts.MaxWALSize)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	// 2. Open ValueLog
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
		commitCh:            make(chan commitRequest, 500), // Buffer for concurrency
		minGarbageThreshold: opts.CompactionMinGarbage,
		checksumInterval:    opts.ChecksumInterval,
	}

	// 3. Recover ValueLog (Source of Truth #1)
	if err := db.recoverValueLog(); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover vlog: %w", err)
	}

	// 4. Sync WAL to ValueLog (Source of Truth #2)
	// We pass the option to allow truncation of corrupt WAL tails
	if err := db.syncWALToValueLog(opts.TruncateCorruptWAL); err != nil {
		db.Close() // Will handle nil ldb gracefully now
		return nil, fmt.Errorf("sync wal: %w", err)
	}

	// 5. Open LevelDB (Index)
	indexPath := filepath.Join(dir, "index")
	ldbOpts := &opt.Options{
		BlockCacheCapacity: 64 * 1024 * 1024,
		Compression:        opt.SnappyCompression,
	}

	db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)

	needsRebuild := false
	if err != nil {
		fmt.Printf("LevelDB open failed (%v), rebuilding...\n", err)
		needsRebuild = true
		os.RemoveAll(indexPath)
		db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("open fresh leveldb: %w", err)
		}
	} else {
		if !db.isIndexConsistent() {
			fmt.Println("Index is stale/inconsistent, rebuilding from ValueLog...")
			needsRebuild = true
		}
	}

	if needsRebuild {
		if err := db.RebuildIndexFromVLog(); err != nil {
			db.Close()
			return nil, fmt.Errorf("rebuild index: %w", err)
		}
	}

	// 6. Connect WAL Rotation to LevelDB
	db.writeAheadLog.SetOnRotate(func(index map[uint64]WALLocation) error {
		if len(index) == 0 {
			return nil
		}
		if db.ldb == nil {
			return nil
		}

		batch := new(leveldb.Batch)
		var keys []uint64
		for k := range index {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		for _, opID := range keys {
			loc := index[opID]
			locBytes, err := json.Marshal(loc)
			if err != nil {
				return err
			}
			batch.Put(encodeWALIndexKey(opID), locBytes)
		}
		return db.ldb.Write(batch, nil)
	})

	// 7. Load persisted deleted bytes stats
	if err := db.loadDeletedBytesStats(); err != nil {
		fmt.Printf("Warning: failed to load garbage stats: %v\n", err)
	}

	// 8. Initialize Auto-Checkpoint, Compaction & Checksumming
	db.lastCkptOpID = db.operationID

	waitCount := 3 // checkpoint + compaction + groupCommit
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

	return db, nil
}

// KeyCount returns the approximate number of keys in the database.
func (db *DB) KeyCount() (int64, error) {
	count := int64(0)
	// Guard against nil LDB if called during partial Open/Close
	if db.ldb == nil {
		return 0, nil
	}
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		// Filter out system keys
		if !bytes.HasPrefix(iter.Key(), []byte("!sys!")) {
			count++
		}
	}
	return count, nil
}

// LastOpID returns the most recent committed Operation ID.
func (db *DB) LastOpID() uint64 {
	return atomic.LoadUint64(&db.operationID)
}

// runGroupCommits aggregates concurrent commits into batches to amortize fsync cost.
func (db *DB) runGroupCommits() {
	defer db.wg.Done()
	var batch []commitRequest

	for {
		// 1. Fetch first item (blocking)
		select {
		case <-db.closeCh:
			return
		case req := <-db.commitCh:
			batch = append(batch, req)
		}

		// 2. Drain pending items (non-blocking) up to max batch size
	Loop:
		for len(batch) < 128 {
			select {
			case req := <-db.commitCh:
				batch = append(batch, req)
			default:
				break Loop
			}
		}

		// 3. Commit the batch
		db.commitGroup(batch)
		batch = batch[:0]
	}
}

// commitGroup processes a batch of transactions.
func (db *DB) commitGroup(requests []commitRequest) {
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	var validRequests []commitRequest
	var walPayloads [][]byte
	var combinedVLog []ValueLogEntry

	// Track stats for index update
	staleBytes := make(map[uint32]int64)

	// Reserve sequences
	currentTxID := atomic.LoadUint64(&db.transactionID)
	currentOpID := atomic.LoadUint64(&db.operationID)

	// Intra-batch Conflict Detection
	// We must track keys written by accepted transactions in this batch
	// to prevent conflicts within the batch itself.
	batchWrites := make(map[string]uint64) // Key -> TxID that wrote it (reserved TxID)

	// Phase 1: Validation & Preparation
	for _, req := range requests {
		tx := req.tx

		// 1. Check against DB history (Standard Conflict Detection)
		if err := tx.checkConflicts(); err != nil {
			req.resp <- err
			continue
		}

		// 2. Check against previous transactions in this batch (Intra-batch Isolation)
		intraBatchConflict := false

		// Check Read Set against Batch Writes
		for k := range tx.readSet {
			if _, exists := batchWrites[k]; exists {
				intraBatchConflict = true
				break
			}
		}

		// Check Write Set against Batch Writes (Blind Write conflict check)
		if !intraBatchConflict {
			for k := range tx.pendingOps {
				if _, exists := batchWrites[k]; exists {
					intraBatchConflict = true
					break
				}
			}
		}

		if intraBatchConflict {
			req.resp <- ErrWriteConflict
			continue
		}

		// Success: Assign IDs
		currentTxID++
		startOpID := currentOpID + 1
		opsCount := uint64(len(tx.pendingOps))
		currentOpID += opsCount

		// Register writes for subsequent intra-batch checks
		for k := range tx.pendingOps {
			batchWrites[k] = currentTxID
		}

		// Serialize to WAL buffer
		var walBuf bytes.Buffer
		binary.Write(&walBuf, binary.BigEndian, currentTxID)
		binary.Write(&walBuf, binary.BigEndian, startOpID)
		binary.Write(&walBuf, binary.BigEndian, uint32(len(tx.pendingOps)))

		opIdx := uint64(0)
		for k, op := range tx.pendingOps {
			key := []byte(k)
			entry := ValueLogEntry{
				Key:           key,
				Value:         op.Value,
				TransactionID: currentTxID,
				OperationID:   startOpID + opIdx,
				IsDelete:      op.IsDelete,
			}
			combinedVLog = append(combinedVLog, entry)
			opIdx++

			binary.Write(&walBuf, binary.BigEndian, uint32(len(key)))
			walBuf.Write(key)
			binary.Write(&walBuf, binary.BigEndian, uint32(len(op.Value)))
			walBuf.Write(op.Value)
			if op.IsDelete {
				walBuf.WriteByte(1)
			} else {
				walBuf.WriteByte(0)
			}
		}

		walPayloads = append(walPayloads, walBuf.Bytes())
		validRequests = append(validRequests, req)
	}

	if len(validRequests) == 0 {
		return
	}

	// Phase 2: IO (WAL & VLog)
	if err := db.writeAheadLog.AppendBatches(walPayloads); err != nil {
		err = fmt.Errorf("wal group error: %w", err)
		for _, req := range validRequests {
			req.resp <- err
		}
		return
	}

	fileID, baseOffset, err := db.valueLog.AppendEntries(combinedVLog)
	if err != nil {
		err = fmt.Errorf("vlog group error: %w", err)
		for _, req := range validRequests {
			req.resp <- err
		}
		return
	}

	// Phase 3: Update Index & Clocks
	// Calculate stale bytes
	iter := db.ldb.NewIterator(nil, nil)
	for _, entry := range combinedVLog {
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
	iter.Release()

	if err := db.UpdateIndexForEntries(combinedVLog, fileID, baseOffset, staleBytes); err != nil {
		err = fmt.Errorf("index group error: %w", err)
		for _, req := range validRequests {
			req.resp <- err
		}
		return
	}

	// Update Global Clocks
	atomic.StoreUint64(&db.transactionID, currentTxID)
	atomic.StoreUint64(&db.operationID, currentOpID)

	// Phase 4: Notify Success
	for _, req := range validRequests {
		req.resp <- nil
	}
}

// SetCompactionMinGarbage updates the minimum garbage threshold required to trigger compaction on a file.
func (db *DB) SetCompactionMinGarbage(minGarbage int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.minGarbageThreshold = minGarbage
}

// runAutoCheckpoint triggers a checkpoint every 60 seconds if new mutations have occurred.
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
					errMsg := err.Error()
					if strings.Contains(errMsg, "file already closed") ||
						strings.Contains(errMsg, "no such file or directory") {
						return
					}
					fmt.Printf("Auto-checkpoint failed: %v\n", err)
				} else {
					safeOpID := atomic.LoadUint64(&db.lastCkptOpID)
					if err := db.PurgeWAL(safeOpID); err != nil {
						if !strings.Contains(err.Error(), "no such file") {
							fmt.Printf("Auto-purge failed: %v\n", err)
						}
					}
				}
			}
		}
	}
}

// runAutoCompaction triggers compaction every 2 minutes.
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
				errMsg := err.Error()
				if strings.Contains(errMsg, "file already closed") ||
					strings.Contains(errMsg, "closed") ||
					strings.Contains(errMsg, "no such file or directory") {
					return
				}
				fmt.Printf("Auto-compaction failed: %v\n", err)
			}
		}
	}
}

// runBackgroundChecksum runs periodic checksum verification on ValueLog files.
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
				errMsg := err.Error()
				if strings.Contains(errMsg, "file already closed") ||
					strings.Contains(errMsg, "no such file") ||
					strings.Contains(errMsg, "closed") {
					return
				}
				fmt.Printf("Background checksum verification failed: %v\n", err)
			}
		}
	}
}

// VerifyChecksums iterates over all immutable VLog files and verifies their CRCs.
func (db *DB) VerifyChecksums() error {
	fids, err := db.valueLog.GetImmutableFileIDs()
	if err != nil {
		return err
	}

	for _, fid := range fids {
		select {
		case <-db.closeCh:
			return nil
		default:
		}

		err := db.valueLog.IterateFile(fid, func(_ ValueLogEntry, _ EntryMeta) error {
			select {
			case <-db.closeCh:
				return errors.New("database closed")
			default:
			}
			return nil
		})
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				return nil
			}
			fmt.Printf("Data Integrity Error: File %04d.vlog corrupt: %v\n", fid, err)
		}
	}
	return nil
}

// NewTransaction creates a new transaction.
func (db *DB) NewTransaction(update bool) *Transaction {
	readTxID := atomic.LoadUint64(&db.transactionID)
	tx := &Transaction{
		db:         db,
		pendingOps: make(map[string]*PendingOp),
		readSet:    make(map[string]struct{}),
		readTxID:   readTxID,
		update:     update,
	}

	// Register in the active transactions map (Fast O(1) insert)
	db.activeTxnsMu.Lock()
	db.activeTxns[tx] = readTxID
	db.activeTxnsMu.Unlock()

	return tx
}

// ScanWAL iterates over the WAL starting from the specified operationId.
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

// PurgeWAL removes WAL files that strictly contain operations older than minOpID.
func (db *DB) PurgeWAL(minOpID uint64) error {
	return db.writeAheadLog.PurgeOlderThan(minOpID)
}

// ApplyBatch writes a batch of external entries (e.g. from replication) to the WAL, VLog, and Index.
func (db *DB) ApplyBatch(entries []ValueLogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

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
		if e.IsDelete {
			walBuf.WriteByte(1)
		} else {
			walBuf.WriteByte(0)
		}
	}

	if err := db.writeAheadLog.AppendBatch(walBuf.Bytes()); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	fileID, baseOffset, err := db.valueLog.AppendEntries(entries)
	if err != nil {
		return fmt.Errorf("vlog append: %w", err)
	}

	staleBytes := make(map[uint32]int64)
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	for _, e := range entries {
		seekKey := encodeIndexKey(e.Key, math.MaxUint64)
		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, e.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					size := int64(ValueLogHeaderSize) + int64(len(e.Key)) + int64(meta.ValueLen)
					staleBytes[meta.FileID] += size
				}
			}
		}
	}

	if err := db.UpdateIndexForEntries(entries, fileID, baseOffset, staleBytes); err != nil {
		return fmt.Errorf("index update: %w", err)
	}

	currentTx := atomic.LoadUint64(&db.transactionID)
	if txID > currentTx {
		atomic.StoreUint64(&db.transactionID, txID)
	}

	lastOp := entries[len(entries)-1].OperationID
	currentOp := atomic.LoadUint64(&db.operationID)
	if lastOp > currentOp {
		atomic.StoreUint64(&db.operationID, lastOp)
	}

	return nil
}

// locateWALStart finds the WALLocation for the batch containing or immediately preceding startOpID
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
		foundOpID := decodeWALIndexKey(key)
		if foundOpID == targetOpID {
			var loc WALLocation
			err := json.Unmarshal(iter.Value(), &loc)
			return loc, true, err
		}
		if iter.Prev() {
			if bytes.HasPrefix(iter.Key(), sysWALIndexPrefix) {
				var loc WALLocation
				err := json.Unmarshal(iter.Value(), &loc)
				return loc, true, err
			}
		}
	} else {
		if iter.Last() && bytes.HasPrefix(iter.Key(), sysWALIndexPrefix) {
			var loc WALLocation
			err := json.Unmarshal(iter.Value(), &loc)
			return loc, true, err
		}
	}
	return WALLocation{}, false, nil
}

// Close closes the database. It is safe to call multiple times.
func (db *DB) Close() error {
	if !atomic.CompareAndSwapInt32(&db.closed, 0, 1) {
		return nil
	}

	close(db.closeCh)
	db.wg.Wait()

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	if err := db.Checkpoint(); err != nil {
		errMsg := err.Error()
		if !strings.Contains(errMsg, "file already closed") && !strings.Contains(errMsg, "no such file or directory") {
			fmt.Printf("Error checkpointing on close: %v\n", err)
		}
	}

	db.persistSequences()

	var err1, err2, err3 error

	if db.writeAheadLog != nil {
		err1 = db.writeAheadLog.Close()
	}
	if db.valueLog != nil {
		err2 = db.valueLog.Close()
	}
	if db.ldb != nil {
		err3 = db.ldb.Close()
		db.ldb = nil
	}

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
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
		return fmt.Errorf("vlog rotate: %w", err)
	}
	currentOp := atomic.LoadUint64(&db.operationID)
	atomic.StoreUint64(&db.lastCkptOpID, currentOp)
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
