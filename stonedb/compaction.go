package stonedb

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// RunCompaction picks the file with the most stale data and compacts it.
// Returns true if a file was compacted, false if no candidates were found.
func (db *DB) RunCompaction() (bool, error) {
	// 1. Pick Candidate
	db.mu.RLock()
	var bestFid uint32
	var maxGarbage int64
	activeFid := db.valueLog.currentFid

	for fid, garbage := range db.deletedBytesByFile {
		// Don't compact the active file
		if fid == activeFid {
			continue
		}
		// Select greedy max, but respect minimum threshold
		if garbage > maxGarbage && garbage >= db.minGarbageThreshold {
			maxGarbage = garbage
			bestFid = fid
		}
	}
	db.mu.RUnlock()

	if maxGarbage == 0 {
		return false, nil
	}

	db.logger.Info("Compacting file", "file_id", bestFid, "garbage_bytes", maxGarbage)

	// 2. Rewrite valid entries
	var validEntries []ValueLogEntry
	currentBatchSize := 0
	// Limit batch size to ~2MB or 1000 entries to control memory usage during compaction
	const maxBatchBytes = 2 * 1024 * 1024
	const maxBatchCount = 1000

	// Batch for deleting stale index entries
	staleBatch := new(leveldb.Batch)

	// Helper to flush stale deletes
	flushStale := func() error {
		if staleBatch.Len() > 0 {
			if err := db.ldb.Write(staleBatch, &opt.WriteOptions{Sync: false}); err != nil {
				return err
			}
			staleBatch.Reset()
		}
		return nil
	}

	// Helper to flush valid entries
	flushValid := func() error {
		if len(validEntries) > 0 {
			// rewriteBatch will handle re-verification and index updates for these
			if err := db.rewriteBatch(validEntries); err != nil {
				return err
			}
			validEntries = validEntries[:0]
			currentBatchSize = 0
		}
		return nil
	}

	// Create an iterator to check validity against the index *before* moving data.
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	err := db.valueLog.IterateFile(bestFid, func(e ValueLogEntry, _ EntryMeta) error {
		// Pre-filter: Check if the entry is the latest version in the index.
		isAlive := false
		seekKey := encodeIndexKey(e.Key, math.MaxUint64)
		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, e.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					// STRICT CHECK: The index must point to THIS transaction/operation.
					if meta.TransactionID == e.TransactionID && meta.OperationID == e.OperationID {
						isAlive = true
					}
				}
			}
		}

		if isAlive {
			validEntries = append(validEntries, e)
			currentBatchSize += len(e.Key) + len(e.Value) + ValueLogHeaderSize

			if currentBatchSize >= maxBatchBytes || len(validEntries) >= maxBatchCount {
				if err := flushValid(); err != nil {
					return err
				}
				// Also flush stale to keep pending deletes in sync
				if err := flushStale(); err != nil {
					return err
				}
			}
		} else {
			// It's stale. We need to remove this specific version from the index.
			staleBatch.Delete(encodeIndexKey(e.Key, e.TransactionID))

			// Flush if stale batch gets too big
			if staleBatch.Len() >= maxBatchCount {
				if err := flushStale(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		// Safety Net: If the file is missing, remove it from stats to prevent infinite retry loops.
		if os.IsNotExist(err) {
			db.logger.Warn("Compaction candidate missing, removing from stats", "file_id", bestFid)
			db.mu.Lock()
			delete(db.deletedBytesByFile, bestFid)
			db.mu.Unlock()
			return false, nil
		}
		return false, fmt.Errorf("compaction iteration failed for file %d: %w", bestFid, err)
	}

	// Flush remaining
	if err := flushValid(); err != nil {
		return false, fmt.Errorf("compaction flush valid failed: %w", err)
	}
	if err := flushStale(); err != nil {
		return false, fmt.Errorf("compaction flush stale failed: %w", err)
	}

	// 3. Mark for Deletion (Deferred)
	db.activeTxnsMu.Lock()
	obsoleteAt := atomic.LoadUint64(&db.transactionID)
	db.pendingDeletes = append(db.pendingDeletes, pendingFile{
		fileID:     bestFid,
		obsoleteAt: obsoleteAt,
	})
	db.activeTxnsMu.Unlock()

	// 4. Cleanup Stats
	db.mu.Lock()
	delete(db.deletedBytesByFile, bestFid)
	db.mu.Unlock()

	// 5. Try to physically delete files that are safe
	db.deleteObsoleteFiles()

	return true, nil
}

// deleteObsoleteFiles checks if any pending files are safe to delete based on active transactions.
func (db *DB) deleteObsoleteFiles() {
	db.activeTxnsMu.Lock()
	defer db.activeTxnsMu.Unlock()

	if len(db.pendingDeletes) == 0 {
		return
	}

	minActiveID := uint64(math.MaxUint64)
	for _, readID := range db.activeTxns {
		if readID < minActiveID {
			minActiveID = readID
		}
	}

	if len(db.activeTxns) == 0 {
		minActiveID = atomic.LoadUint64(&db.transactionID) + 1
	}

	var remaining []pendingFile
	for _, p := range db.pendingDeletes {
		if p.obsoleteAt < minActiveID {
			db.logger.Info("Deleting obsolete VLog file", "file_id", p.fileID)
			if err := db.valueLog.DeleteFile(p.fileID); err != nil {
				db.logger.Error("Failed to delete VLog file", "file_id", p.fileID, "err", err)
				remaining = append(remaining, p)
			}
		} else {
			remaining = append(remaining, p)
		}
	}
	db.pendingDeletes = remaining
}

// rewriteBatch moves valid entries to the active log and updates the index.
// It includes a critical re-verification step under lock to prevent race conditions.
func (db *DB) rewriteBatch(entries []ValueLogEntry) error {
	// 1. Write to VLog (Expensive I/O) - NO LOCK
	fileID, baseOffset, err := db.valueLog.AppendEntries(entries)
	if err != nil {
		return err
	}

	// 2. Update Index (Fast Memory Ops) - LOCK REQUIRED
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	batch := new(leveldb.Batch)
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	currentOffset := baseOffset
	var newGarbage int64

	for _, e := range entries {
		recSize := ValueLogHeaderSize + len(e.Key) + len(e.Value)
		isLatest := false

		// Re-verify against current index state under lock.
		// A user transaction might have updated the key while we were writing to VLog.
		seekKey := encodeIndexKey(e.Key, math.MaxUint64)
		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, e.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					// It is only safe to update the index if it still points to the exact version we moved.
					if meta.TransactionID == e.TransactionID && meta.OperationID == e.OperationID {
						isLatest = true
					}
				}
			}
		}

		if isLatest {
			// Update Index to point to NEW VLog location
			meta := EntryMeta{
				FileID:        fileID,
				ValueOffset:   currentOffset,
				ValueLen:      uint32(len(e.Value)),
				TransactionID: e.TransactionID,
				OperationID:   e.OperationID,
				IsTombstone:   e.IsDelete,
			}
			batch.Put(encodeIndexKey(e.Key, e.TransactionID), meta.Encode())
		} else {
			// Race Condition: The key was updated concurrently or was already stale.
			// The space we just used in the new VLog file is now garbage.
			newGarbage += int64(recSize)

			// We still delete the OLD index entry to keep history clean
			batch.Delete(encodeIndexKey(e.Key, e.TransactionID))
		}

		currentOffset += uint32(recSize)
	}

	if batch.Len() > 0 {
		if err := db.ldb.Write(batch, &opt.WriteOptions{Sync: false}); err != nil {
			return err
		}
	}

	// If we generated garbage in the NEW file (due to race), record it.
	if newGarbage > 0 {
		db.mu.Lock()
		db.deletedBytesByFile[fileID] += newGarbage
		db.mu.Unlock()
	}

	return nil
}

func (db *DB) computeStaleBytes(ops map[string]*PendingOp) map[uint32]int64 {
	staleBytes := make(map[uint32]int64)
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	for k := range ops {
		keyBytes := []byte(k)
		seekKey := encodeIndexKey(keyBytes, math.MaxUint64)

		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, keyBytes) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					size := int64(ValueLogHeaderSize) + int64(len(keyBytes)) + int64(meta.ValueLen)
					staleBytes[meta.FileID] += size
				}
			}
		}
	}
	return staleBytes
}
