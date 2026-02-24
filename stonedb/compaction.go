package stonedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// RunCompaction picks the file with the most stale data and compacts it.
func (db *DB) RunCompaction() error {
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
		return nil
	}

	fmt.Printf("Compacting file %d (garbage: %d bytes)\n", bestFid, maxGarbage)

	// 2. Rewrite valid entries
	var validEntries []ValueLogEntry
	currentBatchSize := 0
	// Limit batch size to ~2MB or 1000 entries to control memory usage during compaction
	const maxBatchBytes = 2 * 1024 * 1024
	const maxBatchCount = 1000

	err := db.valueLog.IterateFile(bestFid, func(e ValueLogEntry, _ EntryMeta) error {
		validEntries = append(validEntries, e)
		currentBatchSize += len(e.Key) + len(e.Value) + ValueLogHeaderSize

		if currentBatchSize >= maxBatchBytes || len(validEntries) >= maxBatchCount {
			if err := db.rewriteBatch(validEntries); err != nil {
				return err
			}
			validEntries = validEntries[:0]
			currentBatchSize = 0
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("compaction iteration failed for file %d: %w", bestFid, err)
	}

	if len(validEntries) > 0 {
		if err := db.rewriteBatch(validEntries); err != nil {
			return fmt.Errorf("compaction flush failed: %w", err)
		}
	}

	// 3. Mark for Deletion (Deferred)
	// We capture the current transaction ID. Any transaction started AFTER this point
	// is guaranteed not to need the old file because we have updated the index.
	// Transactions started BEFORE this point might still have an iterator open pointing to the old file.
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

	return nil
}

// deleteObsoleteFiles checks if any pending files are safe to delete based on active transactions.
func (db *DB) deleteObsoleteFiles() {
	db.activeTxnsMu.Lock()
	defer db.activeTxnsMu.Unlock()

	if len(db.pendingDeletes) == 0 {
		return
	}

	minActiveID := uint64(math.MaxUint64)
	// Iterate map - O(N) where N is active transactions.
	// This is acceptable because N is usually small (<1000) and this runs rarely (every 2m).
	for _, readID := range db.activeTxns {
		if readID < minActiveID {
			minActiveID = readID
		}
	}

	// If no transactions are active, we can delete anything up to the current head.
	if len(db.activeTxns) == 0 {
		minActiveID = atomic.LoadUint64(&db.transactionID) + 1
	}

	var remaining []pendingFile
	for _, p := range db.pendingDeletes {
		// A file is safe to delete if it became obsolete at a TxID strictly less than
		// the oldest active reader's snapshot ID.
		if p.obsoleteAt < minActiveID {
			fmt.Printf("Physically deleting obsolete ValueLog file: %04d.vlog\n", p.fileID)
			if err := db.valueLog.DeleteFile(p.fileID); err != nil {
				fmt.Printf("Failed to delete file %04d.vlog: %v\n", p.fileID, err)
				// Keep it in the list to retry later
				remaining = append(remaining, p)
			}
		} else {
			remaining = append(remaining, p)
		}
	}
	db.pendingDeletes = remaining
}

// rewriteBatch verifies entries are still the latest version and moves them to active log.
// It also cleans up index entries for stale keys that are not moved.
func (db *DB) rewriteBatch(entries []ValueLogEntry) error {
	// OPTIMISTIC WRITE: Write to VLog without global Commit Lock first.
	// We pay the I/O cost upfront. If we lose the race, we just wasted some disk space (garbage),
	// but we maintain consistency.

	// 1. Write to VLog (Expensive I/O) - NO LOCK
	fileID, baseOffset, err := db.valueLog.AppendEntries(entries)
	if err != nil {
		return err
	}

	// 2. Update Index (Fast Memory Ops) - LOCK REQUIRED
	// We must lock to ensure that no other Commit is changing the index while we verify.
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	batch := new(leveldb.Batch)

	// We iterate through the batch we just wrote. For each entry, we check if
	// it is still the *authoritative* version in the index.
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	currentOffset := baseOffset

	for _, e := range entries {
		recSize := ValueLogHeaderSize + len(e.Key) + len(e.Value)
		isLatest := false

		// Verify against current index state
		seekKey := encodeIndexKey(e.Key, math.MaxUint64)
		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, e.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					// CRITICAL RACE CHECK:
					// Does the index still point to the EXACT same TransactionID and OperationID
					// as the entry we are moving?
					// If YES: We are safe to update the pointer to the new location.
					// If NO:  The key was updated by a user transaction (or another process) since we read it.
					//         Our "moved" entry is now garbage. We abandon it.
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
			// Stale: The key was updated concurrently or was already stale.
			// The space we just used in the new VLog file is now garbage.
			// We track this so the new file can eventually be compacted too.
			db.deletedBytesByFile[fileID] += int64(recSize)

			// Clean up the index entry pointing to the OLD VLog file.
			// Since we are compacting the file containing 'e', and 'e' is not latest,
			// this specific version is garbage and the old file will be deleted.
			// We must remove the index record for this specific version to prevent
			// dangling pointers in the version chain.
			batch.Delete(encodeIndexKey(e.Key, e.TransactionID))
		}

		currentOffset += uint32(recSize)
	}

	if batch.Len() > 0 {
		return db.ldb.Write(batch, &opt.WriteOptions{Sync: false})
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
