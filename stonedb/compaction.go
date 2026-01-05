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

	// 2. Rewrite valid entries
	var validEntries []ValueLogEntry

	err := db.valueLog.IterateFile(bestFid, func(e ValueLogEntry, _ EntryMeta) error {
		validEntries = append(validEntries, e)
		if len(validEntries) >= 100 {
			if err := db.rewriteBatch(validEntries); err != nil {
				return err
			}
			validEntries = validEntries[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(validEntries) > 0 {
		if err := db.rewriteBatch(validEntries); err != nil {
			return err
		}
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

	if len(db.activeTxns) == 0 {
		minActiveID = atomic.LoadUint64(&db.transactionID) + 1
	}

	var remaining []pendingFile
	for _, p := range db.pendingDeletes {
		if p.obsoleteAt < minActiveID {
			fmt.Printf("Physically deleting obsolete ValueLog file: %04d.vlog\n", p.fileID)
			if err := db.valueLog.DeleteFile(p.fileID); err != nil {
				fmt.Printf("Failed to delete file %04d.vlog: %v\n", p.fileID, err)
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
	// We only hold lock to Update Index.

	// 1. Write to VLog (Expensive I/O) - NO LOCK
	fileID, baseOffset, err := db.valueLog.AppendEntries(entries)
	if err != nil {
		return err
	}

	// 2. Update Index (Fast Memory Ops) - LOCK
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	batch := new(leveldb.Batch)

	// We need to re-verify against the *latest* index state under lock
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	currentOffset := baseOffset

	for _, e := range entries {
		isLatest := false
		seekKey := encodeIndexKey(e.Key, math.MaxUint64)

		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, e.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					// Check if the Index still points to the OLD location (TxID match)
					if meta.TransactionID == e.TransactionID {
						isLatest = true
					}
				}
			}
		}

		recSize := ValueLogHeaderSize + len(e.Key) + len(e.Value)

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
			// We effectively "wasted" space in the new VLog file.
			// We should count this as garbage immediately so it gets cleaned up later.
			// We only need to delete the specific TxID entry we are moving.
			batch.Delete(encodeIndexKey(e.Key, e.TransactionID))

			// Track garbage created in the NEW file
			db.deletedBytesByFile[fileID] += int64(recSize)
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
