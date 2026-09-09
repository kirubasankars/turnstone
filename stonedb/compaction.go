package stonedb

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync/atomic"

	"turnstone/stonedb/index"
)

// relocateEntry pairs a ValueLog entry being compacted forward with its
// original physical offset in the source file, so rewriteBatch's
// re-verification can confirm the index still points at exactly this
// (file, offset) pair rather than merely a matching OperationID (which a
// previous, possibly partial, compaction pass could have already relocated
// elsewhere while leaving the same OperationID behind).
type relocateEntry struct {
	entry      ValueLogEntry
	origOffset int64
}

// RunCompaction picks the file with the most stale data and compacts it.
// Returns true if a file was compacted, false if no candidates were found.
func (db *DB) RunCompaction() (bool, error) {
	// Guard against concurrent compaction passes (the auto-compaction loop
	// and a manually-triggered one) picking the same candidate file: both
	// would relocate the same live entries independently, duplicating them
	// and leaking the untracked copies as garbage forever, and could each
	// try to os.Remove the same now-empty file.
	if !atomic.CompareAndSwapInt32(&db.compacting, 0, 1) {
		return false, nil
	}
	defer atomic.StoreInt32(&db.compacting, 0)

	// 1. Pick Candidate
	db.mu.RLock()
	var bestFid uint32
	var maxGarbage int64
	activeFid := db.valueLog.CurrentFileID()

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
	var validEntries []relocateEntry
	currentBatchSize := 0
	// Limit batch size to ~2MB or 1000 entries to control memory usage during compaction
	const maxBatchBytes = 2 * 1024 * 1024
	const maxBatchCount = 1000

	// Batch for deleting stale index entries
	staleBatch := new(index.Batch)

	// Create an iterator to check validity against the index *before* moving data.
	var iter index.Iterator
	newIter := func() { iter = db.ldb.NewIterator(nil, nil) }
	releaseIter := func() {
		if iter != nil {
			iter.Release()
			iter = nil
		}
	}
	newIter()
	defer releaseIter()

	// Helper to flush stale deletes. The iterator holds a snapshot read lock,
	// so it must be released before any index write to avoid deadlock.
	flushStale := func() error {
		if staleBatch.Len() > 0 {
			releaseIter()
			if err := db.ldb.Write(staleBatch, &index.WriteOptions{Sync: true}); err != nil {
				return err
			}
			staleBatch.Reset()
			newIter()
		}
		return nil
	}

	// Helper to flush valid entries
	flushValid := func() error {
		if len(validEntries) > 0 {
			releaseIter()
			if err := db.rewriteBatch(validEntries, bestFid); err != nil {
				return err
			}
			validEntries = validEntries[:0]
			currentBatchSize = 0
			newIter()
		}
		return nil
	}

	horizon := db.minActiveSnapshotXmax()

	err := db.valueLog.IterateFile(bestFid, func(e ValueLogEntry, m EntryMeta) error {
		if iter == nil {
			newIter()
		}
		isAlive, isCurrentPointer := db.isEntryAlive(iter, e, horizon)

		if isAlive {
			validEntries = append(validEntries, relocateEntry{entry: e, origOffset: m.ValueOffset})
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
		} else if isCurrentPointer {
			// This VLog record is still the index's current pointer for
			// (key, xid), but it has been resolved as garbage (aborted, or a
			// committed version fully superseded below the vacuum horizon).
			// Only remove the index entry in that case -- if it is NOT the
			// current pointer, a later write within the same transaction has
			// already superseded it in the index and that entry must be left
			// untouched.
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

// isEntryAlive decides whether a ValueLog entry encountered during
// compaction must be preserved. It returns:
//   - isAlive: true if the entry must be rewritten forward.
//   - isCurrentPointer: true if the index for (key, xid) still points at
//     exactly this (TransactionID, OperationID) pair (used by the caller to
//     decide whether it is safe to delete the index entry when !isAlive).
//
// Rules: in-progress versions are always kept (their fate isn't known yet).
// Aborted versions are always garbage. A committed version is kept if it is
// the current visible pointer for its key (no newer committed version
// exists, or the newer one is itself still in-progress), or if some active
// snapshot's horizon still needs it (vacuum horizon).
func (db *DB) isEntryAlive(iter index.Iterator, e ValueLogEntry, horizon uint64) (isAlive bool, isCurrentPointer bool) {
	seekKey := encodeIndexKey(e.Key, math.MaxUint64)

	var newerCommittedXmin uint64
	haveNewerCommitted := false
	haveNewerInProgress := false

	for ok := iter.Seek(seekKey); ok && iter.Valid(); ok = iter.Next() {
		foundKey := iter.Key()
		uKey, xmin, err := decodeIndexKey(foundKey)
		if err != nil || !bytes.Equal(uKey, e.Key) {
			return false, false
		}

		if xmin == e.TransactionID {
			meta, err := decodeEntryMeta(iter.Value())
			if err != nil || meta.OperationID != e.OperationID {
				return false, false
			}
			isCurrentPointer = true
			switch db.clogStatusUnlocked(xmin) {
			case TxAborted:
				return false, true
			case TxInProgress:
				return true, true
			default: // TxCommitted
				if haveNewerInProgress {
					// A newer in-progress writer exists; until it resolves,
					// this committed version is still what current readers see.
					return true, true
				}
				if !haveNewerCommitted {
					return true, true // current visible pointer
				}
				// The superseding version already existed before the oldest
				// active snapshot's horizon, so every active snapshot already
				// sees it; this older version is truly dead. Conversely, if
				// the superseding version landed at or after the horizon,
				// the oldest active snapshot predates it and still needs
				// this older version to remain visible.
				return newerCommittedXmin >= horizon, true
			}
		}

		switch db.clogStatusUnlocked(xmin) {
		case TxCommitted:
			if !haveNewerCommitted {
				haveNewerCommitted = true
				newerCommittedXmin = xmin
			}
		case TxInProgress:
			haveNewerInProgress = true
		case TxAborted:
			// ignore aborted newer versions when looking for a superseding one
		}
	}
	return false, false
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
func (db *DB) rewriteBatch(entries []relocateEntry, origFileID uint32) error {
	// 1. Write to VLog (Expensive I/O) - NO LOCK
	plain := make([]ValueLogEntry, len(entries))
	for i, re := range entries {
		plain[i] = re.entry
	}
	fileID, baseOffset, err := db.valueLog.AppendEntries(plain)
	if err != nil {
		return err
	}

	// Fsync the relocated copy before repointing the index at it. Compaction
	// relocations keep each entry's original, already-below-watermark
	// OperationID, so recovery's redo logic (which only trusts WAL records
	// with OpID above the ValueLog's recovered high-water mark) can never
	// redo them if they go missing -- an unsynced write here is this
	// relocated copy's only chance at durability. If we crash before this
	// Sync, the source file (origFileID) is still untouched and still the
	// index's current pointer, so nothing is lost; the risk is only a crash
	// *after* the index update below points at bytes that were never
	// flushed.
	if err := db.valueLog.Sync(); err != nil {
		return fmt.Errorf("vlog sync failed during compaction relocation: %w", err)
	}

	// 2. Update Index (Fast Memory Ops) - LOCK REQUIRED
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	batch := new(index.Batch)

	currentOffset := baseOffset
	var newGarbage int64

	for _, re := range entries {
		e := re.entry
		recSize := ValueLogHeaderSize + len(e.Key) + len(e.Value)
		isLatest := false

		// Re-verify against current index state under lock, by exact
		// (key, xid) lookup AND the exact (FileID, offset) we relocated
		// this entry from -- NOT merely a matching OperationID.
		// Vacuum-horizon compaction can legitimately keep an older
		// committed version alive (still visible to a long-lived snapshot)
		// even after a newer version has since been written for the same
		// key, so a "still the newest" check would be wrong. But a bare
		// OperationID match is not sufficient either: a relocated entry
		// keeps its original OperationID forever, so it cannot distinguish
		// "this is still the original, unmoved entry in origFileID" from
		// "a previous (possibly partial) compaction pass already relocated
		// this same entry elsewhere, and something else now legitimately
		// occupies this OperationID's slot by coincidence of an aborted
		// retry" -- silently leaking the earlier relocated copy as
		// untracked garbage forever. Checking the source (FileID, offset)
		// too makes the check exact.
		alreadyRelocatedElsewhere := false
		if raw, err := db.ldb.Get(encodeIndexKey(e.Key, e.TransactionID), nil); err == nil {
			if meta, err := decodeEntryMeta(raw); err == nil && meta.OperationID == e.OperationID {
				if meta.FileID == origFileID && meta.ValueOffset == re.origOffset {
					isLatest = true
				} else {
					// The index already points somewhere other than
					// origFileID/re.origOffset for this exact (key, xid),
					// despite the same OperationID: a previous (possibly
					// partial/crashed) compaction pass already relocated
					// this entry there. That copy is the valid one; ours is
					// a stale duplicate we must not let overwrite it.
					alreadyRelocatedElsewhere = true
				}
			}
		}

		if isLatest {
			// Update Index to point to NEW VLog location
			meta := EntryMeta{
				FileID:        fileID,
				ValueOffset:   currentOffset, // int64
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

			if !alreadyRelocatedElsewhere {
				// Truly gone (aborted, or garbage-collected already):
				// delete the OLD index entry to keep history clean. A
				// no-op if it's already absent.
				batch.Delete(encodeIndexKey(e.Key, e.TransactionID))
			}
			// If alreadyRelocatedElsewhere, leave the index untouched: it
			// already correctly points at the valid earlier relocation.
		}

		currentOffset += int64(recSize)
	}

	if batch.Len() > 0 {
		// Use Sync: true to ensure index updates are persisted before we consider the old file obsolete.
		// If we crash before this sync, the old file is still valid (not deleted yet).
		// If we crash after, the index points to the new file.
		if err := db.ldb.Write(batch, &index.WriteOptions{Sync: true}); err != nil {
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
