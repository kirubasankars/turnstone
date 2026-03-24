package stonedb

import (
	"bytes"
	"math"
	"sync/atomic"
)

// checkConflicts implements the core Snapshot Isolation validation rules.
// It ensures that no keys read or written by the transaction have been committed
// by another transaction with a Commit Timestamp (TxID) greater than this transaction's
// Read Snapshot (readTxID).
//
// Rules enforced:
//  1. No Stale Reads: If you read Key A at version 10, and it was updated to version 11
//     before you commit, you abort.
//  2. First-Committer-Wins: If you write Key B (blind write), and someone else updated Key B
//     concurrently, you abort.
func (tx *Transaction) checkConflicts() error {
	// Optimization: Read-only transactions or empty writes don't conflict in this phase
	// (Read-only txns are logically consistent by virtue of reading a fixed snapshot).
	if len(tx.readSet) == 0 && len(tx.pendingOps) == 0 {
		return nil
	}

	// Use a single iterator for the entire validation pass to reduce allocation overhead.
	iter := tx.db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	// validateKey checks a single key against the database index.
	validateKey := func(key string) error {
		keyBytes := []byte(key)
		// Construct the seek key to find the latest version (MaxUint64 - Timestamp)
		seekKey := encodeIndexKey(keyBytes, math.MaxUint64)

		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, version, err := decodeIndexKey(foundKey)

			if err == nil && bytes.Equal(uKey, keyBytes) {
				// Conflict Condition:
				// The latest committed version in the DB is NEWER than our transaction's snapshot.
				if version > tx.readTxID {
					// Debug log conflict details
					tx.db.logger.Debug("Conflict detected", "key", key, "read_ts", tx.readTxID, "db_ts", version)
					return ErrWriteConflict
				}
			}
		}
		return nil
	}

	// 1. Validate Read Set (Prevent Stale Reads / Write Skew)
	for k := range tx.readSet {
		if err := validateKey(k); err != nil {
			atomic.AddUint64(&tx.db.metricsConflicts, 1)
			return err
		}
	}

	// 2. Validate Write Set (Prevent Lost Updates / Blind Write Conflicts)
	// We must ensure we don't overwrite a value committed by a concurrent transaction,
	// even if we didn't read it.
	for k := range tx.pendingOps {
		// Optimization: If we already checked this key in the Read Set loop, skip it.
		if _, checked := tx.readSet[k]; checked {
			continue
		}
		if err := validateKey(k); err != nil {
			atomic.AddUint64(&tx.db.metricsConflicts, 1)
			return err
		}
	}

	return nil
}
