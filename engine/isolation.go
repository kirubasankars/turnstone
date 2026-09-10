// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

// checkReadSetConflicts validates the read set at COMMIT time.
func (tx *Transaction) checkReadSetConflicts() error {
	if len(tx.readSet) == 0 {
		return nil
	}
	for k := range tx.readSet {
		if tx.db.index.HasNewerCommitted([]byte(k), tx.xid, tx.snapshot, tx.db.clogStatus) {
			tx.db.logger.Debug("Read-set conflict detected", "key", k)
			return ErrWriteConflict
		}
	}
	return nil
}

func (db *DB) latestResolvedMeta(key []byte, excludeXid uint64) (*indexVersion, uint64, bool) {
	return db.index.LatestResolved(key, excludeXid, db.clogStatus)
}
