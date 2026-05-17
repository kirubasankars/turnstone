package stonedb

import (
	"bytes"
	"math"

	"turnstone/stonedb/index"
)

// checkReadSetConflicts implements the read half of Snapshot Isolation
// validation at COMMIT time: if any key we read has since gained a newer
// committed version that our snapshot could not see, we must abort
// (prevents stale reads / write-skew). The write-set / blind-write half of
// the old OCC design is now handled eagerly at Put/Delete time via
// first-writer-wins key locks: by the time we reach COMMIT, nobody else
// could have committed against a key we already hold the lock on, so there
// is nothing left to validate for writes here.
//
// Must be called from inside processCommitBatch's commitMu-guarded critical
// section so the check is linearizable with concurrent COMMIT decisions.
func (tx *Transaction) checkReadSetConflicts() error {
	if len(tx.readSet) == 0 {
		return nil
	}

	iter := tx.db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	for k := range tx.readSet {
		key := []byte(k)
		if tx.db.hasNewerCommittedVersion(iter, key, tx.xid, tx.snapshot) {
			tx.db.logger.Debug("Read-set conflict detected", "key", k, "snapshot_xmax", tx.snapshot.Xmax)
			return ErrWriteConflict
		}
	}
	return nil
}

// hasNewerCommittedVersion reports whether a *durably committed* version of
// key exists that snap could not see (xmin >= snap.Xmax, or xmin was still
// in-progress as of snap). Unlike latestResolvedMeta (used by the write
// path, where lock exclusivity guarantees any other version found has
// already fully resolved), this must not treat a still-in-progress writer
// as a definitive conflict: that writer might never commit, and punishing a
// committing reader for a write that never lands would be a real deviation
// from Snapshot Isolation (only a *committed* write can violate it). We skip
// past in-progress/aborted versions and keep looking at older ones instead.
func (db *DB) hasNewerCommittedVersion(iter index.Iterator, key []byte, excludeXid uint64, snap Snapshot) bool {
	seekKey := encodeIndexKey(key, math.MaxUint64)
	if !iter.Seek(seekKey) {
		return false
	}
	for iter.Valid() {
		foundKey := iter.Key()
		uKey, xmin, err := decodeIndexKey(foundKey)
		if err != nil || !bytes.Equal(uKey, key) {
			return false
		}
		if xmin == excludeXid {
			iter.Next()
			continue
		}
		if db.clogStatusUnlocked(xmin) != TxCommitted {
			// Aborted, or still undecided: neither can be a genuine
			// snapshot-isolation conflict. Keep walking toward older
			// versions -- if the writer eventually commits, it will be
			// caught by this same check.
			iter.Next()
			continue
		}
		return xmin >= snap.Xmax || snap.contains(xmin)
	}
	return false
}
