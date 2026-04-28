package stonedb

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
		_, xmin, found := tx.db.latestResolvedMeta(iter, key, tx.xid)
		if found && (xmin >= tx.snapshot.Xmax || tx.snapshot.contains(xmin)) {
			tx.db.logger.Debug("Read-set conflict detected", "key", k, "xmin", xmin, "snapshot_xmax", tx.snapshot.Xmax)
			return ErrWriteConflict
		}
	}
	return nil
}
