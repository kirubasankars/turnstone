// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"sync/atomic"
)

func (db *DB) replayLog(truncateCorrupt bool) error {
	inProgress := make(map[uint64]struct{})

	err := db.log.Replay(truncateCorrupt, db.timelineMeta.History, func(rec WALRecord, span recordSpan) {
		if rec.XID > atomic.LoadUint64(&db.transactionID) {
			atomic.StoreUint64(&db.transactionID, rec.XID)
		}
		if rec.OpID > atomic.LoadUint64(&db.operationID) {
			atomic.StoreUint64(&db.operationID, rec.OpID)
		}

		switch rec.Type {
		case WALRecordBegin:
			inProgress[rec.XID] = struct{}{}
			db.clog[rec.XID] = TxInProgress
		case WALRecordSet:
			db.index.Put(rec.Key, indexVersion{
				offset: span.offset, valueLen: uint32(len(rec.Value)),
				xmin: rec.XID, opID: rec.OpID, tombstone: false,
			})
		case WALRecordDelete:
			db.index.Put(rec.Key, indexVersion{
				offset: span.offset, valueLen: 0,
				xmin: rec.XID, opID: rec.OpID, tombstone: true,
			})
		case WALRecordCommit:
			db.clog[rec.XID] = TxCommitted
			delete(inProgress, rec.XID)
		case WALRecordAbort:
			db.clog[rec.XID] = TxAborted
			delete(inProgress, rec.XID)
			db.index.DropXid(rec.XID)
		}
	})
	if err != nil && err != ErrTruncated {
		return err
	}

	for xid := range inProgress {
		db.clog[xid] = TxAborted
		db.index.DropXid(xid)
	}

	atomic.StoreInt64(&db.keyCount, db.index.LiveKeyCount(db.clogStatus))
	db.logger.Debug("Log replay complete", "tx_id", atomic.LoadUint64(&db.transactionID), "op_id", atomic.LoadUint64(&db.operationID))
	return nil
}
