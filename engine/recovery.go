// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"context"
	"sync/atomic"
)

func (db *DB) replayLog(ctx context.Context, truncateCorrupt bool) error {
	inProgress := make(map[uint64]struct{})

	err := db.log.Replay(ctx, truncateCorrupt, func(rec Record, span recordSpan) {
		if rec.XID > atomic.LoadUint64(&db.transactionID) {
			atomic.StoreUint64(&db.transactionID, rec.XID)
		}

		switch rec.Type {
		case RecordBegin:
			inProgress[rec.XID] = struct{}{}
		case RecordSet:
			db.index.Put(rec.Key, indexVersion{
				offset: span.offset, valueLen: uint32(len(rec.Value)),
				xmin: rec.XID, tombstone: false,
			})
		case RecordDelete:
			db.index.Put(rec.Key, indexVersion{
				offset: span.offset, valueLen: 0,
				xmin: rec.XID, tombstone: true,
			})
		case RecordCommit:
			delete(inProgress, rec.XID)
			db.forgetClog(rec.XID)
		case RecordAbort:
			delete(inProgress, rec.XID)
			db.index.DropXid(rec.XID)
			db.forgetClog(rec.XID)
		}
	})
	if err != nil && err != ErrTruncated {
		return err
	}

	for xid := range inProgress {
		db.index.DropXid(xid)
		db.forgetClog(xid)
	}

	atomic.StoreInt64(&db.keyCount, db.index.LiveKeyCount(db.clogStatus))
	db.logger.Debug("Log replay complete",
		"tx_id", atomic.LoadUint64(&db.transactionID),
		"log_offset", db.log.WriteOffset(),
	)
	return nil
}
