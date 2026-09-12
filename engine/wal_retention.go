// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import "math"

// WalRetentionResult reports segment deletion or copy-forward from a retention pass.
type WalRetentionResult struct {
	SegmentsDeleted int
	BytesReclaimed  int64
	DeleteThrough   int64
}

// RunWalMaintenance compacts the index (when enabled) and deletes sealed WAL
// segments at or below the current scan floor. Call after SetScanFloor moves the
// retention barrier.
func (db *DB) RunWalMaintenance() error {
	if db.indexCompactOnRetention {
		if _, err := db.MaybeCompactIndex(); err != nil {
			return err
		}
	}
	_, err := db.DeleteWalSegments(db.MinDeletableLSN())
	return err
}

// DeleteWalSegments removes sealed WAL segments whose exclusive end is at or
// below the effective delete floor derived from minDeletableLSN and index refs.
func (db *DB) DeleteWalSegments(minDeletableLSN int64) (WalRetentionResult, error) {
	if db.log == nil || minDeletableLSN <= 0 {
		return WalRetentionResult{}, nil
	}

	deleteThrough := minDeletableLSN
	if minOff, ok := db.indexMinReferencedOffset(); ok && minOff < deleteThrough {
		deleteThrough = minOff
	}
	if deleteThrough <= 0 {
		return WalRetentionResult{}, nil
	}

	deleted, reclaimed, err := db.log.deleteSegmentsThrough(deleteThrough)
	if err != nil {
		return WalRetentionResult{}, err
	}
	return WalRetentionResult{
		SegmentsDeleted: deleted,
		BytesReclaimed:  reclaimed,
		DeleteThrough:   deleteThrough,
	}, nil
}

// MaybeCopyForwardWal rewrites live index-referenced frames into a fresh segment.
// Phase 3: not yet implemented.
func (db *DB) MaybeCopyForwardWal(minDeletableLSN int64) (WalRetentionResult, error) {
	_ = minDeletableLSN
	return WalRetentionResult{}, nil
}

// MinDeletableLSN returns the byte offset below which WAL bytes may be deleted.
// Phase 4 will union replication safe points with MVCC snapshot requirements.
func (db *DB) MinDeletableLSN() int64 {
	return db.ScanFloor()
}

func (db *DB) indexMinReferencedOffset() (int64, bool) {
	if db.index == nil {
		return 0, false
	}
	minOff := int64(math.MaxInt64)
	found := false
	db.index.ForEachKey(func(_ []byte, chain []indexVersion) {
		for _, v := range chain {
			found = true
			if v.offset < minOff {
				minOff = v.offset
			}
		}
	})
	if !found {
		return 0, false
	}
	return minOff, true
}
