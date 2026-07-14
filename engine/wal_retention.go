// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

// WalRetentionResult reports segment deletion or copy-forward from a retention pass.
type WalRetentionResult struct {
	SegmentsDeleted int
	BytesReclaimed  int64
	LiveBytesBefore int64
	LiveBytesAfter  int64
}

// DeleteWalSegments removes sealed WAL segments at or below minDeletableLSN.
// Phase 2: not yet implemented.
func (db *DB) DeleteWalSegments(minDeletableLSN int64) (WalRetentionResult, error) {
	_ = minDeletableLSN
	return WalRetentionResult{}, nil
}

// MaybeCopyForwardWal rewrites live index-referenced frames into a fresh segment.
// Phase 3: not yet implemented.
func (db *DB) MaybeCopyForwardWal(minDeletableLSN int64) (WalRetentionResult, error) {
	_ = minDeletableLSN
	return WalRetentionResult{}, nil
}

// MinDeletableLSN returns the byte offset below which WAL bytes must be retained.
// Phase 4 will union replication safe points with MVCC snapshot requirements.
func (db *DB) MinDeletableLSN() int64 {
	return db.ScanFloor()
}
