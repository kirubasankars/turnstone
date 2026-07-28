// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"math"
	"sort"

	"turnstone/engine/hashindex"
)

const defaultWalCopyForwardRatio = 3.0

// WalRetentionResult reports segment deletion or copy-forward from a retention pass.
type WalRetentionResult struct {
	SegmentsDeleted int
	BytesReclaimed  int64
	DeleteThrough   int64
	FramesCopied    int
}

// RunWalMaintenance compacts the index (when enabled), copy-forwards live WAL
// frames when fragmented, then deletes sealed segments at or below scan floor.
//
// Order matters: index compact drops stale versions first; copy-forward rewrites
// live frames and remaps offsets before physical segment delete runs.
func (db *DB) RunWalMaintenance() error {
	if db.indexCompactOnRetention {
		if _, err := db.MaybeCompactIndex(); err != nil {
			return err
		}
	}
	if db.walCopyForwardOnRetention {
		if _, err := db.MaybeCopyForwardWal(db.ScanFloor()); err != nil {
			return err
		}
	}
	_, err := db.DeleteWalSegments(db.MinDeletableLSN())
	return err
}

// DeleteWalSegments removes sealed WAL segments whose exclusive end is at or
// below the effective delete floor derived from minDeletableLSN and MVCC refs.
func (db *DB) DeleteWalSegments(minDeletableLSN int64) (WalRetentionResult, error) {
	if db.log == nil {
		return WalRetentionResult{}, nil
	}

	deleteThrough := db.effectiveWalDeleteThrough(minDeletableLSN)
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

// MaybeCopyForwardWal copies MVCC-visible index frames into a fresh segment when
// on-disk WAL bytes exceed live bytes by WalCopyForwardFragmentation (default 3×).
//
// Under commitMu: skips while write transactions are active (read-only snapshots
// may still pin older frames). Pipeline is append → remap index → purge segments.
// Segment purge respects scan floor for replication; unconstrained DBs purge through
// the pre-copy write head for maximum reclaim.
func (db *DB) MaybeCopyForwardWal(minDeletableLSN int64) (WalRetentionResult, error) {
	if db.log == nil || db.index == nil || db.index.hash == nil {
		return WalRetentionResult{}, nil
	}

	ratio := db.walCopyForwardRatio
	if ratio <= 0 {
		ratio = defaultWalCopyForwardRatio
	}

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	if db.activeWriteTransactionCount() > 0 {
		return WalRetentionResult{}, nil
	}

	ctx := db.BuildIndexGCContext()
	oldOffsets, liveBytes, err := db.collectLiveFrameOffsets(ctx)
	if err != nil {
		return WalRetentionResult{}, err
	}
	if len(oldOffsets) == 0 {
		return WalRetentionResult{}, nil
	}

	allocated := db.log.AllocatedBytesOnDisk()
	if float64(allocated) <= float64(liveBytes)*ratio {
		return WalRetentionResult{}, nil
	}

	frames := make([][]byte, len(oldOffsets))
	for i, off := range oldOffsets {
		frame, err := db.log.ReadFrameBytesAt(off)
		if err != nil {
			return WalRetentionResult{}, err
		}
		frames[i] = frame
	}

	outcome, err := db.log.appendCopyForwardFrames(oldOffsets, frames)
	if err != nil {
		return WalRetentionResult{}, err
	}

	if err := db.remapIndexOffsets(ctx, outcome.remap); err != nil {
		return WalRetentionResult{}, err
	}

	deleteThrough := copyForwardSegmentDeleteThrough(minDeletableLSN, outcome.headBefore, db.ScanFloor())
	deleted, _, err := db.log.deleteSegmentsThrough(deleteThrough)
	if err != nil {
		return WalRetentionResult{}, err
	}

	reclaimed := outcome.bytesBefore - db.log.AllocatedBytesOnDisk()
	if reclaimed < 0 {
		reclaimed = 0
	}
	return WalRetentionResult{
		SegmentsDeleted: deleted,
		BytesReclaimed:  reclaimed,
		FramesCopied:    len(oldOffsets),
		DeleteThrough:   deleteThrough,
	}, nil
}

// MinDeletableLSN returns the byte offset below which sealed WAL segments may
// be deleted. It is the tighter of scan floor (replication / retention) and the
// minimum offset still referenced by an MVCC-visible index version.
func (db *DB) MinDeletableLSN() int64 {
	return db.effectiveWalDeleteThrough(db.ScanFloor())
}

// effectiveWalDeleteThrough is the exclusive-end LSN through which sealed segments
// may be deleted: min(scan/replication floor, MVCC-visible min index offset).
// MVCC tightening allows deleting stale chain entries that index compact has not
// yet rewritten while still honoring snapshot and replication byte retain points.
func (db *DB) effectiveWalDeleteThrough(minDeletableLSN int64) int64 {
	scanFloor := db.ScanFloor()
	if minDeletableLSN > 0 {
		scanFloor = minDeletableLSN
	}

	ctx := db.BuildIndexGCContext()
	mvccMin, ok := db.indexMinMVCCReferencedOffset(ctx)
	if !ok {
		return scanFloor
	}
	if scanFloor <= 0 {
		return mvccMin
	}
	if mvccMin < scanFloor {
		return mvccMin
	}
	return scanFloor
}

func (db *DB) indexMinMVCCReferencedOffset(ctx IndexGCContext) (int64, bool) {
	if db.index == nil {
		return 0, false
	}
	minOff := int64(math.MaxInt64)
	found := false
	db.index.ForEachKey(func(_ []byte, chain []indexVersion) {
		kept := ctx.FilterVersionsForWalRetain(nil, chain)
		for _, v := range kept {
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

// collectLiveFrameOffsets gathers deduplicated frame offsets for copy-forward.
// Uses FilterVersionsForWalRetain (not FilterVersions) so snapshot-pinned frames
// below scan floor are still copied.
func (db *DB) collectLiveFrameOffsets(ctx IndexGCContext) ([]int64, int64, error) {
	seen := make(map[int64]struct{})
	var offsets []int64
	var liveBytes int64

	var readErr error
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		if readErr != nil {
			return
		}
		kept := ctx.FilterVersionsForWalRetain(key, chain)
		for _, v := range kept {
			if _, ok := seen[v.offset]; ok {
				continue
			}
			seen[v.offset] = struct{}{}
			frame, err := db.log.ReadFrameBytesAt(v.offset)
			if err != nil {
				readErr = err
				return
			}
			offsets = append(offsets, v.offset)
			liveBytes += int64(len(frame))
		}
	})
	if readErr != nil {
		return nil, 0, readErr
	}

	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets, liveBytes, nil
}

func (db *DB) remapIndexOffsets(ctx IndexGCContext, remap map[int64]int64) error {
	if len(remap) == 0 {
		return nil
	}
	if err := db.validateRemapCoverage(ctx, remap); err != nil {
		return err
	}
	filter := func(key []byte, chain []hashindex.Version) []hashindex.Version {
		in := make([]indexVersion, len(chain))
		for i, v := range chain {
			in[i] = fromHashVersion(v)
		}
		kept := ctx.FilterVersionsForWalRetain(key, in)
		if len(kept) == 0 {
			return nil
		}
		out := make([]hashindex.Version, len(kept))
		for i, v := range kept {
			newOff := remap[v.offset]
			out[i] = hashindex.Version{
				Offset:    newOff,
				ValueLen:  v.valueLen,
				Xmin:      v.xmin,
				Tombstone: v.tombstone,
			}
		}
		return out
	}
	_, err := db.index.hash.CompactAll(filter)
	return err
}

func (db *DB) validateRemapCoverage(ctx IndexGCContext, remap map[int64]int64) error {
	var missing int64
	found := false
	db.index.ForEachKey(func(_ []byte, chain []indexVersion) {
		for _, v := range ctx.FilterVersionsForWalRetain(nil, chain) {
			found = true
			if _, ok := remap[v.offset]; !ok && missing == 0 {
				missing = v.offset
			}
		}
	})
	if !found {
		return nil
	}
	if missing != 0 {
		return fmt.Errorf("wal copy-forward: missing remap for offset %d", missing)
	}
	return nil
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

func walCopyForwardDisabled() *bool {
	v := false
	return &v
}
