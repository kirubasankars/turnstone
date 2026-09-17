// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"math"
	"sort"
	"time"

	"turnstone/engine/hashindex"
)

const (
	defaultWalCopyForwardRatio     = 3.0
	defaultCopyForwardDrainTimeout = 50 * time.Millisecond
)

// copyForwardDrainTimeout waits for in-flight write transactions after
// walRewriteMu is held. Tests may shorten it.
var copyForwardDrainTimeout = defaultCopyForwardDrainTimeout

// testingAfterCopyForwardCollect runs after the exclusive rewrite lock is held
// and live offsets have been collected, before frames are copied. Tests only.
var testingAfterCopyForwardCollect func()

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
// Eligibility uses an index-only live-size estimate (no WAL reads). If the log
// looks fragmented, walRewriteMu blocks new write begins and replica apply, then
// in-flight writers are drained up to copyForwardDrainTimeout. A writer that is
// still active after the drain skips this tick. commitMu is held only for append,
// remap, and segment purge so COMMIT is not stalled during planning. Read-only
// snapshots may still pin older frames. Pipeline is append → remap index → purge
// segments. Segment purge respects scan floor for replication; unconstrained DBs
// purge through the pre-copy write head for maximum reclaim.
func (db *DB) MaybeCopyForwardWal(minDeletableLSN int64) (WalRetentionResult, error) {
	if db.log == nil || db.index == nil || db.index.hash == nil {
		return WalRetentionResult{}, nil
	}

	ratio := db.walCopyForwardRatio
	if ratio <= 0 {
		ratio = defaultWalCopyForwardRatio
	}

	if !db.copyForwardFragmented(ratio) {
		return WalRetentionResult{}, nil
	}

	db.walRewriteMu.Lock()
	defer db.walRewriteMu.Unlock()

	if !db.waitForWriteTransactions(copyForwardDrainTimeout) {
		return WalRetentionResult{}, nil
	}

	ctx, oldOffsets := db.copyForwardPlan(ratio)
	if len(oldOffsets) == 0 {
		return WalRetentionResult{}, nil
	}

	if testingAfterCopyForwardCollect != nil {
		testingAfterCopyForwardCollect()
	}

	frames := make([][]byte, len(oldOffsets))
	for i, off := range oldOffsets {
		frame, err := db.log.ReadFrameBytesAt(off)
		if err != nil {
			return WalRetentionResult{}, err
		}
		frames[i] = frame
	}

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	outcome, err := db.log.appendCopyForwardFrames(oldOffsets, frames)
	if err != nil {
		return WalRetentionResult{}, err
	}

	// Copied SET/DEL frames have no COMMIT. Write one per xid so reopen does
	// not treat them as crashed in-progress transactions (BEGIN is no longer
	// written on the live path).
	if err := db.appendCopyForwardCommits(frames); err != nil {
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

func (db *DB) waitForWriteTransactions(timeout time.Duration) bool {
	if db.activeWriteTransactionCount() == 0 {
		return true
	}
	if timeout <= 0 {
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		if db.activeWriteTransactionCount() == 0 {
			return true
		}
	}
	return db.activeWriteTransactionCount() == 0
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

func (db *DB) appendCopyForwardCommits(frames [][]byte) error {
	seen := make(map[uint64]struct{})
	var builders []func() []byte
	for _, frame := range frames {
		if len(frame) < LogFrameHeaderSize+LogRecordHeaderSize {
			continue
		}
		rec, err := decodeRecord(frame[LogFrameHeaderSize:])
		if err != nil {
			return err
		}
		if rec.Type != RecordSet && rec.Type != RecordDelete {
			continue
		}
		if _, ok := seen[rec.XID]; ok {
			continue
		}
		seen[rec.XID] = struct{}{}
		xid := rec.XID
		builders = append(builders, func() []byte {
			return encodeRecord(Record{Type: RecordCommit, XID: xid})
		})
	}
	if len(builders) == 0 {
		return nil
	}
	_, err := db.log.AppendRecords(builders, !db.unsafeDisableFsync)
	return err
}

// copyForwardFragmented reports whether logical WAL bytes exceed estimated
// MVCC-live frame bytes by ratio. It walks the index only; it does not read WAL.
func (db *DB) copyForwardFragmented(ratio float64) bool {
	ctx := db.BuildIndexGCContext()
	return db.walExceedsLiveRatio(ratio, db.estimateLiveWalBytes(ctx))
}

func (db *DB) walExceedsLiveRatio(ratio float64, liveBytes int64) bool {
	if liveBytes <= 0 {
		return false
	}
	allocated := db.log.logicalUsedBytes()
	if allocated == 0 {
		allocated = db.log.AllocatedBytesOnDisk()
	}
	return float64(allocated) > float64(liveBytes)*ratio
}

func (db *DB) estimateLiveWalBytes(ctx IndexGCContext) int64 {
	var live int64
	db.forEachLiveWalVersion(ctx, func(key []byte, v indexVersion) {
		live += estimatedWalFrameSize(key, v.tombstone, v.valueLen)
	})
	return live
}

// copyForwardPlan collects live frame offsets when allocated WAL exceeds live
// bytes by ratio. Empty offsets means skip (nothing live or not fragmented).
func (db *DB) copyForwardPlan(ratio float64) (IndexGCContext, []int64) {
	ctx := db.BuildIndexGCContext()
	oldOffsets, liveBytes := db.collectLiveFrameOffsets(ctx)
	if len(oldOffsets) == 0 {
		return ctx, nil
	}
	if !db.walExceedsLiveRatio(ratio, liveBytes) {
		return ctx, nil
	}
	return ctx, oldOffsets
}

// collectLiveFrameOffsets gathers deduplicated frame offsets for copy-forward.
// Uses FilterVersionsForWalRetain (not FilterVersions) so snapshot-pinned frames
// below scan floor are still copied. Live bytes are estimated from index metadata
// so planning does not read WAL frames.
func (db *DB) collectLiveFrameOffsets(ctx IndexGCContext) ([]int64, int64) {
	var offsets []int64
	var liveBytes int64
	db.forEachLiveWalVersion(ctx, func(key []byte, v indexVersion) {
		offsets = append(offsets, v.offset)
		liveBytes += estimatedWalFrameSize(key, v.tombstone, v.valueLen)
	})
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets, liveBytes
}

func (db *DB) forEachLiveWalVersion(ctx IndexGCContext, fn func(key []byte, v indexVersion)) {
	seen := make(map[int64]struct{})
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		kept := ctx.FilterVersionsForWalRetain(key, chain)
		for _, v := range kept {
			if _, ok := seen[v.offset]; ok {
				continue
			}
			seen[v.offset] = struct{}{}
			fn(key, v)
		}
	})
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
	if err == nil {
		if db.valueCache != nil {
			db.valueCache.clear()
		}
		if db.log != nil {
			db.log.buffers.clear()
		}
	}
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
