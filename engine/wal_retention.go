// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
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
func (db *DB) RunWalMaintenance() error {
	if db.indexCompactOnRetention {
		if _, err := db.MaybeCompactIndex(); err != nil {
			return err
		}
	}
	if db.walCopyForwardOnRetention {
		if _, err := db.MaybeCopyForwardWal(db.MinDeletableLSN()); err != nil {
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

// MaybeCopyForwardWal copies live index-referenced frames into a fresh segment
// and remaps index offsets when on-disk WAL bytes exceed live bytes by ratio.
func (db *DB) MaybeCopyForwardWal(minDeletableLSN int64) (WalRetentionResult, error) {
	if db.log == nil || db.index == nil || db.index.hash == nil {
		return WalRetentionResult{}, nil
	}
	if db.ActiveTransactionCount() > 0 {
		return WalRetentionResult{}, nil
	}

	ratio := db.walCopyForwardRatio
	if ratio <= 0 {
		ratio = defaultWalCopyForwardRatio
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

	deleteThrough := minDeletableLSN
	if deleteThrough <= 0 {
		if floor := db.ScanFloor(); floor > 0 {
			deleteThrough = floor
		} else {
			deleteThrough = math.MaxInt64
		}
	}

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	outcome, err := db.log.copyForwardLiveFrames(oldOffsets, frames, deleteThrough)
	if err != nil {
		return WalRetentionResult{}, err
	}

	if err := db.remapIndexOffsets(ctx, outcome.remap); err != nil {
		return WalRetentionResult{}, err
	}

	reclaimed := outcome.bytesBefore - outcome.bytesAfter
	if reclaimed < 0 {
		reclaimed = 0
	}
	return WalRetentionResult{
		SegmentsDeleted: outcome.segmentsPurged,
		BytesReclaimed:  reclaimed,
		FramesCopied:    len(oldOffsets),
		DeleteThrough:   deleteThrough,
	}, nil
}

// MinDeletableLSN returns the byte offset below which WAL bytes may be deleted.
func (db *DB) MinDeletableLSN() int64 {
	return db.ScanFloor()
}

func (db *DB) collectLiveFrameOffsets(ctx IndexGCContext) ([]int64, int64, error) {
	seen := make(map[int64]struct{})
	var offsets []int64
	var liveBytes int64

	var readErr error
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		if readErr != nil {
			return
		}
		kept := ctx.FilterVersions(key, chain)
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
	filter := func(key []byte, chain []hashindex.Version) []hashindex.Version {
		in := make([]indexVersion, len(chain))
		for i, v := range chain {
			in[i] = fromHashVersion(v)
		}
		kept := ctx.FilterVersions(key, in)
		if len(kept) == 0 {
			return nil
		}
		out := make([]hashindex.Version, len(kept))
		for i, v := range kept {
			newOff, ok := remap[v.offset]
			if !ok {
				return nil
			}
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
