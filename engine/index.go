// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"turnstone/engine/hashindex"
)

// Index is an MVCC index backed by a sharded in-memory hash arena.
// The index is ephemeral: rebuilt from data.log replay on open and dropped on close.
type Index struct {
	hash *hashindex.Index
}

// NewIndex creates a fresh in-memory index. Log replay on DB open rebuilds contents.
func NewIndex() *Index {
	return &Index{hash: hashindex.New()}
}

// Close drops the in-memory index and frees shard buffers.
func (idx *Index) Close() error {
	if idx.hash == nil {
		return nil
	}
	err := idx.hash.Close()
	idx.hash = nil
	return err
}

func (idx *Index) Put(key []byte, v indexVersion) {
	idx.hash.Put(key, toHashVersion(v))
}

func (idx *Index) DropXid(xid uint64) {
	idx.hash.DropXid(xid)
}

func (idx *Index) LatestResolved(key []byte, excludeXid uint64, clog func(uint64) TxStatus) (*indexVersion, uint64, bool) {
	var found *indexVersion
	var foundXmin uint64
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		if v.xmin == excludeXid {
			return true
		}
		if clog(v.xmin) == TxAborted {
			return true
		}
		cp := v
		found = &cp
		foundXmin = v.xmin
		return false
	})
	if found == nil {
		return nil, 0, false
	}
	return found, foundXmin, true
}

func (idx *Index) GetVisible(key []byte, snap Snapshot, myXid uint64, update bool, visible func(uint64, Snapshot) bool) (*indexVersion, bool) {
	var found *indexVersion
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		if update && v.xmin == myXid {
			cp := v
			found = &cp
			return false
		}
		if visible(v.xmin, snap) {
			cp := v
			found = &cp
			return false
		}
		return true
	})
	if found == nil {
		return nil, false
	}
	return found, true
}

func (idx *Index) HasNewerCommitted(key []byte, excludeXid uint64, snap Snapshot, clog func(uint64) TxStatus) bool {
	found := false
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		if v.xmin == excludeXid {
			return true
		}
		if clog(v.xmin) != TxCommitted {
			return true
		}
		found = v.xmin >= snap.Xmax || snap.contains(v.xmin)
		return false
	})
	return found
}

func (idx *Index) ForEachKey(fn func(key []byte, chain []indexVersion)) {
	idx.hash.ForEachKey(func(key []byte, chain []hashindex.Version) {
		out := make([]indexVersion, len(chain))
		for i, v := range chain {
			out[i] = fromHashVersion(v)
		}
		fn(key, out)
	})
}

func (idx *Index) LiveKeyCount(clog func(uint64) TxStatus) int64 {
	var count int64
	idx.ForEachKey(func(_ []byte, chain []indexVersion) {
		for _, v := range chain {
			if clog(v.xmin) == TxCommitted && !v.tombstone {
				count++
				break
			}
		}
	})
	return count
}

func (idx *Index) walkKeyVersions(key []byte, fn func(indexVersion) bool) {
	idx.hash.WalkVersions(key, func(v hashindex.Version) bool {
		return fn(fromHashVersion(v))
	})
}

func toHashVersion(v indexVersion) hashindex.Version {
	return hashindex.Version{
		Offset:    v.offset,
		ValueLen:  v.valueLen,
		Xmin:      v.xmin,
		Tombstone: v.tombstone,
	}
}

func fromHashVersion(v hashindex.Version) indexVersion {
	return indexVersion{
		offset:    v.Offset,
		valueLen:  v.ValueLen,
		xmin:      v.Xmin,
		tombstone: v.Tombstone,
	}
}

// visibleVersionForKey returns the first visible version for key in snapshot.
func visibleVersionForKey(chain []indexVersion, snap Snapshot, myXid uint64, update bool, isVisible func(uint64, Snapshot) bool) (*indexVersion, bool) {
	for _, v := range chain {
		if update && v.xmin == myXid {
			cp := v
			return &cp, true
		}
		if isVisible(v.xmin, snap) {
			cp := v
			return &cp, true
		}
	}
	return nil, false
}
