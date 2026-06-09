// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"turnstone/stonedb/segindex"
)

const indexVersionSize = 29 // offset(8)+valueLen(4)+xmin(8)+opID(8)+tombstone(1)

// Index is an MVCC index backed by a segmented mmap hash table.
// Index files are ephemeral: wiped on open, dropped on close (no munmap/writeback), rebuilt from data.log replay.
type Index struct {
	dir string
	seg *segindex.SegmentedIndex
}

// OpenIndex opens (or recreates) the index under dbDir/index.
// Any existing index files are removed; log replay on DB open rebuilds contents from scratch.
func OpenIndex(dbDir string) (*Index, error) {
	indexDir := filepath.Join(dbDir, "index")
	if err := os.RemoveAll(indexDir); err != nil {
		return nil, err
	}
	seg, err := segindex.Open(indexDir)
	if err != nil {
		return nil, err
	}
	return &Index{dir: indexDir, seg: seg}, nil
}

// Close drops the in-memory index without unmapping segments. MAP_SHARED munmap would
// write dirty pages back to seg-*.bin; that work is wasted because OpenIndex wipes
// index/ on the next open. The kernel reclaims mappings at process exit.
func (idx *Index) Close() error {
	idx.seg = nil
	return nil
}

func (idx *Index) Put(key []byte, v indexVersion) {
	idx.seg.Put(key, toSegVersion(v))
}

func (idx *Index) DropXid(xid uint64) {
	idx.seg.DropXid(xid)
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
	idx.seg.ForEachKey(func(key []byte, chain []segindex.Version) {
		out := make([]indexVersion, len(chain))
		for i, v := range chain {
			out[i] = fromSegVersion(v)
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

func (idx *Index) collectDeadVersions(horizon uint64, clog func(uint64) TxStatus) []struct {
	key string
	ver indexVersion
} {
	var dead []struct {
		key string
		ver indexVersion
	}

	idx.ForEachKey(func(uk []byte, chain []indexVersion) {
		var newestCommitted *indexVersion
		for i := range chain {
			v := chain[i]
			if clog(v.xmin) == TxCommitted {
				cp := v
				newestCommitted = &cp
				break
			}
		}

		k := string(uk)
		for _, v := range chain {
			if clog(v.xmin) == TxInProgress {
				continue
			}
			if clog(v.xmin) == TxAborted {
				dead = append(dead, struct {
					key string
					ver indexVersion
				}{k, v})
				continue
			}
			isNewest := newestCommitted != nil && v.offset == newestCommitted.offset
			if isNewest {
				continue
			}
			if v.xmin < horizon {
				dead = append(dead, struct {
					key string
					ver indexVersion
				}{k, v})
			}
		}
	})
	return dead
}

func (idx *Index) RemoveDead(dead []struct {
	key string
	ver indexVersion
}) {
	for _, d := range dead {
		idx.seg.RemoveVersion([]byte(d.key), d.ver.xmin)
	}
}

func (idx *Index) HasLiveRefAtOffset(offset int64) bool {
	return idx.seg.HasLiveRefAtOffset(offset)
}

func (idx *Index) walkKeyVersions(key []byte, fn func(indexVersion) bool) {
	idx.seg.WalkVersions(key, func(v segindex.Version) bool {
		return fn(fromSegVersion(v))
	})
}

func toSegVersion(v indexVersion) segindex.Version {
	return segindex.Version{
		Offset:    v.offset,
		ValueLen:  v.valueLen,
		Xmin:      v.xmin,
		OpID:      v.opID,
		Tombstone: v.tombstone,
	}
}

func fromSegVersion(v segindex.Version) indexVersion {
	return indexVersion{
		offset:    v.Offset,
		valueLen:  v.ValueLen,
		xmin:      v.Xmin,
		opID:      v.OpID,
		tombstone: v.Tombstone,
	}
}

func encodeIndexVersion(v indexVersion) []byte {
	buf := make([]byte, indexVersionSize)
	binary.BigEndian.PutUint64(buf[0:], uint64(v.offset))
	binary.BigEndian.PutUint32(buf[8:], v.valueLen)
	binary.BigEndian.PutUint64(buf[12:], v.xmin)
	binary.BigEndian.PutUint64(buf[20:], v.opID)
	if v.tombstone {
		buf[28] = 1
	}
	return buf
}

func decodeIndexVersion(data []byte) (indexVersion, error) {
	if len(data) < indexVersionSize {
		return indexVersion{}, errors.New("invalid index version length")
	}
	return indexVersion{
		offset:    int64(binary.BigEndian.Uint64(data[0:])),
		valueLen:  binary.BigEndian.Uint32(data[8:]),
		xmin:      binary.BigEndian.Uint64(data[12:]),
		opID:      binary.BigEndian.Uint64(data[20:]),
		tombstone: data[28] == 1,
	}, nil
}

func alignRange(start, end, blockSize int64) (int64, int64) {
	if blockSize <= 0 {
		blockSize = 4096
	}
	alignedStart := (start + blockSize - 1) / blockSize * blockSize
	alignedEnd := end / blockSize * blockSize
	if alignedEnd <= alignedStart {
		return 0, 0
	}
	return alignedStart, alignedEnd
}

func recordSpanSize(keyLen, valLen int, recType WALRecordType) int64 {
	var body int
	switch recType {
	case WALRecordSet:
		body = 4 + keyLen + 4 + valLen
	case WALRecordDelete:
		body = 4 + keyLen
	}
	return frameSize(LogRecordHeaderSize + body)
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
