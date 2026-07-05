// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"math"
	"sync/atomic"

	"turnstone/engine/hashindex"
)

const defaultIndexFragmentationRatio = 3.0

// IndexGCReader captures one active transaction snapshot for index pruning.
type IndexGCReader struct {
	Snapshot Snapshot
	MyXid    uint64
	Update   bool
}

// IndexGCContext is a point-in-time view of which index versions must be kept.
type IndexGCContext struct {
	Readers    []IndexGCReader
	ActiveXids map[uint64]struct{}
	LogFloor   int64
	Clog       func(uint64) TxStatus
	Visible    func(uint64, Snapshot) bool
}

// BuildIndexGCContext snapshots active transactions and the log scan floor.
func (db *DB) BuildIndexGCContext() IndexGCContext {
	db.activeTxnsMu.Lock()
	txs := make([]*Transaction, 0, len(db.activeTxns))
	for tx := range db.activeTxns {
		txs = append(txs, tx)
	}
	db.activeTxnsMu.Unlock()

	db.txMu.Lock()
	activeXids := make(map[uint64]struct{}, len(db.activeXids))
	for xid := range db.activeXids {
		activeXids[xid] = struct{}{}
	}
	db.txMu.Unlock()

	readers := make([]IndexGCReader, 0, len(txs))
	for _, tx := range txs {
		readers = append(readers, IndexGCReader{
			Snapshot: tx.snapshot,
			MyXid:    tx.xid,
			Update:   tx.update,
		})
	}

	return IndexGCContext{
		Readers:    readers,
		ActiveXids: activeXids,
		LogFloor:   atomic.LoadInt64(&db.scanFloor),
		Clog:       db.clogStatus,
		Visible:    db.isVisible,
	}
}

// FilterVersions returns the version chain to retain for key (newest first).
func (ctx IndexGCContext) FilterVersions(_ []byte, chain []indexVersion) []indexVersion {
	if len(chain) == 0 {
		return nil
	}
	mask := ctx.keepMask(chain)
	out := make([]indexVersion, 0, len(chain))
	for i, v := range chain {
		if mask[i] {
			out = append(out, v)
		}
	}
	return out
}

func (ctx IndexGCContext) keepMask(chain []indexVersion) []bool {
	n := len(chain)
	keep := make([]bool, n)

	if len(ctx.Readers) == 0 {
		for i, v := range chain {
			keep[i] = true
			if ctx.Clog(v.xmin) == TxCommitted {
				break
			}
		}
	} else {
		for _, r := range ctx.Readers {
			for i, v := range chain {
				keep[i] = true
				if r.Update && v.xmin == r.MyXid {
					break
				}
				if ctx.Visible(v.xmin, r.Snapshot) {
					break
				}
			}
		}
	}

	for i, v := range chain {
		if _, ok := ctx.ActiveXids[v.xmin]; ok {
			keep[i] = true
		}
		if ctx.LogFloor > 0 && v.offset < ctx.LogFloor {
			keep[i] = false
		}
	}
	return keep
}

// IndexCompactResult reports arena shrink from a compact pass.
type IndexCompactResult struct {
	ShardsCompacted int
	ArenaBefore     uint64
	ArenaAfter      uint64
}

// CompactIndex rewrites every shard, dropping unneeded versions and reclaiming arena space.
func (db *DB) CompactIndex(ctx IndexGCContext) (IndexCompactResult, error) {
	if db.index == nil || db.index.hash == nil {
		return IndexCompactResult{}, nil
	}

	statsBefore := db.index.hash.Stats()
	var before uint64
	for i := range statsBefore.Shards {
		before += statsBefore.Shards[i].ArenaUsed
	}

	statsAfter, err := db.index.hash.CompactAll(db.indexVersionFilter(ctx))
	if err != nil {
		return IndexCompactResult{}, err
	}

	var after uint64
	for i := range statsAfter.Shards {
		after += statsAfter.Shards[i].ArenaUsed
	}

	return IndexCompactResult{
		ShardsCompacted: numHashShards(statsBefore),
		ArenaBefore:     before,
		ArenaAfter:      after,
	}, nil
}

func numHashShards(stats hashindex.IndexStats) int {
	n := 0
	for i := range stats.Shards {
		if stats.Shards[i].KeyCount > 0 {
			n++
		}
	}
	return n
}

// MaybeCompactIndex compacts shards whose arena is substantially larger than live data.
func (db *DB) MaybeCompactIndex() (IndexCompactResult, error) {
	if db.index == nil || db.index.hash == nil {
		return IndexCompactResult{}, nil
	}

	ratio := db.indexFragmentationRatio
	if ratio <= 0 {
		ratio = defaultIndexFragmentationRatio
	}

	statsBefore := db.index.hash.Stats()
	ctx := db.BuildIndexGCContext()
	filter := db.indexVersionFilter(ctx)

	var before, after uint64
	compacted := 0
	for i := range statsBefore.Shards {
		st := statsBefore.Shards[i]
		before += st.ArenaUsed
		if st.KeyCount == 0 || st.LiveBytes == 0 {
			after += st.ArenaUsed
			continue
		}
		if float64(st.ArenaUsed) <= float64(st.LiveBytes)*ratio {
			after += st.ArenaUsed
			continue
		}
		afterStats, err := db.index.hash.CompactShard(i, filter)
		if err != nil {
			return IndexCompactResult{}, err
		}
		compacted++
		after += afterStats.ArenaUsed
	}

	if compacted == 0 {
		return IndexCompactResult{}, nil
	}
	return IndexCompactResult{
		ShardsCompacted: compacted,
		ArenaBefore:     before,
		ArenaAfter:      after,
	}, nil
}

func (db *DB) indexVersionFilter(ctx IndexGCContext) hashindex.VersionFilter {
	return func(key []byte, chain []hashindex.Version) []hashindex.Version {
		in := make([]indexVersion, len(chain))
		for i, v := range chain {
			in[i] = fromHashVersion(v)
		}
		out := ctx.FilterVersions(key, in)
		if len(out) == 0 {
			return nil
		}
		versions := make([]hashindex.Version, len(out))
		for i, v := range out {
			versions[i] = toHashVersion(v)
		}
		return versions
	}
}

// IndexArenaStats returns total arena bytes and estimated live bytes.
func (db *DB) IndexArenaStats() (arenaUsed, liveBytes uint64) {
	if db.index == nil || db.index.hash == nil {
		return 0, 0
	}
	stats := db.index.hash.Stats()
	for i := range stats.Shards {
		arenaUsed += stats.Shards[i].ArenaUsed
		liveBytes += stats.Shards[i].LiveBytes
	}
	return arenaUsed, liveBytes
}

// MinActiveSnapshotXmax returns the oldest active snapshot upper bound, or MaxUint64 if none.
func (ctx IndexGCContext) MinActiveSnapshotXmax() uint64 {
	minXmax := uint64(math.MaxUint64)
	for _, r := range ctx.Readers {
		if r.Snapshot.Xmax < minXmax {
			minXmax = r.Snapshot.Xmax
		}
	}
	return minXmax
}
