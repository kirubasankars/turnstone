# `engine/hashindex/` — Sharded in-memory hash index

The `hashindex` package implements a **256-shard open-addressing hash table** with per-shard mutexes, storing **MVCC version chains** in a custom arena allocator. It is used exclusively through `engine.Index` — callers outside `engine` should not import it directly.

## Design goals

| Goal | Mechanism |
| --- | --- |
| Concurrent writes | 256 shards, hash key → shard by low 8 bits |
| MVCC | Linked list of `Version` nodes per key |
| Memory efficiency | Bump-pointer arena per shard; compaction reclaims fragmentation |
| Crash safety | None required — rebuilt from WAL on open |

## Core types

```go
type Version struct {
    Offset    int64   // global WAL byte offset of value
    ValueLen  uint32
    Xmin      uint64  // creating transaction id
    Tombstone bool
}

type Index struct {
    shards [256]*shard
}
```

Each `shard` owns:

- Slot table (open addressing, load-factor growth)
- Arena backing version nodes and key bytes
- `RWMutex` for readers / writers

## Operations

| Method | Behavior |
| --- | --- |
| `Put(key, ver)` | Prepend new version to key's chain |
| `WalkVersions(key, fn)` | Traverse chain newest-first |
| `DropXid(xid)` | Remove in-progress/aborted versions for xid |
| `ForEachKey(fn)` | Full scan — used by compaction / GC |
| `Compact(ctx)` | MVCC-aware prune + arena reclaim (`compact.go`) |

## Compaction (`compact.go`)

Triggered from `engine.index_gc` when estimated fragmentation exceeds ratio thresholds:

1. Walk all keys; drop versions invisible to current GC context.
2. If arena live bytes ≪ allocated, copy live nodes to fresh arena.
3. Preserve offsets referenced by active snapshots / replication floor.

Regression tests: `compact_regression_test.go`, `compact_test.go`.

## On-disk format note

The arena header includes magic `TGHSH` and `formatVersion` for debugging — this is **not** a durable database file, only an in-memory layout marker.

## Educational focus

### Why 256 shards?

Reduces lock contention vs single global map. Shard count is fixed (`numShards = 256`); key hash uses `hashKey(key) & 255`.

### Version chains vs overwrite

`SET` does not delete prior versions immediately — old versions remain until GC proves they are invisible. Deletes insert tombstone versions.

### Separation from `engine.Index`

`engine/index.go` adds:

- Visibility using `clog` and `Snapshot`
- Integration with transaction `xid`
- Offset remapping after WAL copy-forward

When debugging index bugs, determine whether the fault is in **chain structure** (`hashindex`) or **visibility** (`engine`).

## Review checklist

- [ ] `Put` / `WalkVersions` agree on chain order (newest at head).
- [ ] `DropXid` safe during concurrent reads (shard lock held).
- [ ] Compaction does not drop offsets still referenced by MVCC or replication.
- [ ] Load factor growth copies slots correctly (no lost keys).
- [ ] `Close` releases arena memory (tests for leak detection).

## Suggestions

- SIMD or xxhash for `hashKey` if profiling shows hotspot.
- Per-shard statistics exported to metrics (chain depth histogram).
- Fuzz `Put`/`WalkVersions`/`Compact` sequences.
