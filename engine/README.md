# `engine/` — Storage engine core

The `engine` package is TurnstoneDB's **durability and concurrency heart**: segmented write-ahead log (WAL), MVCC index, commit log (clog), snapshot-isolated transactions, group commit, and WAL maintenance (retention, copy-forward, index GC).

Everything on disk that matters lives here; the in-memory index is rebuilt from WAL replay on every open.

## Architecture

```
                    ┌─────────────┐
  SET/DEL/BEGIN ──► │ Transaction │
                    └──────┬──────┘
                           │ eager append
                           ▼
                    ┌─────────────┐     ┌──────────────┐
                    │   DataLog   │────►│ wal/seg-*.wal│
                    │  (WAL I/O)  │     └──────────────┘
                    └──────┬──────┘
                           │ frame offset
                           ▼
                    ┌─────────────┐
                    │    Index    │◄── hashindex (256 shards)
                    │  (MVCC)     │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    clog     │  xid → committed | aborted
                    └─────────────┘

  COMMIT ──► committer goroutine ──► group fsync ──► setClog(committed)
```

## File map

| File(s) | Topic |
| --- | --- |
| `db.go` | `DB` lifecycle, open/close, public API surface |
| `transaction.go` | `BEGIN` / `SET` / `GET` / `COMMIT` / `ABORT` |
| `types.go` | Record types, options, errors, snapshot struct |
| `encode.go` | Log record encoding/decoding |
| `wal_log.go`, `wal_manifest.go` | Segmented WAL I/O, rotation, manifest |
| `logrange.go` | `ReadLogRange`, `AppendRawFrames` for replication |
| `index.go` | MVCC visibility, version chains on top of `hashindex` |
| `clog.go` | Commit status, snapshot construction |
| `committer.go` | Async group commit with configurable delay/siblings |
| `isolation.go` | Read-set validation at commit (write skew detection) |
| `recovery.go` | WAL replay on open |
| `wal_retention.go`, `wal_copyforward.go` | Maintenance pipeline |
| `index_gc.go` | Index compaction driver |
| `disk_unix.go` | Disk usage monitoring |

Tests (`*_test.go`, `correctness_test.go`, `benchmark_test.go`) are extensive — treat them as executable specification.

## WAL format (summary)

- Segments: `wal/seg-NNNNNN.wal`, rotated at ~64 MiB (see `normalizeWalSegmentSize`).
- **Global byte LSN** spans segments; index `Version.Offset` uses this address space.
- Each frame: `Length(4) + CRC32(4) + payload`.
- Log record header: `Type(1) + XID(8)` + key/value bodies for SET/DEL.

`manifest.json` tracks active segment and sealed segment list.

## Transaction semantics

| Property | Implementation |
| --- | --- |
| Isolation | Snapshot isolation |
| Writes | Eager log at `SET`/`DEL` time |
| Durability | Group `fsync` on `COMMIT` |
| Conflicts | First-writer-wins per key (`NOWAIT` lock) |
| Read own writes | `xmin == myXid` visible before commit |
| Stale reads | `checkReadSetConflicts` at commit |
| Timeout | Reaper aborts txs older than `MaxTxDuration` |

## MVCC index

The `Index` type in `index.go` wraps [`hashindex`](hashindex/) with visibility rules (`getVisible`, `isVisible`). Version chains hang off each key; tombstones represent deletes.

Only **committed** versions are visible to other transactions; aborted versions are pruned via `DropXid`.

## Maintenance pipeline (`RunWalMaintenance`)

Executed when retention policy allows (from `database.EnforceRetentionPolicy`):

1. **Index compaction** — prune stale chains; reclaim arena when fragmentation > ~3× live bytes.
2. **WAL copy-forward** — copy MVCC-visible frames to new segment when allocated WAL > ~3× live; remap index offsets.
3. **Segment delete** — remove sealed segments ≤ `min(scanFloor, mvccFloor)`.

Skips copy-forward while write transactions are active; read-only snapshots pin older frames.

## `Options` (engine open)

Notable tunables in `types.go` `Options`:

- `TruncateCorruptTail` — recovery behavior on partial last frame
- `CommitDelay` / `CommitSiblings` — group commit batching
- `UnsafeDisableFsync` — tests only
- `IndexCompactOnRetention`, `WalCopyForwardOnRetention` — maintenance toggles
- `MaxDiskUsagePercent` — reject writes when disk full

## Educational focus

### Ephemeral index

The hash index is **not** durable. Crash recovery = replay WAL. This simplifies consistency: WAL is the source of truth.

### Global LSN vs xid

- **xid**: monotonic transaction identifier in log records.
- **LSN / offset**: byte position in combined WAL — used for replication and retention.

Mixing these when reading code is a common source of confusion.

### Group commit

`committer.go` batches concurrent `COMMIT` requests to amortize `fsync` cost — study `committer_test.go` for latency/throughput tradeoffs.

## Review checklist

- [ ] New record types: recovery path in `recovery.go` + `ApplyRecord`.
- [ ] Index and WAL offsets stay consistent after copy-forward (remap tests).
- [ ] MVCC visibility rules covered by table-driven tests.
- [ ] Lock ordering documented when touching `txMu`, `commitMu`, shard locks.
- [ ] Background goroutines exit on `Close` (`closeCh`, `wg`).

## Suggested reading order

1. `types.go` — vocabulary
2. `transaction.go` + `clog.go` — user-visible tx path
3. `wal_log.go` — durability
4. `index.go` + [`hashindex/README.md`](hashindex/README.md) — reads
5. `wal_retention.go` — space reclamation

## Suggestions

- Formal on-disk format document generated from `encode.go`.
- Optional `EXPLAIN`-style debug API for visibility decisions per key.
- Pluggable `fsync` policy for NVMe vs remote disk.
