<!--
Copyright (c) 2026 Kiruba Sankar Swaminathan

This source code is licensed under the MIT license found in the
LICENSE file in the root of this source tree.
-->

# How TurnstoneDB can be faster than PostgreSQL

TurnstoneDB will not beat PostgreSQL at SQL, joins, secondary indexes, or
ad-hoc analytics. It can beat PostgreSQL on the workload it is built for:
**durable single-key GET/SET with snapshot isolation**.

PostgreSQL is a general-purpose RDBMS. Every `UPDATE kv SET v=$1 WHERE k=$2`
pays for a SQL parser, planner, btree probe, heap tuple, WAL + clog, and
`shared_buffers`. Turnstone stores opaque bytes under ASCII keys: hash lookup,
append-only WAL, group `fdatasync`, in-memory value cache.

## What must stay true

A fair win is **not** `synchronous_commit=off` vs Turnstone with fsync, and
not Redis-style memory-only vs a durable engine. Compare:

| Knob | Turnstone | PostgreSQL |
| --- | --- | --- |
| Durability | group `fdatasync` on `COMMIT` (default) | `synchronous_commit=on` (default) |
| Isolation | snapshot isolation | `READ COMMITTED` or `REPEATABLE READ` |
| API | `BEGIN` / `SET` / `GET` / `COMMIT` | `INSERT … ON CONFLICT` / `SELECT` |
| Schema | one keyspace of bytes | `CREATE TABLE kv (k TEXT PRIMARY KEY, v BYTEA)` |

Turnstone still loses when you need SQL, multi-column predicates, or
foreign keys. Use PostgreSQL there.

## Engine choices that beat a btree + SQL stack

1. **No SQL.** The hot path is a binary opcode, not parse/plan/execute.
2. **Hash index, not btree.** Point GET/SET is O(1) across 256 shards.
3. **Postgres-style group commit, without a default sleep.** Concurrent
   `COMMIT`s that arrive during an in-flight `fdatasync` share the next
   flush. `CommitDelay` defaults to **0** (PostgreSQL's `commit_delay`
   default). Set a positive delay only for spinning disks.
4. **WAL insert does not wait for `fdatasync`.** The write lock is dropped
   after the COMMIT record (and TSF1 footer) hit the page cache. Concurrent
   `SET`s keep appending so the next group already has frames ready. The
   commit path also skips `stat` — preallocated segments remember they have
   a footer slot. Replication and sync quorum still wait for `durableOffset`
   (the fsync snapshot), not the live write head: a concurrent `pwrite`
   during flush is not crash-safe.
5. **`fdatasync` on Unix.** Same durable-write shortcut PostgreSQL uses:
   flush file data, skip inode metadata that `fsync` would write.
6. **No `BEGIN` WAL record.** Recovery treats `SET`/`DEL` without `COMMIT`
   as in-progress, unless that xid already committed. Copy-forward writes
   a `COMMIT` per copied xid. That cuts one log frame per write transaction.
7. **Keep WAL files open and mmap them.** GET used to `open` + `close`
   the segment on every read. Sealed segments keep a reader FD; the
   active segment reuses the writer. On Unix the file is also
   `mmap(MAP_SHARED)` with `madvise` (`RANDOM` for point GET, `SEQUENTIAL`
   on replay, `DONTNEED` on unmap).
8. **Shared buffers + decoded value cache.** An 8 KiB page pool
   (`SharedBuffersBytes`, default 64 MiB **per process**, split across
   databases) holds hot WAL pages so concurrent GETs share one copy
   instead of each `pread`'ing. A 64 MiB process-wide offset value cache
   sits in front for decoded payloads. Disable the page pool with
   `SharedBuffersBytes < 0`, the decoded cache with `ValueCacheBytes < 0`.
9. **Batch writes in one transaction.** `--batch N` on `turnstone bench`
   amortizes one group fsync across N keys — the same advice as
   multi-row `INSERT` in PostgreSQL.
10. **Fully allocated, recycled WAL segments.** New segments are
   `fallocate`d to the configured size (default 64 MiB). Retention
   **renames** retired files into `wal/recycle/` and the next rotation
   reuses them — PostgreSQL's `wal_recycle` pattern, so commit does not
   pay create/unlink/metadata growth. If the disk cannot reserve the
   segment (`ENOSPC` on a small tmpfs), Open still succeeds and the
   file grows with writes.

## How to measure

### In-process engine (no TLS, no SQL)

```bash
make bench
# or
go test -run='^$' -bench='BenchmarkDB_(Insert|Read|Update)Parallel' -benchmem ./engine/
```

`InsertParallel` / `ReadParallel` are the closest apples-to-apples
comparison with a PostgreSQL client doing one primary-key write or read
per transaction.

### Networked Turnstone (includes mTLS)

```bash
./bin/turnstone init --home /tmp/ts-perf --ip 127.0.0.1,localhost
./bin/turnstone server --home /tmp/ts-perf --dev
./bin/turnstone bench --home /tmp/ts-perf --duration 15s --concurrency 50 --read-ratio 0.5
./bin/turnstone bench --home /tmp/ts-perf --batch 10 --depth 4 --ops 100000
```

mTLS and one-request-per-command framing cost latency. Use `--batch` and
`--depth` when comparing pipelined PostgreSQL (`INSERT` multi-row or
`COPY`).

### Side-by-side script

```bash
scripts/bench-vs-postgres.sh
```

The script always runs the engine microbenchmarks. If `psql` can reach a
server (`PGHOST` / `PGDATABASE` / …), it runs the same KV shape against
`TEXT PRIMARY KEY` + `BYTEA` with `synchronous_commit=on` and prints both
sides.

## Operator checklist

- **Do not** set a multi-millisecond `CommitDelay` on NVMe. That was the
  old 2 ms default and capped serial commit rate near 500 TPS.
- **Do** batch independent keys in one transaction.
- **Do** keep the working set inside `SharedBuffersBytes` (WAL pages)
  and `ValueCacheBytes` (decoded values) if the goal is memory-latency GETs.
- **Do not** compare Turnstone `--dev` + `UnsafeDisableFsync` to
  production PostgreSQL.
- **Do** pin PostgreSQL to the same disk, `fsync=on`, and a single
  primary-key table.

## What will not get you there

- Adding SQL. A planner puts you back in PostgreSQL's complexity class.
- Synchronous replication quorum (`promote N` with N > 0). That is a
  latency floor PostgreSQL sync replicas share.
- Huge values that miss the cache. Those reread the WAL, while
  PostgreSQL may already have the page in `shared_buffers`.
