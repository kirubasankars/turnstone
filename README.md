<!--
Copyright (c) 2026 Kiruba Sankar Swaminathan

This source code is licensed under the MIT license found in the
LICENSE file in the root of this source tree.
-->

# TurnstoneDB

**TurnstoneDB** is a persistent, transactional key-value database in Go. Run it on a single machine, split workloads across logical databases, replicate to followers when you need redundancy, and back up the write-ahead log for disaster recovery.

> **Disclaimer:** TurnstoneDB is research-quality software. It ships group commit, MVCC, mTLS, and physical replication, but it is not recommended for mission-critical production use without further hardening.

## Why TurnstoneDB

| You need… | TurnstoneDB gives you… |
| --- | --- |
| Safe writes | ACID transactions with snapshot isolation |
| Simple ops | One binary, local disk, Redis-style `SELECT <db>` namespaces |
| Standby copies | Optional primary → follower replication per database |
| Recovery | Physical WAL backup and restore (full + differential chains) |
| Visibility | Prometheus metrics and a built-in devtool for local development |

TurnstoneDB is a **single-node store** with **optional replication**. It is not a distributed database — there is no Raft, sharding, or automatic failover.

---

## Quick start

**Prerequisites:** Go 1.25+ (see `go.mod`)

```bash
git clone https://github.com/kirubasankars/turnstone.git
cd turnstone
make build
```

**1. Initialize** a home directory (TLS certs + config):

```bash
./bin/turnstone init --home tsdata --ip 127.0.0.1,localhost
```

**2. Start the server** in development mode (auto-promote databases, no tx timeouts):

```bash
./bin/turnstone server --home tsdata --dev
```

**3. Use the database** — interactive CLI or devtool:

```bash
# CLI
./bin/turnstone cli --home tsdata
# begin → set mykey hello → commit → get mykey

# Devtool (opened automatically with --dev)
# http://127.0.0.1:8080 — browse keys, edit values, view metrics
```

The server listens on `:6379` by default. Logical databases `0`–`N` are independent keyspaces.

---

## Devtool

With `--dev`, TurnstoneDB serves a local web UI at **http://127.0.0.1:8080** (override with `--devtool-addr`).

| Section | What you can do |
| --- | --- |
| **Keys** | Search by prefix, browse, create, edit, and delete keys |
| **Monitor** | Live server stats and Prometheus metrics |

The devtool binds to localhost only. Use it for development and debugging — not as a production admin surface.

---

## Everyday commands

Full reference: **[docs/cli.md](docs/cli.md)**

| Command | Purpose |
| --- | --- |
| `turnstone init` | Create home directory, TLS certs, and `turnstone.json` |
| `turnstone server` | Run the database (`--dev` for local use + devtool) |
| `turnstone cli` | Interactive REPL or `cli exec <command>` one-shot |
| `turnstone bench` | Load and throughput benchmark |
| `turnstone backup` | Stream a physical WAL backup from a primary |
| `turnstone restore` | Restore WAL backups into a new home directory |

```bash
# One-shot read
./bin/turnstone cli exec get mykey

# Admin failover (requires --admin cert)
./bin/turnstone cli --admin exec promote

# Benchmark
./bin/turnstone bench --home tsdata --ops 10000 --concurrency 50
```

All writes require a transaction (`begin` → `set`/`del` → `commit`). Admin commands (`promote`, `stepdown`, `replicaof`, `flushdb`) need `--admin`.

---

## Replication and failover

Replication is configured **per database**. Each database follows:

`UNDEFINED` → `REPLICA` → `PRIMARY`

| Admin command | Effect |
| --- | --- |
| `replicaof <host:port> <db>` | Follow a remote primary |
| `stepdown` | Drain writes and return to `UNDEFINED` |
| `promote [min_replicas]` | Become primary; optional sync quorum |

**Manual failover (node A → node B):**

1. **Node A:** `select 1` → `stepdown`
2. **Node B:** `select 1` → `promote`
3. **Node A:** `select 1` → `replicaof <B>:6379 1`

There is no automatic leader election — an operator runs failover.

Sync replication (`promote N` with N > 0) waits for N follower ACKs after each commit. Backup streams do not count toward quorum.

---

## Backup and restore

Back up a running primary, then restore offline into a fresh home directory:

```bash
# Full WAL backup
./bin/turnstone backup --home tsdata --host localhost:6379 --db 1 --out backup_full

# Differential backup (smaller, resumes from previous end LSN)
./bin/turnstone backup --home tsdata --db 1 --type differential \
  --base-meta backup_full/backup.meta --out backup_diff

# Restore chain and serve
./bin/turnstone restore --chain backup_full,backup_diff --out restored_home
./bin/turnstone server --home restored_home --dev
```

See **[docs/cli.md](docs/cli.md)** for `backup.meta` schema, chain validation, and LSN semantics.

---

## Configuration

Key fields in `turnstone.json`:

| Field | Default | Description |
| --- | --- | --- |
| `port` | `:6379` | Listen address |
| `number_of_databases` | `4` | Logical databases (`0` … `N`) |
| `log_retention` | `replication` | WAL purge policy: `replication` or `none` |
| `max_disk_usage_percent` | `90` | Reject writes above this disk usage |
| `metrics_addr` | `:9090` | Prometheus scrape address |

All connections use **mTLS**. Client authorization is driven by the certificate **Organization** field.

---

## Observability

| Surface | Address | Use |
| --- | --- | --- |
| Prometheus | `:9090` (configurable) | Scrape server and per-database metrics |
| Devtool | `127.0.0.1:8080` (with `--dev`) | Key browser and live dashboard |
| `stat` command | CLI (`--admin`) | JSON replication state, offsets, replica lag |

---

## Limitations

1. **Single node** — one process, local disk; scale-out needs application-level sharding.
2. **Manual failover** — no Raft/Paxos; operators run `stepdown` / `promote`.
3. **No lock waiting** — hot-key conflicts return immediately; clients must retry.
4. **Keys and bytes only** — not SQL.

---

## Documentation

| Topic | Guide |
| --- | --- |
| CLI reference | [docs/cli.md](docs/cli.md) |
| Architecture & reading order | [docs/README.md](docs/README.md) |
| Storage engine internals | [engine/README.md](engine/README.md) |
| Replication | [database/README.md](database/README.md), [repl/README.md](repl/README.md) |
| Wire protocol | [protocol/README.md](protocol/README.md) |
| Package index | [docs/README.md](docs/README.md) |

---

## License

MIT — see [LICENSE](LICENSE).
