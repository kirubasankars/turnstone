# TurnstoneDB CLI guide

TurnstoneDB ships a single binary, `turnstone`, with subcommands for setup, serving, client access, benchmarking, and WAL backup/restore. All subcommands share a global `--home` flag (default `tsdata`) that points at the data directory containing TLS certificates, `turnstone.json`, and per-database storage.

## Build

```bash
make build
# produces bin/turnstone
```

## Global flags

| Flag | Default | Description |
| --- | --- | --- |
| `--home` | `tsdata` | Home directory for certs, config, and data |
| `-v`, `--version` | — | Print version and exit |

## Subcommands

| Command | Purpose |
| --- | --- |
| `turnstone init` | Create home directory, TLS certs, and `turnstone.json` |
| `turnstone server` | Run the database server |
| `turnstone cli` | Interactive REPL or `cli exec <command>` one-shot |
| `turnstone bench` | Load and throughput benchmark |
| `turnstone backup` | Stream a physical WAL backup from a primary |
| `turnstone restore` | Restore WAL backups into a new home directory |

---

## `turnstone init`

Create a new home directory with TLS certificates, database directories, and `turnstone.json`.

```bash
turnstone init --home tsdata --ip 192.168.1.10,myserver.local
```

| Flag | Description |
| --- | --- |
| `--ip` | Comma-separated IPs or hostnames added to the server certificate SANs |

After init, the home directory contains:

```
tsdata/
  turnstone.json       # server configuration
  certs/               # CA, server, client, and admin certificates
  data/
    0/                 # logical database 0
    1/
    ...
```

Databases start in `UNDEFINED` replication state. They must be promoted to `PRIMARY` before accepting writes (unless the server runs with `--dev`).

---

## `turnstone server`

Start the database server. Reads `turnstone.json` from `--home` and listens on the configured port (default `:6379`).

```bash
turnstone server --home tsdata
```

### Flags

| Flag | Description |
| --- | --- |
| `--dev` | Disable transaction timeouts and auto-promote all databases to `PRIMARY` |

Use `--dev` for local development only. Production failover semantics are bypassed.

Prometheus metrics are served on the address in `metrics_addr` (default `:9090`).

---

## `turnstone cli`

Connect to a running server over mTLS. Certificates are loaded from `<home>/certs/`.

### Connection flags

| Flag | Default | Description |
| --- | --- | --- |
| `--host` | `localhost:6379` | Server address |
| `--admin` | off | Use admin certificate (required for admin commands) |
| `--debug` | off | Enable debug logging |

### Interactive REPL

```bash
turnstone cli --home tsdata
```

On connect, the REPL prints available commands and shows the current database in the prompt:

```
Connected.
Commands: select <db>, replicaof <host:port> <remote_db>, promote [min_replicas], stepdown, flushdb, get <k>, set <k> <v>, del <k>, mget <k>..., mset <k> <v>..., mdel <k>..., begin [read], commit, abort, stat, clear, quit
0> 
```

Type `quit` or `exit` to leave. Type `clear` or `cls` to clear the screen.

### One-shot execution

Run a single command without entering the REPL:

```bash
turnstone cli exec get mykey
turnstone cli exec "mget key1 key2 key3"
turnstone cli --admin exec promote
```

### REPL commands

#### Database selection

| Command | Description |
| --- | --- |
| `select <db>` | Switch logical database (`0`, `1`, …) |
| `ping` | Health check; responds `PONG` |

#### Key-value operations

All reads and writes run inside a transaction. Call `begin` before `set`, `del`, `mget`, `mset`, or `mdel`. `get` can be used outside a transaction.

| Command | Description |
| --- | --- |
| `begin` | Start a read-write transaction |
| `begin read` | Start a read-only snapshot transaction |
| `commit` | Commit the active transaction |
| `abort` | Abort the active transaction |
| `get <key>` | Read a key |
| `set <key> <value>` | Write a key (requires active transaction) |
| `del <key>` | Delete a key (requires active transaction) |
| `mget <key1> [<key2> ...]` | Read multiple keys (requires active transaction) |
| `mset <key1> <val1> [<key2> <val2> ...]` | Write multiple keys (requires active transaction) |
| `mdel <key1> [<key2> ...]` | Delete multiple keys (requires active transaction) |

Example session:

```
0> select 1
OK
1> begin
OK
1> set mykey hello
OK
1> get mykey
OK: hello
1> commit
OK
```

#### Admin commands

Require `--admin` and an admin certificate from init.

| Command | Description |
| --- | --- |
| `promote [min_replicas]` | Promote database to `PRIMARY`; optional sync quorum (see below) |
| `stepdown` | Drain writes and return to `UNDEFINED` |
| `replicaof <host:port> <remote_db>` | Follow a remote primary |
| `flushdb` | Delete all keys in the current database |
| `stat` | Print JSON status (replication state, log offset, replica slots and lag, etc.) |

Example `stat` response on a primary with one connected follower:

```json
{
  "state": "PRIMARY",
  "key_count": 42,
  "log_offset": 8192,
  "replica_lag": 0,
  "min_replicas": 0,
  "replicas": [
    {
      "id": "stat_replica",
      "role": "server",
      "connected": true,
      "offset": 8192,
      "lag": 0,
      "last_seen": "2026-09-12T23:45:00Z"
    }
  ]
}
```

`replica_lag` is the lag in bytes of the **slowest** registered consumer. Each entry in `replicas` includes that consumer's ack offset and individual lag behind `log_offset`.

### Common errors

| Output | Meaning |
| --- | --- |
| `(nil)` | Key not found |
| `ERR: Transaction Required` | Mutating command called without `begin` |
| `ERR: Conflict Detected (Retry)` | Write conflict; retry the transaction |
| `ERR: Transaction Timeout` | Transaction exceeded `MaxTxDuration` (30s) |
| `ERR: Server Busy` | Connection limit reached, or sync-replication quorum not met after local commit |
| `ERR: Entity Too Large` | Value exceeds size limit |
| `ERR: Server Memory Limit Exceeded` | Disk usage above configured threshold |

### Sync replication (`promote N`)

When a database is promoted with `min_replicas = N` (N > 0), every successful `commit` on the primary is **written to local WAL first**, then the server waits until at least **N connected server-role replicas** have ACKed the commit's end byte offset before returning `OK` to the client. If replicas are slow or disconnected, the client may receive `ERR: Server Busy` even though the write is already durable on the primary.

Backup streams (`turnstone-backup`) and admin replication connections do **not** count toward quorum and do **not** pin WAL retention on the leader.

---

## `turnstone bench`

Run a concurrent load and throughput benchmark against a running server.

```bash
turnstone bench --home tsdata --addr localhost:6379 --ops 10000 --concurrency 50
```

The database must be in `PRIMARY` state. If not, promote it first or start the server with `--dev`.

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--addr` | `localhost:6379` | Server address |
| `--concurrency` | `50` | Number of concurrent clients |
| `--ops` | `10000` | Total operations per phase |
| `--db` | `1` | Logical database number |
| `--batch` | `1` | Operations per transaction |
| `--depth` | `1` | Pipeline depth (transactions per round-trip) |
| `--value-size` | `128` | Value size in bytes for SET operations |
| `--key-size` | `32` | Minimum key size in bytes |
| `--prefix` | `bench` | Key prefix to avoid collisions between runs |
| `--read-ratio` | `-1` | Read ratio `0.0`–`1.0` for mixed workload; `-1` runs separate write then read phases |

### Examples

Mixed 70% read / 30% write workload:

```bash
turnstone bench --home tsdata --read-ratio 0.7 --ops 50000 --concurrency 100
```

High-throughput pipelined writes:

```bash
turnstone bench --home tsdata --batch 10 --depth 5 --ops 100000
```

---

## Typical workflows

### Local development

```bash
turnstone init --home tsdata --ip 127.0.0.1
turnstone server --home tsdata --dev
turnstone cli --home tsdata
```

### Production setup

```bash
# Node A — primary
turnstone init --home /var/lib/turnstone --ip 10.0.0.1
turnstone server --home /var/lib/turnstone
turnstone cli --home /var/lib/turnstone --admin
# select 1
# promote

# Node B — replica
turnstone init --home /var/lib/turnstone --ip 10.0.0.2
turnstone server --home /var/lib/turnstone
turnstone cli --home /var/lib/turnstone --admin
# select 1
# replicaof 10.0.0.1:6379 1
```

### Manual failover (primary A → primary B)

On node A:

```
select 1
stepdown
```

On node B:

```
select 1
promote
```

On node A (rejoin as replica):

```
select 1
replicaof <B-host>:6379 1
```

See the [root README](../README.md#replication-and-failover) for replication state machine details.

### Backup and restore

Back up a primary database, then restore offline into a new home directory:

```bash
# Full WAL backup from a running primary
turnstone backup --home tsdata --host localhost:6379 --db 1 --out backup_full

# Incremental differential backup (resumes at previous end_lsn)
turnstone backup --home tsdata --db 1 --type differential \
  --base-meta backup_full/backup.meta --out backup_diff

# Restore full + differential chain into a new home
turnstone restore --chain backup_full,backup_diff --out restored_home
turnstone server --home restored_home --dev
```

Requirements:

- The source database must be `PRIMARY`.
- Backup uses the admin certificate from `<home>/certs/`.
- Differential backups require the primary to retain WAL back to the differential `base_lsn`.

---

## `turnstone backup`

Stream raw WAL frames from a running **primary** database using the replication protocol (`ReplHello` + `ReplLogRange`). Backups are physical byte ranges keyed by **WAL LSN** (global byte offset in the segmented WAL).

### Full vs differential

| Type | Start LSN | Output | Use case |
| --- | --- | --- | --- |
| `full` | `0` | Complete WAL from the beginning | Base backup, disaster recovery baseline |
| `differential` | previous `end_lsn` | WAL delta since last backup | Smaller incremental captures between full backups |

Differential backups require either `--base-meta` (reads `end_lsn` from a prior `backup.meta`) or an explicit `--from-lsn`.

### Usage

```bash
# Full backup (starts at LSN 0)
turnstone backup --home tsdata --host localhost:6379 --db 1 --out backup_full

# Differential backup (resume from a previous backup.meta end_lsn)
turnstone backup --home tsdata --db 1 --type differential \
  --base-meta backup_full/backup.meta --out backup_diff1

# Differential backup with explicit start LSN
turnstone backup --home tsdata --db 1 --type differential --from-lsn 1048576 --out backup_diff2
```

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--host` | `localhost:6379` | Primary server address |
| `--db` | `1` | Database name to backup |
| `--out` | `backup_data` | Output directory for backup artifacts |
| `--file` | `wal.bin` | Backup filename (`.gz` appended when `--compress` is set) |
| `--type` | `full` | `full` or `differential` |
| `--from-lsn` | `0` | Start WAL LSN for differential backup (overrides `--base-meta` when set) |
| `--base-meta` | — | Path to previous `backup.meta` to resume from |
| `--compress` | on | GZIP the WAL artifact |
| `--wait` | `2s` | Idle time before finishing once caught up |

### Artifacts

Each backup writes:

```
<out>/
  wal.bin[.gz]     # raw concatenated WAL frames
  backup.meta      # JSON metadata (see below)
```

### `backup.meta` schema

| Field | Description |
| --- | --- |
| `timestamp` | Backup creation time |
| `database` | Logical database name |
| `type` | `full` or `differential` |
| `base_lsn` | Exclusive-start WAL LSN for this artifact |
| `end_lsn` | Exclusive-end WAL LSN after streaming completes |
| `parent_sha256` | SHA256 of parent artifact (differential only) |
| `compressed` | Whether `wal.bin` is gzip-compressed |
| `sha256` | SHA256 of the WAL artifact file |

Example:

```json
{
  "timestamp": "2026-09-12T22:00:00Z",
  "database": "1",
  "type": "differential",
  "base_lsn": 1048576,
  "end_lsn": 2097152,
  "parent_sha256": "abc123...",
  "compressed": true,
  "sha256": "def456..."
}
```

Legacy `backup.meta` files using `base_opid` / `end_opid` are still accepted on restore.

### Notes

- Uses the admin certificate from `<home>/certs/`.
- Streaming stops after `--wait` with no new WAL data (caught up).
- If WAL has been purged below the differential start LSN, the handshake fails with an invalid cursor error.

---

## `turnstone restore`

Rebuild a database offline by applying one or more physical WAL backups through `engine.ApplyLogRange`. The target `--out` home directory must **not** already exist.

Restore opens a fresh database under `<out>/data/<db>/`, replays each artifact in order, and leaves a runnable WAL on disk.

### Usage

```bash
# Restore a single full backup
turnstone restore --in backup_full --out restored_home

# Restore a full backup plus differential chain
turnstone restore --chain backup_full,backup_diff1,backup_diff2 --out restored_home

# Start the restored database
turnstone server --home restored_home --dev
```

When `--chain` is set, `--in` is ignored. Directories are applied left-to-right.

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--in` | `backup_data` | Single backup directory (ignored when `--chain` is set) |
| `--out` | `restored_data` | Target home directory to create |
| `--file` | `wal.bin` | Backup filename inside each directory (auto-detects `.gz`) |
| `--verify` | on | Verify SHA256 checksum before applying each artifact |
| `--chain` | — | Comma-separated backup directories in apply order |

### Chain validation

Restore validates that:

1. The first backup is `type=full` with `base_lsn=0`.
2. Each subsequent backup is `type=differential`.
3. Each differential's `base_lsn` equals the previous backup's `end_lsn`.
4. When present, `parent_sha256` matches the prior artifact's `sha256`.

All backups in a chain must target the same `database` field.

### After restore

The restored database is placed at `<out>/data/<db>/`. To serve it:

```bash
turnstone server --home restored_home
```

Promote the database if not using `--dev`:

```bash
turnstone cli --home restored_home --admin exec promote
```

---

## Shell completion

Generate autocompletion scripts for bash, zsh, fish, or PowerShell:

```bash
turnstone completion bash > /etc/bash_completion.d/turnstone
turnstone completion zsh > "${fpath[1]}/_turnstone"
```
