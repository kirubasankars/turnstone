```markdown
# TurnstoneDB

**TurnstoneDB** is a high-performance, transactional Key-Value store written in Go. It features a custom storage engine inspired by **WiscKey** (separating keys from values), built-in **Change Data Capture (CDC)**, and support for **synchronous replication**.

> ⚠️ **Disclaimer:** TurnstoneDB is currently research-quality software. It is functional and passes integration tests but is **not** recommended for mission-critical production workloads without further hardening of the replication and compaction subsystems.

## 🚀 Key Features

* ⚡ **WiscKey-style Storage Engine:** Uses an LSM-tree (LevelDB) for keys and an append-only Value Log (VLog) for values. This minimizes write amplification and improves throughput for large values.
* 🔒 **ACID Transactions:** Full support for multi-key transactions with **Snapshot Isolation**. Uses Optimistic Concurrency Control (OCC) to handle conflicts.
* 🛡️ **Secure by Default:** All connections (Client-Server and Inter-Node) are secured via **mTLS** (Mutual TLS). Certificates are auto-generated during initialization.
* 📡 **Replication:** Partition-aware Leader-Follower replication. Supports **Quorum Writes** (`minReplicas`) for synchronous durability.
* 🌊 **Change Data Capture (CDC):** First-class support for streaming data changes to JSON logs or external systems.
* 🦆 **DuckDB Integration:** Includes a specialized loader (`turnstone-duck`) to ingest CDC streams directly into DuckDB for analytics.

---

## 🛠️ Architecture

TurnstoneDB separates the storage of keys and values using the `stonedb` engine:

1.  **LevelDB (Index):** Stores `Key -> <FileID, Offset, Size>`. Keeps the index small and cacheable.
2.  **Value Log (VLog):** Stores the actual values on disk in append-only files.
3.  **Write-Ahead Log (WAL):** Ensures durability and crash recovery.

### Write Flow & Group Commit
To maximize throughput, TurnstoneDB uses an asynchronous **Group Commit Pipeline**:

1.  **Prepare:** Transactions buffer writes in memory. On commit, conflicts are checked (OCC).
2.  **Persist:** Concurrent transactions are batched together. The batch is appended to the WAL and VLog in a single I/O operation (fsync).
3.  **Apply:** The in-memory Index is updated, and transactions are acknowledged to the client.

### Crash Recovery
The system is crash-safe. On startup:

* **WAL Replay:** Any committed data in the WAL that is not yet in the VLog/Index is replayed.
* **Truncation:** Partially written batches at the end of the WAL (due to power loss) are detected and truncated to ensure atomicity.
* **Index Rebuild:** If the index is missing or corrupt, it can be fully reconstructed from the persistent Value Log.

---

## 📦 Installation & Getting Started

### Prerequisites
* Go 1.21 or higher.
* `make` (optional, for convenience).

### 1. Build the Binaries
TurnstoneDB comes with a server and several utility tools.

```bash
# Clone the repository
git clone [https://github.com/yourusername/turnstone.git](https://github.com/yourusername/turnstone.git)
cd turnstone

# Build all binaries
go build -o bin/turnstone ./cmd/turnstone
go build -o bin/turnstone-cli ./cmd/turnstone-cli
go build -o bin/turnstone-bench ./cmd/turnstone-bench
go build -o bin/turnstone-load ./cmd/turnstone-load
go build -o bin/turnstone-duck ./cmd/turnstone-duck

```

### 2. Initialize Configuration & Certificates

TurnstoneDB requires mTLS certificates. The `--init` flag generates a complete environment with a CA, server/client certs, and default config files.

```bash
# Create a data directory and generate artifacts
./bin/turnstone --init --home tsdata

# Output:
# Certificates generated in: tsdata/certs
# Sample configuration written to tsdata/turnstone.json
# Sample CDC configuration written to tsdata/turnstone.cdc.json

```

### 3. Start the Server

```bash
./bin/turnstone --home tsdata

```

* Listens on `:6379` by default (Configurable in `tsdata/turnstone.json`).
* **Note:** Partition `0` is reserved as a read-only system partition. User data goes into Partitions `1` through `16`.

---

## 💻 Usage

### Using the CLI (`turnstone-cli`)

The CLI automatically looks for certificates in the `--home` directory.

```bash
# Connect to the server
./bin/turnstone-cli --home tsdata

# Commands:
> select 1
OK
> set mykey "Hello Turnstone"
OK
> get mykey
OK: Hello Turnstone
> begin
OK
> set txkey "This is atomic"
OK
> commit
OK

```

### Benchmarking

Stress test the server using the built-in benchmark tool.

```bash
# Run a mixed workload (50% Read / 50% Write) with 50 concurrent clients
./bin/turnstone-bench --home tsdata -c 50 -n 100000 -ratio 0.5

```

---

## 📡 Replication & CDC

### Setting up Replication

TurnstoneDB supports partition-level replication. To make a node a replica of another:

1. **Start Node A (Leader)** on port 6379.
2. **Start Node B (Replica)** on port 6380 (configure separate `home` dir).
3. **Connect to Node B** using the CLI (as Admin) and issue:

```bash
# Replicate Partition 1 from Leader at localhost:6379
> select 1
> replicaof localhost:6379 1
Replication started from localhost:6379/1

```

### Running a CDC Consumer

You can run a dedicated process to tail the transaction logs and output JSON for ETL pipelines.

1. Edit `tsdata/turnstone.cdc.json` if needed.
2. Run the CDC sidecar:

```bash
./bin/turnstone --mode cdc --home tsdata

```

This will stream changes to `tsdata/cdc_logs/cdc-*.jsonl`.

### Analytics with DuckDB

Use `turnstone-duck` to watch the CDC logs and ingest them into a DuckDB database for SQL analysis.

```bash
./bin/turnstone-duck -input tsdata/cdc_logs -db analytics.duckdb

```

---

## ⚙️ Configuration (`turnstone.json`)

The configuration file controls server behavior and storage engine tuning.

### Server Options

| Field | Default | Description |
| --- | --- | --- |
| `port` | `:6379` | TCP listening address. |
| `max_conns` | `1000` | Maximum concurrent client connections. |
| `number_of_partitions` | `16` | Number of data partitions (shards). |
| `tls_cert_file` | `certs/server.crt` | Server TLS Certificate. |
| `tls_key_file` | `certs/server.key` | Server Private Key. |
| `tls_client_auth` | `true` | Enforces mTLS client verification. |

### Storage Engine Options (StoneDB)

These settings apply per-partition.

| Field | Default | Description |
| --- | --- | --- |
| `wal_max_size` | `10MB` | Size limit for a WAL file before rotation. |
| `wal_retention` | `2h` | Duration to keep WAL files after they are checkpointed. |
| `compaction_min_garbage` | `1MB` | Stale data threshold to trigger VLog garbage collection. |
| `truncate_corrupt_wal` | `false` | If true, allow startup by truncating corrupt tail data. |

---

## 🏗️ Project Structure

* `cmd/`: Entry points (`turnstone`, `cli`, `bench`).
* `stonedb/`: **Core Storage Engine**. Contains WAL, VLog, Compaction, and Transaction logic.
* `server/`: TCP Server, Connection Multiplexing, and Replication handlers.
* `client/`: Go client library implementation.
* `replication/`: CDC client and Replication Manager logic.
* `protocol/`: Binary wire protocol definitions.

---

## ⚠️ Current Limitations (Roadmap)

1. **Consensus:** Replication currently uses simple leader-follower async/sync streaming. No Raft/Paxos support yet for automatic failover.
2. **Flow Control:** The replication stream lacks backpressure; slow followers may be disconnected by the leader.
3. **Compaction:** Background garbage collection is simple and may compete with foreground writes under heavy load.

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

```

```
