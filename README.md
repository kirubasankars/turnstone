# TurnstoneDB

**TurnstoneDB** is a high-performance, persistent, distributed Key-Value store written in Go. It features a custom storage engine inspired by **WiscKey** (separating keys from values), full ACID transactions via Snapshot Isolation, and robust asynchronous replication with **Timeline** support for safe failovers.

> ⚠️ **Disclaimer:** TurnstoneDB is currently research-quality software. While it implements advanced mechanisms like Group Commit, MVCC, and mTLS, it is not recommended for mission-critical production workloads without further hardening.

## 🚀 Key Features

* **⚡ WiscKey-style Storage Engine**: Uses a LevelDB LSM-tree for the index (Keys) and an append-only Value Log (VLog) for values. This minimizes write amplification and drastically improves throughput for large payloads.
* **🚅 Group Commit Pipeline**: Writes are aggregated into batches and flushed to the Write-Ahead Log (WAL) and VLog in a single I/O operation, maximizing disk bandwidth.
* **🔒 ACID Transactions**: Full support for multi-key transactions with **Snapshot Isolation**. Uses Optimistic Concurrency Control (OCC) to detect conflicts (No stale reads, First-committer-wins).
* **🛡️ Secure by Default**: All connections (Client-Server and Inter-Node) are secured via **mTLS** (Mutual TLS). Role-Based Access Control (RBAC) is enforced via X.509 Certificate Organization fields.
* **📡 Replication & Timelines**: Database-level Leader-Follower replication. Supports **Timelines** to handle split-brain scenarios and allow safe history divergence during promotion.
* **🌊 Change Data Capture (CDC)**: Built-in support for streaming data changes to local JSONL logs with configurable rotation policies.
* **🦆 DuckDB Integration**: Includes a specialized loader (`turnstone-duck`) to ingest CDC streams directly into DuckDB for high-speed OLAP analytics.
* **🔭 Observability**: Built-in **Prometheus** exporter (`/metrics`) for tracking active transactions, conflicts, WAL size, and replication lag.

---

## 🛠️ Architecture

TurnstoneDB separates the storage of keys and values to optimize for modern SSDs:

1. **LevelDB (Index)**: Stores `Key + (MaxUint64 - TxID) -> <FileID, Offset, Size>`. This encoding allows for efficient MVCC lookups (time-travel queries) and keeps the LSM tree small.
2. **Value Log (VLog)**: Stores the actual values on disk in append-only files. Garbage collection is performed only when a file exceeds a configurable staleness threshold.
3. **Write-Ahead Log (WAL)**: Ensures durability. Supports retention strategies based on time or replication acknowledgment.

**The Write Path:**

1. A Transaction buffers writes in memory.
2. On `Commit`, the request enters the **Group Commit** channel.
3. The Committer aggregates multiple requests into a single Batch.
4. **Persist Stage**: Batch is appended to WAL (fsync) and VLog.
5. **Apply Stage**: In-memory Index is updated.

---

## 📦 Installation & Getting Started

### Prerequisites

* Go 1.22 or higher.

### 1. Build the Binaries

```bash
# Clone the repository
git clone https://github.com/yourusername/turnstone.git
cd turnstone

# Build server and tools
go build -o bin/turnstone ./cmd/turnstone
go build -o bin/turnstone-cli ./cmd/turnstone-cli
go build -o bin/turnstone-bench ./cmd/turnstone-bench
go build -o bin/turnstone-load ./cmd/turnstone-load
go build -o bin/turnstone-duck ./cmd/turnstone-duck

```

### 2. Initialize Configuration & Certificates

TurnstoneDB requires mTLS certificates. The `--init` flag generates a complete environment with a CA, server/client/admin certs, and default config files.

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

* Listens on `:6379` by default.
* **Note**: Database `0` is reserved as a read-only system database. User data goes into Databases `1` through `16` (configurable).

---

## 💻 Usage

### Using the CLI (`turnstone-cli`)

The CLI automatically detects certificates in the `--home` directory.

```bash
# Connect to the server
./bin/turnstone-cli --home tsdata

```

#### Basic Commands

```bash
> select 1
OK

> set mykey "Hello Turnstone"
OK

> get mykey
OK: Hello Turnstone

# Atomic Transaction
> begin
OK
> set tx_key_1 "Value A"
OK
> set tx_key_2 "Value B"
OK
> commit
OK

```

#### Batch Operations (MGET / MSET)

TurnstoneDB supports pipelined batch operations for high throughput.

```bash
# Batch Set (MSET) - Atomic write of multiple keys
> mset user:1 "Alice" user:2 "Bob" user:3 "Charlie"
OK

# Batch Get (MGET) - Fetch multiple values in one round-trip
> mget user:1 user:2 user:3 non_existent_key
1) Alice
2) Bob
3) Charlie
4) (nil)

# Batch Delete (MDEL)
> mdel user:1 user:2
(integer) 2

```

### Benchmarking

Stress test the server using the built-in benchmark tool, which supports pipeline depth and batching.

```bash
# Run a mixed workload (50% Read / 50% Write) with 50 concurrent clients
# Pipeline depth 5 (5 transactions in flight), Batch size 10 (10 ops per tx)
./bin/turnstone-bench --home tsdata -c 50 -n 100000 -ratio 0.5 -depth 5 -batch 10

```

---

## 📡 Replication & CDC

### Setting up Replication

TurnstoneDB supports database-level replication.

1. **Start Node A (Leader)** on port 6379.
2. **Start Node B (Replica)** on port 6380 (configure separate `home` dir).
3. **Connect to Node B** using the CLI and issue:

```bash
# Replicate Database 1 from Leader at localhost:6379
> select 1
> replicaof localhost:6379 1
Replication started from localhost:6379/1

```

* **Snapshotting**: If Node B is too far behind (WAL purged on Leader), it will automatically request a full Snapshot stream before switching to WAL streaming.
* **Safety**: The Leader propagates a "Safe Point" to followers, ensuring they don't purge WAL entries required by other stragglers.

### Running a Change Data Capture (CDC) Consumer

You can run a dedicated process to tail the transaction logs and output JSON for ETL pipelines.

#### 1. Configure the Consumer

Edit `tsdata/turnstone.cdc.json` to define the target database and output format:

```json
{
  "id": "cdc-worker-01",
  "host": "localhost:6379",
  "database": "1",
  "output_dir": "cdc_logs",
  "rotate_interval": "1m",
  "value_format": "json" 
}

```

#### 2. Start the CDC Process

```bash
./bin/turnstone --mode cdc --home tsdata

```

#### 3. Consume the Data

The CDC worker writes rotation-safe JSONL files to the `output_dir`. Example output:

```json
// cdc-1-1709320000.jsonl.part
{"seq": 101, "tx": 50, "key": "device:452", "val": {"lat": 34.05, "lon": -118.25}, "ts": 1709320000}
{"seq": 102, "tx": 50, "key": "device:452:status", "val": "active", "ts": 1709320000}
{"seq": 103, "tx": 51, "key": "session:99", "del": true, "ts": 1709320005}

```

* **`seq`**: Monotonically increasing Log Sequence Number (LSN).
* **`tx`**: Transaction ID. Events with the same `tx` were committed atomically.
* **`del`**: Boolean flag indicating a delete operation (tombstone).

### Analytics with DuckDB

Use `turnstone-duck` to watch CDC logs, deduplicate them (handling the "same key updated multiple times" scenario), and upsert them into a DuckDB database.

```bash
# Watch cdc_logs, load into duckdb, and archive processed files
./bin/turnstone-duck -input tsdata/cdc_logs -archive tsdata/archive -db analytics.duckdb

```

---

## ⚙️ Configuration (`turnstone.json`)

| Field | Default | Description |
| --- | --- | --- |
| `id` | `hostname` | Unique Node ID. |
| `port` | `:6379` | TCP listening address. |
| `max_conns` | `1000` | Maximum concurrent client connections. |
| `number_of_databases` | `16` | Number of logical databases. |
| `wal_retention` | `2h` | Duration to keep WAL files. |
| `wal_retention_strategy` | `time` | Strategy for WAL purge: `time` or `replication` (wait for replicas). |
| `max_disk_usage_percent` | `90` | Reject writes if disk usage exceeds this %. |
| `metrics_addr` | `:9090` | Address for Prometheus metrics. |
| `tls_cert_file` | `certs/server.crt` | Server Certificate. |
| `tls_client_cert_file` | `certs/server.crt` | Cert used when acting as a Replication Client. |

---

## 🏗️ Project Structure

* `cmd/`: Entry points (`turnstone`, `cli`, `bench`, `duck`, `load`).
* `stonedb/`: **Core Storage Engine**. Contains WAL, VLog, Group Committer, and Compaction logic.
* `server/`: TCP Server, Connection Multiplexing, and Protocol handling.
* `replication/`: Replication Manager and CDC logic.
* `protocol/`: Binary wire protocol definitions.
* `metrics/`: Prometheus collectors.

---

## ⚠️ Current Limitations

1. **Consensus**: Replication uses async/sync streaming. There is no automated Raft/Paxos failover; promotion must be triggered manually via API/CLI (though `Timelines` make this safe).
2. **Sharding**: The server is single-node (multi-db). Sharding must be handled client-side (see `cmd/turnstone-load2` for a reference implementation).
3. **Memory**: The index (LevelDB) relies heavily on OS Page Cache. Large datasets require sufficient RAM for optimal performance.

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.
