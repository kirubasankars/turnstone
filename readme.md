
# High-Performance Serializable KV Store (Go)

A fast and persistent key-value store written in Go.
It supports **Serializable Snapshot Isolation (SSI)**, **MVCC**, and a hybrid storage engine using an append-only **WAL** and a **B+Tree** index.

---

## 🚀 Key Features

* **ACID Transactions**
  Full multi-key transactions with strict serial order.

* **Serializable Snapshot Isolation (SSI)**
  Detects:

  * write–write conflicts
  * read–write conflicts (write skew)
    No read locks.

* **Hybrid Storage Engine**

  * **Hot layer**: in-memory index for fast writes (`O(1)`).
  * **Warm layer**: background snapshot flush.
  * **Cold layer**: persistent B+Tree using `bbolt`.

* **Crash Recovery**
  Fast restart using WAL replay from last checkpoint.

* **Single Writer Model**
  One write path. No heavy locks on hot paths.

* **Simple Protocol**
  Text-based, Redis-like commands over TCP.

---

## 🏗️ Architecture

### Storage Tiers

* **Journal (WAL)**

  * File: `data.db`
  * Append-only
  * Source of truth
  * File offsets act as logical time

* **In-Memory Index**

  * **Active**: mutable map for new writes
  * **Flushing**: immutable snapshot being written to disk

* **Persistent Index**

  * File: `index.db`
  * Uses `bbolt`
  * Maps `Key → WAL Offset` for old data

---

### Concurrency Model

* **Reads**

  * Mostly lock-free
  * Each transaction gets a snapshot offset at start
  * Reads scan versions to find visible data

* **Writes**

  * Buffered inside the transaction
  * On `END`, sent to a central run loop

* **Conflict Detection**

  * On commit, checks if keys in:

    * read set or
    * write set
      were changed after the transaction start

---

## 🛠️ Getting Started

### Prerequisites

* Go 1.18+

### Build

```bash
go build -o kvstore main.go
```

### Run

```bash
# Defaults (port :6379, ./data)
./kvstore

# Custom options
./kvstore -port :9000 -dir ./mydata -max-conns 5000
```

---

## 📡 Protocol & Commands

Text-based TCP protocol.
Each line ends with `\r\n`.

---

### Basic Operations

#### GET

```
GET <key>
```

Example:

```
> GET user:100
< OK 5
< alice
```

---

#### SET

```
SET <key> <length>
<value>
```

Example:

```
> SET user:100 5
> alice
< OK
```

---

#### DEL

```
DEL <key>
```

Example:

```
> DEL user:100
< OK
```

---

## 🔁 Transactions

Changes are private until `END`.

```
> BEGIN
< OK

> GET balance:A
< OK 3
< 100

> SET balance:A 3
> 150
< OK

> END
< OK
```

### Conflict Error

If a conflict is found during `END`:

```
< ERR_TX_CONFLICT
```

---

## 🧰 Administrative Commands

* `STAT` – server stats (uptime, keys, memory, offsets)
* `PING` – health check (`PONG`)
* `ABORT` – cancel current transaction
* `QUIT` – close connection

---

## ⚙️ Configuration

| Flag         | Default | Description                 |
| ------------ | ------- | --------------------------- |
| `-port`      | `:6379` | TCP listen port             |
| `-dir`       | `data`  | Data directory              |
| `-max-conns` | `10000` | Max client connections      |
| `-truncate`  | `false` | Truncate WAL on error       |
| `-skip-crc`  | `false` | Disable CRC checks (unsafe) |
| `-debug`     | `false` | Debug logging               |

---

## 🧠 Internals

### Data Consistency

**WAL Format**

```
[KeyLen:u32][ValLen:u32][CRC:u32][KeyBytes][ValueBytes]
```

* Deletes use tombstones (`ValLen = ^uint32(0)`)
* File offsets are logical time
  Smaller offset means earlier write

---

### Pruning and Memory

* **MVCC Versions**

  * Multiple versions kept for snapshot reads

* **Garbage Collection**

  * Tracks oldest active snapshot
  * Older versions are safe to drop

* **Backpressure**

  * If pending WAL writes exceed 128MB
    new writes return:

```
ERR_SERVER_BUSY
```

---

## 🔄 Recovery

On startup:

1. Read last checkpoint offset from `index.db`
2. Scan `data.db` from that offset
3. Rebuild in-memory active index
4. Start serving requests

---

## ⚠️ Limitations

* **Max value size**: 8KB
* **Max transaction time**: 5 seconds
  Prevents blocking cleanup
* **Memory usage**:
  Long transactions with heavy writes increase version history
