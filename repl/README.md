# `repl/` — Outbound replication manager

The `repl` package manages **client-side replication connections**: when an operator runs `replicaof <host> <db>` on a node, the `Manager` dials the remote primary and continuously applies `ReplLogRange` payloads to the local `database.Database`.

Replication configuration is **runtime-only** — not persisted across server restarts. Operators must re-issue `replicaof` after restart (by design).

## Core type: `Manager`

```go
type Manager struct {
    serverID   string
    peers      map[string][]Source   // remote addr → [{LocalDB, RemoteDB}, ...]
    cancelFunc map[string]context.CancelFunc
    stores     map[string]*database.Database
    tlsConf    *tls.Config
}
```

`Source` pairs a local database name with the remote database name on the leader.

## Key behaviors

| Method | Purpose |
| --- | --- |
| `Follow(addr, localDB, remoteDB)` | Start (or restart) replication goroutine for one mapping |
| `Unfollow(localDB)` | Cancel upstream sync for a database |
| `IsFollowing(db)` | Returns true if local DB already has an upstream — **prevents cascading replication** |
| `Source(db)` | Lookup current upstream address and remote DB name |

## Replication loop (conceptual)

```
Manager.Follow
    │
    ▼
mTLS dial leader:6379
    │
    ▼
ReplHello(local id, db, start offset from local WAL head)
    │
    ▼
read frames ◄── OpCodeReplLogRange (raw WAL bytes)
    │
    ▼
Database.ApplyLogRange ──► engine append + index update
    │
    ▼
ReplAck(offset) ──► leader updates ReplicaSlot
```

On disconnect, the loop retries with backoff (see implementation in `manager.go`).

## Cascade prevention

`IsFollowing` ensures a database that is already consuming a remote log cannot itself act as an intermediate relay in the same process. `TestReplication_Cascading_Rejected` in `server/` validates end-to-end behavior.

## Educational focus

### Why not persist `replicaof`?

Explicit operator control matches the **manual failover** philosophy: after promote/stepdown, the cluster topology is intentional, not resurrected from stale config files.

### Byte-oriented sync

Followers replicate **physical frames**, not logical `SET` commands. This preserves exact WAL layout and makes leader/follower binary-compatible at the storage layer.

### TLS identity

Outbound connections use `tls_client_cert_file` from config (often the server cert) with Organization `server` so the leader accepts the stream as replication traffic.

## Dependencies

```
repl
  ├── database   (ApplyLogRange, state checks)
  └── protocol   (replication opcodes)
```

## Review checklist

- [ ] Goroutine cancellation on `Unfollow` and server shutdown — no leaks.
- [ ] Start offset chosen correctly on full sync vs partial catch-up.
- [ ] Errors on `ApplyLogRange` trigger safe retry without corrupting index.
- [ ] One `Follow` per `(addr, localDB)` — duplicate registration handled.
- [ ] Panic recovery does not leave DB stuck in `REPLICA` without active connection.

## Suggestions

- Optional `repl.state.json` for opt-in persistence of `replicaof` mappings.
- Metrics: `turnstone_repl_upstream_connected`, bytes applied per second.
- Expose last error / last successful offset via `STAT` for operators.
