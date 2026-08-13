# `database/` — Logical database + replication orchestration

The `database` package wraps `engine.DB` with **per-keyspace lifecycle**: replication role state machine, follower slot tracking, retention policy, quorum waits, and admin-level operations (`promote`, `stepdown`, `replicaof` effects).

One `Database` instance corresponds to one logical namespace (e.g. `"0"`, `"1"`).

## State machine

```
UNDEFINED ──replicaof──► REPLICA
     ▲                      │
     │                      │ promote (on follower)
     │ stepdown             ▼
     └────────────── PRIMARY ◄── promote (on undefined)
```

| State | Writes allowed? | Replication behavior |
| --- | --- | --- |
| `UNDEFINED` | No (until promoted) | Neither leader nor follower |
| `REPLICA` | No (applies remote WAL) | Consumes `ReplLogRange` from primary |
| `PRIMARY` | Yes | Streams WAL to registered replicas |
| `STEPPING_DOWN` | Draining | Blocks new writes; syncs followers |

States are persisted indirectly via `repl.slots` and runtime configuration — there is no separate state file; role is operator-driven.

## Core type: `Database`

```go
type Database struct {
    *engine.DB          // storage engine
    state string         // UNDEFINED | PRIMARY | REPLICA | STEPPING_DOWN
    replicas map[string]*ReplicaSlot
    retentionStrategy string
    minReplicas int
    // ...
}
```

`ReplicaSlot` tracks each follower's **exclusive-end byte offset** in the WAL, last seen time, and connection liveness.

## Major responsibilities

| Area | Functions / files | Description |
| --- | --- | --- |
| Open / close | `Open`, `Close` | Wraps `engine.Open`, starts background goroutines |
| Retention | `EnforceRetentionPolicy`, `runRetentionManager` | Computes scan floor from local mark, replica lag, leader safe point; calls `engine.RunWalMaintenance` |
| Replica registry | `RegisterReplica`, `UpdateReplicaOffset`, `UnregisterReplica` | Persists slots to `repl.slots` JSON |
| Quorum | `WaitForQuorum`, `SetMinReplicas` | Sync replication: commit waits for N followers to ack offset |
| Replication apply | `ApplyLogRange` | Follower ingests raw WAL frames from leader |
| Admin | `Promote`, `StepDown`, role checks | Coordinates with `server` handlers |
| Stats | `Stats` | Active txs, conflicts, log size, key count, replica lag |

## On-disk artifacts (per database)

```
<home>/<node-id>/<db>/
  wal/
    manifest.json
    seg-*.wal
  repl.slots          # follower ack positions (when primary)
```

## Interaction diagram

```
server (handlers)
    │
    ├─► Database.Get/Set/... ──► engine.Transaction
    │
    ├─► Database.RegisterReplica ──► repl.slots
    │
    └─► Database.EnforceRetentionPolicy ──► engine.RunWalMaintenance
                                              ├─ index compaction
                                              ├─ WAL copy-forward
                                              └─ segment delete

repl.Manager (outbound) ──► applies to follower Database via ApplyLogRange
```

## Educational focus

### Replication cursor vs transaction ID

Operators and metrics speak in **global byte LSN** (WAL offset). Transaction `xid` is orthogonal — used inside log records for MVCC. When reviewing replication bugs, always ask: “which offset is pinned?”

### Retention triangle

The effective delete floor is the minimum of:

1. Local retention mark (`MarkRetention`)
2. Slowest connected replica offset
3. Leader-propagated safe point (on followers)

`EnforceRetentionPolicy` raises `scanFloor` then triggers maintenance — study `retention_test.go` for worked examples.

### Zombie replica eviction

`evictZombieReplicas` removes stale slots so a dead follower does not pin WAL forever. Tune `replicaTimeout` when reviewing production retention.

## Review checklist

- [ ] Role transitions are atomic under `dbMu` / `mu` and visible to all handlers.
- [ ] `StepDown` drains active writers before returning to `UNDEFINED`.
- [ ] Quorum wait respects cancellation on server shutdown.
- [ ] `ApplyLogRange` never splits frames across batches.
- [ ] Stats fields match Prometheus collector in `metrics/`.

## Suggestions

- Expose replication lag in human time (bytes → seconds estimate) in `Stats`.
- Structured audit log for role transitions with operator cert subject.
- Integration test matrix: async vs sync replication × retention modes.
