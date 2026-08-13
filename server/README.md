# `server/` — Network front door

The `server` package implements the **mTLS TCP listener**, per-connection protocol dispatch, RBAC, connection limits, and replication ingress/egress. It maps wire opcodes to `database.Database` methods.

## Core type: `Server`

```go
type Server struct {
    stores       map[string]*database.Database  // db name → instance
    replManager  *repl.Manager                  // outbound replicaof
    tlsConfig    *tls.Config
    maxConns     int
    sem          chan struct{}                  // connection backpressure
    devMode      bool
    // metrics, active client tracking for stepdown, buffer pools...
}
```

## Connection lifecycle

1. `Accept` on configured port (default `:6379`).
2. Acquire semaphore slot or reject with `SERVER_BUSY`.
3. TLS handshake; extract peer certificate Organization → role (`client`, `admin`, `server`).
4. Per-connection goroutine reads framed requests until `OpCodeQuit` or error.
5. Dispatch by opcode to handler; selected DB from connection state (`SELECT`).

Platform-specific TCP keepalive / user timeout: `connstatus_unix.go`, `connstatus_other.go`.

## Handler categories

| Category | Examples | Auth |
| --- | --- | --- |
| Data plane | `GET`, `SET`, `BEGIN`, `COMMIT`, `MGET` | `client` or `admin` |
| Admin plane | `PROMOTE`, `STEPDOWN`, `REPLICAOF`, `FLUSHDB` | `admin` only |
| Replication | `ReplHello`, `ReplAck`, `ReplLogRange`, `ReplSafePoint` | `server` role cert |
| Meta | `PING`, `STAT`, `SELECT` | `client`+ |

Implementation is concentrated in `server.go`; replication streaming in `replication.go`.

## Replication server path (`replication.go`)

When a follower connects:

1. `ReplHello` carries database name and starting **byte offset**.
2. Leader validates cursor via `Database.IsValidReplicationCursor`.
3. `streamDB` / `runLogStreamLoop` reads WAL ranges via `ReadLogRange` and sends `ReplLogRange` packets.
4. Follower acks advance `ReplicaSlot.Offset`; leader broadcasts `ReplSafePoint` for cluster retention.

Slow consumers may be dropped to protect leader memory — see `TestReplication_SlowConsumer_Dropped`.

## Step-down coordination

`StepDown` must:

- Stop accepting new write transactions on the database.
- Wait for in-flight client transactions to finish or abort.
- Ensure followers are caught up to a safe offset.
- Transition database to `UNDEFINED`.

`activeClients` map tracks connections per DB for forced drain scenarios.

## Security

- **mTLS required** — no plaintext fallback.
- **RBAC** via certificate `Organization` field (`RoleClient`, `RoleAdmin`, `RoleServer`).
- Buffer pool on read path mitigates allocation-based DoS.

## Educational focus

### Server vs database vs engine

| Layer | Knows about |
| --- | --- |
| `server` | TLS, roles, opcodes, connection limits |
| `database` | Replication roles, slots, retention orchestration |
| `engine` | WAL, MVCC, transactions |

Keep protocol parsing in `server`; do not duplicate MVCC rules here.

### Dev mode effects

Passed from `cmd/turnstone/server.go`: disables tx timeouts, auto-promotes DBs. Review any new guardrail to ensure dev mode bypass is intentional.

### Panic recovery

`recoverAndLog` in replication paths prevents one bad stream from crashing the process — verify new long-running loops use similar protection.

## Test coverage highlights

| Test file | Focus |
| --- | --- |
| `server_test.go` | RBAC, metrics, basic protocol |
| `replication_test.go` | Fan-out, cascading rejection, catch-up, retention |
| `consistency_test.go` | Cross-connection visibility |
| `overflow_test.go` | Large payload / limit enforcement |

CI runs **all packages with `-race`** in one invocation — concurrency bugs often span `server` ↔ `database`.

## Review checklist

- [ ] New opcodes: RBAC rule, payload bounds, error status mapping.
- [ ] Handlers respect `closing` / shutdown flag — no new work after `Close`.
- [ ] Replication never starts for DB in `REPLICA` state on same node (cascade check via `repl.Manager`).
- [ ] Idle and write timeouts applied consistently.
- [ ] Metrics hooks updated for new per-DB events.

## Suggestions

- Connection-level rate limiting per client cert CN.
- Structured request logging with sampled `xid` / offset for trace correlation.
- HTTP health endpoint separate from Prometheus for load balancers.
