# `client/` — Go client library

The `client` package provides a **typed Go API** for talking to a TurnstoneDB server over mTLS. It is used by the `turnstone cli` and `bench` commands and is suitable for embedding in application code or integration tests.

## Responsibilities

- Dial the server with mutual TLS (`internal/tlsutil`).
- Frame requests and decode responses using `protocol` opcodes and status bytes.
- Map wire-level status codes to typed Go errors (`ErrTxConflict`, `ErrNotFound`, etc.).
- Manage optional transaction state on the client side (tracking whether a `BEGIN` is active).

## Key types

| Type | Purpose |
| --- | --- |
| `Config` | Address, client ID, cert paths, I/O timeouts |
| `Client` | Connection handle; methods mirror REPL commands |
| `ServerError` | Generic server-side error message (`ResStatusErr`) |

## Error mapping

`mapStatusToError` translates response status bytes into stable sentinel errors. Callers should use `errors.Is` rather than string matching:

```go
if errors.Is(err, client.ErrTxConflict) {
    // retry with backoff
}
```

Default I/O timeout is **30 seconds** when `ReadTimeout` / `WriteTimeout` are zero, preventing hung goroutines on partitioned networks.

## API surface (conceptual)

| Method family | Transaction required? | Notes |
| --- | --- | --- |
| `Get`, `MGet` | Read-only tx or implicit read path per server rules | Returns `ErrNotFound` |
| `Set`, `Del`, `MSet`, `MDel` | Yes (`Begin` first) | Eager logging on server at call time |
| `Begin`, `Commit`, `Abort` | — | Snapshot isolation on server |
| `Select` | No | Switches logical database namespace |
| Admin ops | Admin cert + server RBAC | `ReplicaOf`, `Promote`, `StepDown` |

Consult `client.go` for the authoritative method list.

## Dependencies

```
client
  ├── protocol   (opcodes, frame encoding)
  └── tlsutil    (cert loading from home dir)
```

The client does **not** link to `engine` or `database` — it only speaks the wire protocol.

## Educational focus

### Client vs server transaction model

The server enforces ACID semantics; the client only tracks whether it sent `BEGIN` and must send `COMMIT` or `ABORT`. A disconnect mid-transaction causes the server reaper to abort stale transactions.

### Retry strategy

`ErrTxConflict` (first-writer-wins) and `ErrServerBusy` are expected under load. Application code should implement bounded retries with jitter — the engine does not block on key locks.

### Testing pattern

`client_test.go` uses the external test package (`client_test`) to exercise the public API against a real or test server. Follow this pattern for black-box integration tests.

## Review checklist

- [ ] New protocol opcodes have matching `Client` methods and error mappings.
- [ ] Timeouts are applied to both read and write paths in `roundTrip`.
- [ ] Connection errors wrap underlying `net` errors with `ErrConnection` where appropriate.
- [ ] No breaking changes to status-code mapping without updating `protocol` docs.
- [ ] Large payloads respect `protocol.MaxValueSize` / `MaxCommandSize` client-side where feasible.

## Suggestions

- Export a `RetryPolicy` helper for `ErrTxConflict` with configurable backoff.
- Add context-aware methods (`GetContext`) if long-running callers need cancellation.
- Document thread-safety: typically one `Client` per goroutine or external serialization.
