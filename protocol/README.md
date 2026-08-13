# `protocol/` — Wire format and shared constants

The `protocol` package is the **single source of truth** for the TurnstoneDB binary protocol: opcodes, response status codes, size limits, timeouts, and frame encoding helpers. Both `server` and `client` import it; the `engine` imports limits like `MaxTxDuration`.

## Frame layout

Every request and response begins with a **5-byte header**:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 1 | Opcode (`uint8`) |
| 1 | 4 | Payload length (`uint32`, big-endian) |

`wire.go` provides `AppendHeader`, `EncodeFrame`, and `StatusName` for debugging.

Payload structure is opcode-specific; see `protocol.go` comments and server handlers in `server/server.go`.

## Opcodes (selected)

| Opcode | Hex | Direction | Summary |
| --- | --- | --- | --- |
| `OpCodePing` | 0x01 | either | Health check |
| `OpCodeGet` | 0x02 | client → server | Read key |
| `OpCodeSet` | 0x03 | client → server | Write key (in tx) |
| `OpCodeDel` | 0x04 | client → server | Delete key (in tx) |
| `OpCodeSelect` | 0x05 | client → server | Choose logical database |
| `OpCodeBegin` | 0x10 | client → server | Start transaction; optional read-only flag |
| `OpCodeCommit` | 0x11 | client → server | Commit + group fsync |
| `OpCodeReplicaOf` | 0x32 | admin | Follow remote primary |
| `OpCodePromote` | 0x34 | admin | Become primary |
| `OpCodeStepDown` | 0x35 | admin | Drain and relinquish primary |
| `OpCodeReplHello` | 0x50 | replication | Handshake with start offset |
| `OpCodeReplLogRange` | 0x57 | replication | Raw WAL byte range |
| `OpCodeReplSafePoint` | 0x55 | replication | Cluster-wide retention hint |

Full list: `protocol.go` `OpCode*` constants.

## Response status codes

| Code | Name | Typical client error |
| --- | --- | --- |
| `0x00` | OK | — |
| `0x01` | ERR | `ServerError` |
| `0x02` | NOT_FOUND | `ErrNotFound` |
| `0x03` | TX_REQUIRED | `ErrTxRequired` |
| `0x04` | TX_TIMEOUT | `ErrTxTimeout` |
| `0x05` | TX_CONFLICT | `ErrTxConflict` |
| `0x06` | TX_IN_PROGRESS | `ErrTxInProgress` |
| `0x07` | SERVER_BUSY | `ErrServerBusy` |
| `0x08` | ENTITY_TOO_LARGE | `ErrEntityTooLarge` |
| `0x09` | MEMORY_LIMIT | `ErrMemoryLimit` |

## Limits and timeouts

| Constant | Value | Rationale |
| --- | --- | --- |
| `MaxTxDuration` | 30s | Reaper aborts stale transactions |
| `MaxTxSize` | 200 MiB | Bound per-transaction memory |
| `MaxValueSize` | 4 MiB | Single value cap |
| `MaxCommandSize` | 512 MiB | Must fit in `uint32` length field |
| `IdleTimeout` | 3 min | Server may close idle connections |
| `DefaultWriteTimeout` | 5s | Replication / write path guard |

Changing limits requires coordinated updates in server enforcement, client validation, and documentation.

## CRC and replication

Replication streams **physical WAL frames** (`OpCodeReplLogRange`), not logical records. Frame integrity uses CRC32 (Castagnoli) at the engine layer; `protocol` defines how byte ranges are addressed (global LSN, exclusive end offset).

## Educational focus

### Why a separate package?

Shared constants prevent drift between client and server. When adding an opcode, you touch `protocol` first, then server dispatch, then client API.

### Big-endian lengths

Network byte order keeps framing unambiguous for non-Go clients implementing the same protocol.

### Read-only transactions

`OpCodeBegin` accepts an optional one-byte payload: `BeginReadOnly` opens a snapshot read transaction without write locks — important for long-running analytics on replicas.

## Review checklist

- [ ] New opcodes documented in comment above constant.
- [ ] Opcode values do not collide; replication ops grouped in `0x5x` range.
- [ ] Status codes appended without renumbering existing values.
- [ ] `StatusName` updated for new statuses.
- [ ] Integration tests cover round-trip for new operations.

## Suggestions

- Publish a `protocol.md` code-generated from constants for non-Go implementers.
- Version field in handshake for future protocol evolution.
- Fuzz tests on `EncodeFrame` + server parser for malformed lengths.
