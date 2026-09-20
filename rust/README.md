# TurnstoneDB (Rust)

Rust port of [TurnstoneDB](../README.md). The Go tree remains in-repo for comparison and for tests not yet ported to Rust.

## Status (approx.)

| Area | Rust crate | Notes |
| --- | --- | --- |
| Wire protocol | `turnstone-protocol` | Opcode/framing parity |
| mTLS | `turnstone-tls` | Client + server cert loading |
| Hash index | `turnstone-hashindex` | Mmap/heaps, compact, budget |
| Engine | `turnstone-engine` | WAL, MVCC `Db`, transactions; no group-commit batching yet |
| Database layer | `turnstone-database` | Roles, slots, quorum, retention hooks |
| Replication | `turnstone-repl` | Outbound follower sync |
| Server | `turnstone-server` | mTLS accept, KV + tx + partial admin/repl |
| Backup | `turnstone-backup` | Meta/stream/restore (restore apply still partial) |
| Config / metrics | `turnstone-config`, `turnstone-metrics` | Init artifacts, Prometheus collector |
| Client | `turnstone-client`, `turnstone-cli` | Wire client + `turnstone-rs` smoke |
| CLI binary | `turnstone` | `init`, `server --dev`, `cli ping` |
| Console UI | — | Still Go-only (`console/`) |
| Full CLI | — | Go: `bench`, interactive REPL, backup/restore commands |

Rough size: ~12k lines of Rust vs ~32k lines of Go (prod + tests). **Rust `cargo test` covers a subset** of Go’s `-race ./...` suite (chaos, hardening, e2e console, most server integration tests are not ported).

## Build

```bash
cd rust
cargo build --release
# binaries: target/release/turnstone, target/release/turnstone-rs
```

From repo root:

```bash
make build-rust
make test-rust
```

## Quick start (Rust server)

```bash
./rust/target/release/turnstone init --home tsdata --ip 127.0.0.1
./rust/target/release/turnstone server --home tsdata --dev
# other terminal:
./rust/target/release/turnstone-rs --home tsdata ping
./rust/target/release/turnstone-rs --home tsdata smoke mykey hello
```

## Tests

```bash
cargo test --manifest-path rust/Cargo.toml   # Rust unit/integration tests
go test -race -count=1 ./...               # Full Go reference suite (unchanged)
```
