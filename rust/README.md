# TurnstoneDB (Rust)

Work-in-progress Rust port of [TurnstoneDB](../README.md). The Go implementation remains the reference server and full engine; this tree adds wire-compatible building blocks and a native client.

## Crates

| Crate | Status |
| --- | --- |
| `turnstone-protocol` | Wire opcodes, limits, framing, payload helpers |
| `turnstone-engine` | WAL record encode/decode and shared constants |
| `turnstone-tls` | mTLS loading from a Turnstone home directory |
| `turnstone-client` | Blocking TCP/mTLS client (Go `client` package parity) |
| `turnstone-cli` | `turnstone-rs` binary for ping and smoke tests |

## Not yet ported

Server, replication, backup/restore, hash index, WAL mmap I/O, console, metrics, and the full CLI surface still live under the Go tree. Engine behavior should match Go tests before cutover.

## Build

```bash
cd rust
cargo build --release
# binary: target/release/turnstone-rs
```

From the repo root:

```bash
make build-rust
make test-rust
```

## Quick check against a Go server

```bash
./bin/turnstone init --home tsdata --ip 127.0.0.1
./bin/turnstone server --home tsdata --dev &
./rust/target/release/turnstone-rs --home tsdata ping
./rust/target/release/turnstone-rs --home tsdata smoke mykey hello
```
