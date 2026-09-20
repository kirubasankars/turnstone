# TurnstoneDB (Rust)

Rust port of [TurnstoneDB](../README.md). The Go tree remains in-repo for comparison and for tests not yet ported to Rust.

## Status (approx.)

| Area | Rust crate | Notes |
| --- | --- | --- |
| Wire protocol | `turnstone-protocol` | Opcode/framing parity |
| mTLS | `turnstone-tls` | Optional: plain TCP when cert files absent; mTLS when present |
| Hash index | `turnstone-hashindex` | Sharded `HashMap` + MVCC version chains, compact, budget |
| Engine | `turnstone-engine` | WAL, MVCC `Db`, group commit, shared buffer pool, index GC, WAL copy-forward/retention |
| Database layer | `turnstone-database` | Roles, slots, quorum, retention hooks |
| Replication | `turnstone-repl` | Outbound follower sync |
| Server | `turnstone-server` | mTLS accept, KV + tx + partial admin/repl |
| Backup | `turnstone-backup` | Meta/stream/restore (restore apply still partial) |
| Config / metrics | `turnstone-config`, `turnstone-metrics` | Init artifacts, Prometheus collector |
| Client | `turnstone-client`, `turnstone-cli` | Wire client + `turnstone-rs` smoke |
| CLI binary | `turnstone` | `init`, `server --dev`, `cli ping` |
| Console UI | — | Still Go-only (`console/`) |
| Full CLI | `turnstone`, `turnstone-rs` | `init`, `server`, `cli` (REPL + exec), `bench`, `backup`, `restore` |

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
./rust/target/release/turnstone server --home tsdata --dev   # plain TCP if server certs missing; --dev grants admin on plain links
# other terminal:
./rust/target/release/turnstone cli --home tsdata exec get mykey
./rust/target/release/turnstone cli --home tsdata   # interactive REPL
./rust/target/release/turnstone-rs --home tsdata ping
```

## Tests

```bash
cargo test --manifest-path rust/Cargo.toml   # Rust unit/integration tests
go test -race -count=1 ./...               # Full Go reference suite (unchanged)
```

From repo root, `make test-rust` runs the Rust suite. Coverage (requires `llvm-tools` / `cargo-llvm-cov`, installed by the script if missing):

```bash
make test-rust-coverage
# writes rust/target/llvm-cov/lcov.info and prints a line summary
```

Recent additions include Go-ported **index GC keep-mask** regression tests (`turnstone-engine` `index_gc` module), **active transaction readers** in `build_index_gc_context`, value-cache and shared-buffer integration tests, and heap-only hash-index smoke checks.

## Benchmarks

Microbenchmarks (Criterion, aligned with Go `engine/benchmark_test.go` and `hashindex/benchmark_test.go`):

```bash
make bench-rust
# or:
cd rust && cargo bench -p turnstone-engine -p turnstone-hashindex
```

Lightweight **benchmark smoke tests** run with the normal test suite (`tests/benchmark_test.rs` in `turnstone-engine` and `turnstone-hashindex`).
