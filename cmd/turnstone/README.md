# `cmd/turnstone/` — TurnstoneDB binary

The `turnstone` executable is the only shipped program. It uses [Cobra](https://github.com/spf13/cobra) for subcommands and a persistent `--home` flag (default `tsdata`).

## Subcommands

| Command | File(s) | Role |
| --- | --- | --- |
| `turnstone init` | `initcmd.go` | Create home dir, TLS CA + server/client/admin certs, database directories, `turnstone.json` |
| `turnstone server` | `server.go` | Load config, open databases, start mTLS listener, Prometheus metrics, replication manager |
| `turnstone cli` | `cli.go`, `clicommand.go`, `cliconn.go` | Interactive REPL or `cli exec <one-liner>` against a running server |
| `turnstone bench` | `bench.go` | Concurrent load generator for throughput experiments |
| `turnstone backup` | `backup.go` | Stream physical WAL backup from a primary (uses `internal/backup`) |
| `turnstone restore` | `restore.go` | Offline restore of WAL backup chains (uses `internal/backup`) |

## File reference

| File | Responsibility |
| --- | --- |
| `main.go` | Entry point; executes root command, exits 1 on error |
| `root.go` | Root Cobra command, `--home` flag, subcommand registration |
| `initcmd.go` | `init` — calls `config.GenerateConfigArtifacts` |
| `server.go` | `server` — config load, TLS, `database.Open` per DB, `server.New`, metrics HTTP, signal handling |
| `cli.go` | `cli` REPL loop, history-friendly UX |
| `clicommand.go` | Parses REPL tokens into protocol operations |
| `cliconn.go` | mTLS dial + framed read/write for CLI |
| `bench.go` | Benchmark driver using `client` package |
| `backup.go` | `backup` — thin wrapper around `internal/backup` |
| `restore.go` | `restore` — thin wrapper around `internal/backup` |

## Server startup flow (`server.go`)

Understanding this sequence is essential for debugging “server won’t start” issues:

1. Read `turnstone.json` from `--home`.
2. Validate with `config.ValidateConfig`.
3. Load server TLS keypair and CA; build `tls.Config` with client-auth required.
4. For each logical database `0 … N-1`, call `database.Open` on `<home>/<id>`.
5. Construct `repl.Manager` for outbound `replicaof` connections.
6. Construct `server.Server` with stores map, connection limits, dev mode.
7. Start Prometheus scrape endpoint (`metrics_addr`, default `:9090`).
8. Block in `Server.Run()` until SIGINT/SIGTERM.

### Dev mode (`--dev`)

When enabled:

- Transaction timeouts are disabled at the engine layer.
- All databases are auto-promoted to `PRIMARY` on startup (no manual `promote`).

Use only for local development; production failover semantics are bypassed.

## CLI connection model

The CLI loads **client** or **admin** certificates from `<home>/certs/` via `internal/tlsutil`:

- Default role: `client` (Organization must match server RBAC expectations).
- `--admin`: loads `admin.crt` / `admin.key` for `promote`, `stepdown`, `replicaof`, `flushdb`.

Every mutating read (`get` outside a tx is allowed on some paths; `set`/`del`/`mset` require `begin` … `commit`) follows the transactional model documented in the root README.

## Educational notes

### Why Cobra?

Subcommands map cleanly to operator workflows (`init` once, `server` always-on, `cli` ad hoc). Persistent `--home` avoids repeating the data directory on every invocation.

### Separation from `server` package

`cmd/turnstone` should not import `engine` directly except transitively. If you find engine types in `cmd/`, that is a layering smell — push logic down into `database` or `server`.

### Benchmark subcommand

`bench` exists to exercise the **client → server → engine** path under concurrency without writing a separate tool. Review `bench.go` when investigating latency outliers or connection pool behavior.

## Review checklist

- [ ] New flags are bound on the correct command (persistent vs local).
- [ ] `init` refuses to clobber an existing home without explicit UX (if policy changes).
- [ ] `server` shuts down gracefully: closes listener, waits for handlers, closes databases.
- [ ] CLI errors surface server status codes (`TX_CONFLICT`, `TX_REQUIRED`, etc.) readably.
- [ ] `bench` defaults are safe for shared dev clusters (document destructive ops if any).

## Suggested experiments

1. Run `turnstone init --home /tmp/ts-test --ip 127.0.0.1` and inspect generated `certs/` and `turnstone.json`.
2. Start `turnstone server --home /tmp/ts-test --dev` and trace logs while connecting with `turnstone cli --home /tmp/ts-test`.
3. Use `turnstone bench` with varying `--concurrency` and correlate with `turnstone_server_connections_active` metrics.
