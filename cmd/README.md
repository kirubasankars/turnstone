# `cmd/` — Command-line entry points

This directory holds **executable programs** for TurnstoneDB. In Go projects, `cmd/` is the conventional home for `main` packages: each subdirectory builds to a separate binary.

TurnstoneDB ships a single binary, `turnstone`, defined under `cmd/turnstone/`.

## Layout

| Path | Purpose |
| --- | --- |
| [`turnstone/`](turnstone/) | Main CLI: `init`, `server`, `cli`, `bench` |

## How it fits in the stack

```
User / operator
      │
      ▼
  cmd/turnstone  ──►  server / client / config / metrics / repl
      │                      │
      │                      ▼
      │               database ──► engine
      ▼
  turnstone.json + certs + data dirs on disk
```

The `cmd` layer is intentionally thin: it parses flags, loads configuration from the home directory, wires dependencies, and delegates all storage semantics to lower packages.

## Build

```bash
make build          # produces bin/turnstone
go build -o bin/turnstone ./cmd/turnstone
```

## Educational focus

When reading TurnstoneDB for the first time, start here to see **how operators interact** with the system. The CLI is the contract between humans and the server. Notice:

- **`--home`** is the global data root (certs, config, per-database directories).
- **`init`** bootstraps PKI and `turnstone.json`; nothing in the engine runs until this exists.
- **`server`** is the long-running process; **`cli`** is a short-lived client.
- **`--dev`** on the server relaxes production guardrails (timeouts, auto-promote) for local development.

## Code review checklist

- [ ] New user-facing commands are registered in `turnstone/root.go` and documented in the root README.
- [ ] Flag defaults match `config.Config` defaults where applicable.
- [ ] Exit codes are consistent (`os.Exit(1)` on failure from `main`).
- [ ] No storage or protocol logic leaks into `cmd/` — keep orchestration only.
- [ ] TLS certificate paths resolve through `config.ResolvePath(home, ...)`.

## Suggested reading order

1. [`turnstone/README.md`](turnstone/README.md) — per-file breakdown of subcommands.
2. [`../config/README.md`](../config/README.md) — what `init` and `server` load from disk.
3. [`../server/README.md`](../server/README.md) — what `server` actually starts.

## Suggestions for extension

- Add a `turnstone doctor` subcommand that validates certs, config, disk space, and database openability without starting the listener.
- Consider `turnstone version` embedding build metadata (`-ldflags "-X main.version=..."`).
- Document environment-variable overrides (e.g. `TURNSTONE_HOME`) if added in the future.
