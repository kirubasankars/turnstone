# `.github/` — GitHub automation

CI and GitHub-specific configuration for the TurnstoneDB repository.

## Layout

| Path | Purpose |
| --- | --- |
| [`workflows/ci.yml`](workflows/ci.yml) | Continuous integration on push/PR to `main` |

## CI pipeline (`workflows/ci.yml`)

Runs on `ubuntu-latest` with Go **1.25** (see workflow; `go.mod` may specify newer).

| Step | Command | Intent |
| --- | --- | --- |
| Build | `go build ./...` | Compile all packages |
| Vet | `go vet ./...` | Static analysis |
| Gofmt | `gofmt -l .` | Enforce formatting (fail if any file differs) |
| Test | `go test -race -count=1 ./...` | Full suite with race detector (includes backup/restore integration tests in `server/`) |

### Why one `./...` race run?

Comments in the workflow and `Makefile` document a recurring lesson: **races between packages** (e.g. `server` ↔ `database` ↔ `engine`) only appear when all packages test together in a single race-enabled invocation. Per-package CI jobs can miss cross-package concurrency bugs.

Local equivalent:

```bash
make test-race
```

## Educational focus

TurnstoneDB relies heavily on goroutines (committer, retention, replication streams, per-connection handlers). The race detector is not optional polish — it is part of the correctness story.

Backup/restore integration tests (`server/backup_restore_integration_test.go`) exercise full and differential WAL backup chains end-to-end: write on a primary, backup via replication stream, restore offline, and verify keys on a new server node. They run as part of the standard `./...` CI test step.

## Review checklist

- [ ] New packages included automatically via `./...` (no CI whitelist to update).
- [ ] Go version in workflow stays compatible with `go.mod`.
- [ ] Breaking changes to test timeouts documented if CI becomes flaky.

## Suggestions

- Add `bench` job on schedule (non-blocking) to track engine regressions.
- `go test -short` fast path for PRs + nightly full race run if runtime grows.
- Codecov or cover profile upload for `engine` package.
