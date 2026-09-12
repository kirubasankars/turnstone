# `internal/` — Private shared utilities

Go's `internal/` directory convention: packages here may only be imported by code under the TurnstoneDB module root (`turnstone/...`). External consumers of TurnstoneDB cannot depend on these APIs — they are free to change without semver guarantees.

## Current packages

| Package | Path | Purpose |
| --- | --- | --- |
| `tlsutil` | [`tlsutil/`](tlsutil/) | Load mTLS client configs from a Turnstone home directory |

## Why `internal` exists

Cross-cutting helpers used by both `cmd/` and `client/` (and potentially tests) would otherwise create awkward dependency cycles or tempt third parties to import unstable paths. Placing them under `internal/` documents intent: **not public API**.

## When to add a new internal package

Add here when:

- Multiple top-level packages need the same helper.
- The helper is not part of TurnstoneDB's product surface.
- You want freedom to refactor without breaking importers.

Keep storage, protocol, and replication logic in their domain packages — do not grow `internal` into a junk drawer.

## Educational focus

The Go compiler enforces the import restriction: code outside `turnstone` cannot import `turnstone/internal/...`. This is stronger than documentation alone.

## Review checklist

- [ ] New package is truly shared — not used by only one caller (could stay local).
- [ ] No import of `engine` from `internal` unless absolutely necessary.
- [ ] README added under new subpackage.

## Suggestions

- `internal/testutil` for shared test cert generation (if duplication grows).
- `internal/buildinfo` for version injection from `cmd`.
