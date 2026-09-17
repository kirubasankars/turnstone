#!/usr/bin/env bash
# Copyright (c) 2026 Kiruba Sankar Swaminathan
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root of this source tree.

# Compare TurnstoneDB engine KV throughput with PostgreSQL on a TEXT PRIMARY KEY
# table. Engine benches always run. The Postgres side runs only when psql can
# connect (PGHOST, PGDATABASE, PGUSER, …).
#
# Usage:
#   scripts/bench-vs-postgres.sh
#   PGHOST=127.0.0.1 PGDATABASE=postgres scripts/bench-vs-postgres.sh
#
# See docs/performance.md for what is and is not a fair comparison.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

OPS=${OPS:-5000}
BENCHTIME=${BENCHTIME:-2s}

echo "=== TurnstoneDB engine (in-process, durable group fdatasync) ==="
go test -run='^$' -bench='BenchmarkDB_(Insert|Read|Update)Parallel' \
  -benchtime="$BENCHTIME" -benchmem ./engine/

if ! command -v psql >/dev/null 2>&1; then
  echo
  echo "psql not on PATH — skipped PostgreSQL side."
  echo "Start Postgres and re-run, or see docs/performance.md."
  exit 0
fi

if ! psql -v ON_ERROR_STOP=1 -c 'SELECT 1' >/dev/null 2>&1; then
  echo
  echo "psql cannot connect — skipped PostgreSQL side."
  echo "Export PGHOST/PGDATABASE/PGUSER (and PGPASSWORD if needed) and re-run."
  exit 0
fi

echo
echo "=== PostgreSQL KV (TEXT PRIMARY KEY, BYTEA, synchronous_commit=on) ==="
echo "Ops per phase: $OPS"

psql -v ON_ERROR_STOP=1 <<SQL
SET synchronous_commit = on;
DROP TABLE IF EXISTS turnstone_kv_bench;
CREATE TABLE turnstone_kv_bench (
  k TEXT PRIMARY KEY,
  v BYTEA NOT NULL
);
SQL

python3 - "$OPS" <<'PY'
import os, sys, subprocess, time

ops = int(sys.argv[1])
payload = "x" * 32

def psql(sql: str) -> None:
    subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
        check=True,
        stdout=subprocess.DEVNULL,
    )

def timed(label, sql_factory):
    start = time.perf_counter()
    for i in range(ops):
        psql(sql_factory(i))
    elapsed = time.perf_counter() - start
    tps = ops / elapsed if elapsed > 0 else 0
    print(f"{label:18} {ops} ops in {elapsed:.3f}s  {tps:.1f} TPS")

timed(
    "PG insert (1 tx)",
    lambda i: (
        "INSERT INTO turnstone_kv_bench (k, v) VALUES "
        f"('ins-{i}', decode('{payload.encode().hex()}', 'hex'));"
    ),
)
timed(
    "PG update (1 tx)",
    lambda i: (
        "UPDATE turnstone_kv_bench SET v = decode("
        f"'{payload.encode().hex()}', 'hex') WHERE k = 'ins-{i % ops}';"
    ),
)
timed(
    "PG select (1 tx)",
    lambda i: f"SELECT v FROM turnstone_kv_bench WHERE k = 'ins-{i % ops}';",
)
PY

psql -v ON_ERROR_STOP=1 -c 'DROP TABLE IF EXISTS turnstone_kv_bench;' >/dev/null
echo
echo "Note: the psql loop pays process/SQL overhead per op. Use a driver"
echo "(lib/pq, pgx) for a tighter client, or compare make bench ns/op."
echo "Details: docs/performance.md"
