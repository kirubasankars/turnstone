#!/usr/bin/env bash
# Copyright (c) 2026 Kiruba Sankar Swaminathan
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root of this source tree.
#
# Seed a few keys, overwrite them, then abort many updates so the hash-index
# bump arena fragments. Index GC runs on the 60s retention ticker when a
# shard's bump-used bytes exceed 3× live payload.
#
# Watch the Console Metrics tab: Hash compacted, Hash reclaimed, Last compact.
#
# Usage:
#   scripts/frag-index-gc.sh
#   scripts/frag-index-gc.sh --home tsdata --db 0
#   TS_HOME=/tmp/ts-metrics-group scripts/frag-index-gc.sh

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${TURNSTONE_BIN:-"$ROOT/bin/turnstone"}
TS_HOME=${TS_HOME:-"$ROOT/tsdata"}
DB=0
KEYS=8
ABORTS=24
WAIT_SECS=90
HOST=""
METRICS_URL=""
PREFIX=gc-frag

usage() {
  echo "Usage: $0 [--home DIR] [--db N] [--keys N] [--aborts N] [--wait SECS] [--host HOST] [--metrics URL]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --home) TS_HOME=$2; shift 2 ;;
    --db) DB=$2; shift 2 ;;
    --keys) KEYS=$2; shift 2 ;;
    --aborts) ABORTS=$2; shift 2 ;;
    --wait) WAIT_SECS=$2; shift 2 ;;
    --host) HOST=$2; shift 2 ;;
    --metrics) METRICS_URL=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -x "$BIN" ]]; then
  echo "missing $BIN — run: make build" >&2
  exit 1
fi
if [[ ! -f "$TS_HOME/turnstone.json" ]]; then
  echo "no turnstone.json in $TS_HOME" >&2
  exit 1
fi

eval "$(python3 - "$TS_HOME" <<'PY'
import json, sys, os
cfg = json.load(open(os.path.join(sys.argv[1], "turnstone.json")))
port = str(cfg.get("port") or ":6379").lstrip(":")
metrics = str(cfg.get("metrics_addr") or ":9090").lstrip(":")
print("CFG_HOST=%s" % json.dumps("127.0.0.1:" + port))
print("CFG_METRICS=%s" % json.dumps("http://127.0.0.1:" + metrics))
PY
)"
HOST=${HOST:-$CFG_HOST}
METRICS_URL=${METRICS_URL:-$CFG_METRICS}

metric() {
  local name=$1
  curl -sf "$METRICS_URL/metrics" | awk -v n="$name" -v db="$DB" '
    index($1, n "{") == 1 && $0 ~ "db=\"" db "\"" { print $NF; found=1 }
    END { if (!found) print "na" }
  '
}

print_metrics() {
  local label=$1
  printf '%s  compacted=%s  reclaimed=%s  arena=%s  live=%s\n' \
    "$label" \
    "$(metric turnstone_db_hash_shards_compacted_total)" \
    "$(metric turnstone_db_index_compact_bytes_reclaimed_total)" \
    "$(metric turnstone_db_index_arena_bytes)" \
    "$(metric turnstone_db_index_live_bytes)"
}

if ! curl -sf "$METRICS_URL/metrics" >/dev/null; then
  echo "cannot scrape $METRICS_URL/metrics — is the server running?" >&2
  exit 1
fi

VAL=$(python3 -c 'print("x" * 1024)')
CMDS=$(mktemp)
trap 'rm -f "$CMDS"' EXIT

{
  echo "select $DB"
  i=0
  while [[ $i -lt $KEYS ]]; do
    echo "begin"
    echo "set ${PREFIX}-$i seed"
    echo "commit"
    i=$((i + 1))
  done
  i=0
  while [[ $i -lt $KEYS ]]; do
    echo "begin"
    echo "set ${PREFIX}-$i updated"
    echo "commit"
    i=$((i + 1))
  done
  r=1
  while [[ $r -le $ABORTS ]]; do
    i=0
    while [[ $i -lt $KEYS ]]; do
      echo "begin"
      echo "set ${PREFIX}-$i $VAL"
      echo "abort"
      i=$((i + 1))
    done
    r=$((r + 1))
  done
  echo quit
} >"$CMDS"

echo "home=$TS_HOME host=$HOST db=$DB keys=$KEYS aborts/key=$ABORTS"
print_metrics "before "
echo "writing commits, then aborting $ABORTS overwrites per key (this fragments bump arena)..."

if ! "$BIN" --home "$TS_HOME" cli --host "$HOST" <"$CMDS" >/dev/null; then
  echo "cli failed — check that the server is PRIMARY (or started with --dev)" >&2
  exit 1
fi

print_metrics "loaded "
echo "waiting up to ${WAIT_SECS}s for retention GC (default interval 60s)..."
echo "watch Console → Metrics → Hash index: compacted / reclaimed / last compact"

before=$(metric turnstone_db_hash_shards_compacted_total)
elapsed=0
while [[ $elapsed -lt $WAIT_SECS ]]; do
  now=$(metric turnstone_db_hash_shards_compacted_total)
  if [[ "$now" != "na" && "$before" != "na" && "$now" != "$before" ]]; then
    print_metrics "gc     "
    echo "index GC ran (hash_shards_compacted_total $before -> $now)"
    exit 0
  fi
  sleep 2
  elapsed=$((elapsed + 2))
done

print_metrics "timeout"
echo "no compaction yet — bump-used must exceed 3× live on a shard; retry with --aborts 48" >&2
exit 1
