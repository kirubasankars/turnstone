# `metrics/` — Prometheus observability

The `metrics` package exposes TurnstoneDB statistics via **Prometheus** on a separate HTTP listener (default `:9090`, configured by `metrics_addr` in `turnstone.json`).

## Components

| Piece | Role |
| --- | --- |
| `TurnstoneCollector` | Custom collector implementing `prometheus.Collector` |
| `ServerStatsProvider` | Interface for server-wide connection / tx counts |
| `StartMetricsServer` | Registers collectors and serves `/metrics` |

## Metric namespaces

All metrics use FQ name prefix `turnstone_`.

### Server-wide

| Metric | Type | Meaning |
| --- | --- | --- |
| `turnstone_server_connections_active` | gauge | Current open connections |
| `turnstone_server_connections_accepted_total` | counter | Lifetime accepts |
| `turnstone_server_transactions_active` | gauge | Sum of active txs across DBs |

### Per database (`db` label)

| Metric | Meaning |
| --- | --- |
| `turnstone_db_connections` | Connections with `SELECT` on this DB |
| `turnstone_db_active_txs` | Open transactions |
| `turnstone_db_conflicts_total` | Cumulative `TX_CONFLICT` |
| `turnstone_db_offset` | WAL exclusive end offset |
| `turnstone_db_replica_lag` | Bytes behind slowest replica |
| `turnstone_db_log_bytes` | Logical WAL size |
| `turnstone_db_log_allocated_bytes` | On-disk segment allocation |
| `turnstone_db_key_count` | Approximate live keys |

## Data flow

```
database.Stats() ──► TurnstoneCollector.Collect()
server (implements ServerStatsProvider) ──► connection gauges
```

Collection runs on each Prometheus scrape — keep `Stats()` O(1) or cheap; avoid heavy walks per scrape.

## Startup wiring (`cmd/turnstone/server.go`)

1. Build `stores map[string]*database.Database`.
2. `metrics.NewTurnstoneCollector(stores, srv)`.
3. `metrics.StartMetricsServer(addr, logger, collector)`.
4. Runs concurrently with main server — separate HTTP server, no mTLS on metrics port by default.

## Educational focus

### Custom collector vs individual counters

A single collector reads live state from databases on scrape. This avoids synchronizing duplicate counter state but means metrics are **point-in-time snapshots**, not hook-per-event.

### Security note

Metrics endpoint is typically **unauthenticated HTTP**. Bind to localhost or protect with network policy in production.

### Correlating with replication

Compare `turnstone_db_offset` on primary vs follower (via separate scrape targets) and `turnstone_db_replica_lag` on the leader.

## Review checklist

- [ ] New `database.Stats` fields get corresponding Prometheus descriptors.
- [ ] Metric names follow Prometheus naming conventions (snake_case, `_total` suffix for counters).
- [ ] `Describe` and `Collect` register the same descriptors.
- [ ] Tests in `metrics_test.go` parse output without flake.

## Suggestions

- TLS or bearer auth option for metrics listener.
- Histograms for commit latency and WAL fsync duration.
- Exemplars for trace correlation if OpenTelemetry is added.
