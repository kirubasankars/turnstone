// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::sync::Arc;

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};
const NAMESPACE: &str = "turnstone";

/// Database statistics returned by [`DatabaseMetrics::stats`].
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub active_txs: i64,
    pub offset: i64,
    pub conflicts: u64,
    pub replica_lag: u64,
    pub server_replicas: i64,
    pub log_size: i64,
    pub log_allocated: i64,
    pub key_count: i64,
    pub hash_shards_compacted: u64,
    pub hash_compact_bytes_reclaimed: u64,
    pub hash_compact_unix: i64,
}

/// Hash-index metrics (matches Go `engine.IndexHashMetrics`).
#[derive(Debug, Clone, Default)]
pub struct IndexHashMetrics {
    pub shards_used: i64,
    pub arena_bytes: i64,
    pub allocated_bytes: i64,
    pub live_bytes: i64,
}

/// Server-wide connection and transaction counters (matches Go `ServerStatsProvider`).
pub trait ServerStatsProvider: Send + Sync {
    fn active_conns(&self) -> i64;
    fn total_conns(&self) -> u64;
    fn active_txs(&self) -> i64;
    fn database_conns(&self, db_name: &str) -> i64;
}

/// Per-database metrics source (matches methods used from `database.Database` in Go).
pub trait DatabaseMetrics: Send + Sync {
    fn stats(&self) -> DbStats;
    fn wal_segment_count(&self) -> i64;
    fn index_hash_metrics(&self) -> IndexHashMetrics;
}

fn fq_name(sub: &str, name: &str) -> String {
    format!("{NAMESPACE}_{sub}_{name}")
}

fn new_desc(sub: &str, name: &str, help: &str) -> Desc {
    Desc::new(
        fq_name(sub, name),
        help.to_string(),
        vec![],
        std::collections::HashMap::new(),
    )
    .unwrap()
}

fn new_desc_with_labels(sub: &str, name: &str, help: &str, labels: &[&str]) -> Desc {
    Desc::new(
        fq_name(sub, name),
        help.to_string(),
        labels.iter().map(|l| (*l).to_string()).collect(),
        std::collections::HashMap::new(),
    )
    .unwrap()
}

/// Prometheus collector for Turnstone server and database metrics.
pub struct TurnstoneCollector {
    stores: HashMap<String, Arc<dyn DatabaseMetrics>>,
    server_stats: Option<Arc<dyn ServerStatsProvider>>,

    active_conns: Desc,
    total_conns: Desc,
    total_active_txs: Desc,

    db_connections: Desc,
    db_active_txs: Desc,
    db_conflicts: Desc,
    db_offset: Desc,
    db_replica_lag: Desc,
    db_replicas: Desc,
    db_log_bytes: Desc,
    db_log_allocated: Desc,
    db_key_count: Desc,
    db_wal_segments: Desc,
    db_hash_shards: Desc,
    db_index_arena_bytes: Desc,
    db_index_allocated_bytes: Desc,
    db_index_live_bytes: Desc,
    db_hash_compacted: Desc,
    db_hash_compact_reclaimed: Desc,
    db_hash_compact_unix: Desc,
}

impl TurnstoneCollector {
    pub fn new(
        stores: HashMap<String, Arc<dyn DatabaseMetrics>>,
        server_stats: Option<Arc<dyn ServerStatsProvider>>,
    ) -> Self {
        Self {
            stores,
            server_stats,
            active_conns: new_desc("server", "connections_active", "Active connections"),
            total_conns: new_desc(
                "server",
                "connections_accepted_total",
                "Connections that started a handler",
            ),
            total_active_txs: new_desc(
                "server",
                "transactions_active",
                "Total active transactions across server",
            ),
            db_connections: new_desc_with_labels(
                "db",
                "connections",
                "Active connections currently selected on this database",
                &["db"],
            ),
            db_active_txs: new_desc_with_labels(
                "db",
                "active_txs",
                "Active transactions in database",
                &["db"],
            ),
            db_conflicts: new_desc_with_labels(
                "db",
                "conflicts_total",
                "Total transaction conflicts in database",
                &["db"],
            ),
            db_offset: new_desc_with_labels(
                "db",
                "offset",
                "Exclusive end byte offset of the WAL",
                &["db"],
            ),
            db_replica_lag: new_desc_with_labels(
                "db",
                "replica_lag",
                "Lag of the slowest server-role replica in bytes; 0 if none",
                &["db"],
            ),
            db_replicas: new_desc_with_labels(
                "db",
                "replicas",
                "Server-role replication slots (connected or not)",
                &["db"],
            ),
            db_log_bytes: new_desc_with_labels(
                "db",
                "log_bytes",
                "Retained WAL LSN span in bytes (write head minus oldest segment base)",
                &["db"],
            ),
            db_log_allocated: new_desc_with_labels(
                "db",
                "log_allocated_bytes",
                "Allocated on-disk bytes for WAL segments",
                &["db"],
            ),
            db_key_count: new_desc_with_labels(
                "db",
                "key_count",
                "Approximate number of live keys in database",
                &["db"],
            ),
            db_wal_segments: new_desc_with_labels(
                "db",
                "wal_segments",
                "Number of WAL segment files",
                &["db"],
            ),
            db_hash_shards: new_desc_with_labels(
                "db",
                "hash_shards",
                "Hash-index shards that currently hold keys",
                &["db"],
            ),
            db_index_arena_bytes: new_desc_with_labels(
                "db",
                "index_arena_bytes",
                "Bump-allocated hash-index key and version bytes",
                &["db"],
            ),
            db_index_allocated_bytes: new_desc_with_labels(
                "db",
                "index_allocated_bytes",
                "Allocated hash-index shard buffer bytes (header, slot table, and capacity)",
                &["db"],
            ),
            db_index_live_bytes: new_desc_with_labels(
                "db",
                "index_live_bytes",
                "Estimated live hash-index key and version payload bytes",
                &["db"],
            ),
            db_hash_compacted: new_desc_with_labels(
                "db",
                "hash_shards_compacted_total",
                "Hash-index shards compacted by index GC",
                &["db"],
            ),
            db_hash_compact_reclaimed: new_desc_with_labels(
                "db",
                "index_compact_bytes_reclaimed_total",
                "Bump-arena bytes reclaimed by hash-index compaction",
                &["db"],
            ),
            db_hash_compact_unix: new_desc_with_labels(
                "db",
                "hash_compact_last_timestamp_seconds",
                "Unix time of last hash-index compaction; 0 if never",
                &["db"],
            ),
        }
    }
}

impl Collector for TurnstoneCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![
            &self.active_conns,
            &self.total_conns,
            &self.total_active_txs,
            &self.db_connections,
            &self.db_active_txs,
            &self.db_conflicts,
            &self.db_offset,
            &self.db_replica_lag,
            &self.db_replicas,
            &self.db_log_bytes,
            &self.db_log_allocated,
            &self.db_key_count,
            &self.db_wal_segments,
            &self.db_hash_shards,
            &self.db_index_arena_bytes,
            &self.db_index_allocated_bytes,
            &self.db_index_live_bytes,
            &self.db_hash_compacted,
            &self.db_hash_compact_reclaimed,
            &self.db_hash_compact_unix,
        ]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = Vec::new();

        if let Some(stats) = &self.server_stats {
            families.push(gauge_family(
                &self.active_conns,
                stats.active_conns() as f64,
                &[],
            ));
            families.push(counter_family(
                &self.total_conns,
                stats.total_conns() as f64,
                &[],
            ));
            families.push(gauge_family(
                &self.total_active_txs,
                stats.active_txs() as f64,
                &[],
            ));
        }

        for (name, db) in &self.stores {
            let stats = db.stats();
            let hash = db.index_hash_metrics();

            let db_conns = self
                .server_stats
                .as_ref()
                .map(|s| s.database_conns(name))
                .unwrap_or(0);

            let labels = [("db", name.as_str())];
            families.push(gauge_family(&self.db_connections, db_conns as f64, &labels));
            families.push(gauge_family(
                &self.db_active_txs,
                stats.active_txs as f64,
                &labels,
            ));
            families.push(counter_family(
                &self.db_conflicts,
                stats.conflicts as f64,
                &labels,
            ));
            families.push(gauge_family(&self.db_offset, stats.offset as f64, &labels));
            families.push(gauge_family(
                &self.db_replica_lag,
                stats.replica_lag as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_replicas,
                stats.server_replicas as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_log_bytes,
                stats.log_size as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_log_allocated,
                stats.log_allocated as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_key_count,
                stats.key_count as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_wal_segments,
                db.wal_segment_count() as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_hash_shards,
                hash.shards_used as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_index_arena_bytes,
                hash.arena_bytes as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_index_allocated_bytes,
                hash.allocated_bytes as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_index_live_bytes,
                hash.live_bytes as f64,
                &labels,
            ));
            families.push(counter_family(
                &self.db_hash_compacted,
                stats.hash_shards_compacted as f64,
                &labels,
            ));
            families.push(counter_family(
                &self.db_hash_compact_reclaimed,
                stats.hash_compact_bytes_reclaimed as f64,
                &labels,
            ));
            families.push(gauge_family(
                &self.db_hash_compact_unix,
                stats.hash_compact_unix as f64,
                &labels,
            ));
        }

        families
    }
}

fn gauge_family(desc: &Desc, value: f64, labels: &[(&str, &str)]) -> MetricFamily {
    let mut mf = MetricFamily::default();
    mf.set_name(desc.fq_name.clone());
    mf.set_help(desc.help.clone());
    mf.set_field_type(MetricType::GAUGE);
    mf.mut_metric()
        .push(metric_with_labels(value, labels, |m, v| {
            let mut g = Gauge::default();
            g.set_value(v);
            m.set_gauge(g);
        }));
    mf
}

fn counter_family(desc: &Desc, value: f64, labels: &[(&str, &str)]) -> MetricFamily {
    let mut mf = MetricFamily::default();
    mf.set_name(desc.fq_name.clone());
    mf.set_help(desc.help.clone());
    mf.set_field_type(MetricType::COUNTER);
    mf.mut_metric()
        .push(metric_with_labels(value, labels, |m, v| {
            let mut c = Counter::default();
            c.set_value(v);
            m.set_counter(c);
        }));
    mf
}

fn metric_with_labels(
    value: f64,
    labels: &[(&str, &str)],
    set_value: impl FnOnce(&mut Metric, f64),
) -> Metric {
    let mut m = Metric::default();
    for (k, v) in labels {
        let mut lp = LabelPair::default();
        lp.set_name(k.to_string());
        lp.set_value(v.to_string());
        m.mut_label().push(lp);
    }
    set_value(&mut m, value);
    m
}

/// Binds `addr` to `127.0.0.1` when it is port-only (`:9090`), matching Go `StartMetricsServer`.
pub fn normalize_metrics_addr(addr: &str) -> Option<String> {
    if addr.is_empty() {
        return None;
    }
    if addr.starts_with(':') {
        Some(format!("127.0.0.1{addr}"))
    } else {
        Some(addr.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    struct MockServerStats {
        active_conns: i64,
        total_conns: u64,
        active_txs: i64,
        db_conns: HashMap<String, i64>,
    }

    impl ServerStatsProvider for MockServerStats {
        fn active_conns(&self) -> i64 {
            self.active_conns
        }
        fn total_conns(&self) -> u64 {
            self.total_conns
        }
        fn active_txs(&self) -> i64 {
            self.active_txs
        }
        fn database_conns(&self, db_name: &str) -> i64 {
            self.db_conns.get(db_name).copied().unwrap_or(0)
        }
    }

    struct MockDatabase {
        stats: DbStats,
        wal_segments: i64,
        hash: IndexHashMetrics,
    }

    impl DatabaseMetrics for MockDatabase {
        fn stats(&self) -> DbStats {
            self.stats.clone()
        }
        fn wal_segment_count(&self) -> i64 {
            self.wal_segments
        }
        fn index_hash_metrics(&self) -> IndexHashMetrics {
            self.hash.clone()
        }
    }

    #[test]
    fn turnstone_collector_emits_expected_families() {
        let mock_stats = Arc::new(MockServerStats {
            active_conns: 10,
            total_conns: 100,
            active_txs: 5,
            db_conns: HashMap::from([("test_db".to_string(), 42)]),
        });

        let mock_db: Arc<dyn DatabaseMetrics> = Arc::new(MockDatabase {
            stats: DbStats {
                log_size: 128,
                ..Default::default()
            },
            wal_segments: 1,
            hash: IndexHashMetrics::default(),
        });

        let mut stores: HashMap<String, Arc<dyn DatabaseMetrics>> = HashMap::new();
        stores.insert("test_db".to_string(), mock_db);

        let mock_stats: Arc<dyn ServerStatsProvider> = mock_stats;
        let collector = TurnstoneCollector::new(stores, Some(mock_stats));
        let reg = Registry::new();
        reg.register(Box::new(collector)).unwrap();

        let mfs = reg.gather();
        assert!(!mfs.is_empty());

        let mut expected = HashMap::from([
            ("turnstone_server_connections_active", false),
            ("turnstone_server_connections_accepted_total", false),
            ("turnstone_server_transactions_active", false),
            ("turnstone_db_connections", false),
            ("turnstone_db_active_txs", false),
            ("turnstone_db_conflicts_total", false),
            ("turnstone_db_offset", false),
            ("turnstone_db_replica_lag", false),
            ("turnstone_db_replicas", false),
            ("turnstone_db_log_bytes", false),
            ("turnstone_db_log_allocated_bytes", false),
            ("turnstone_db_key_count", false),
            ("turnstone_db_wal_segments", false),
            ("turnstone_db_hash_shards", false),
            ("turnstone_db_index_arena_bytes", false),
            ("turnstone_db_index_allocated_bytes", false),
            ("turnstone_db_index_live_bytes", false),
            ("turnstone_db_hash_shards_compacted_total", false),
            ("turnstone_db_index_compact_bytes_reclaimed_total", false),
            ("turnstone_db_hash_compact_last_timestamp_seconds", false),
        ]);

        for mf in &mfs {
            let name = mf.get_name();
            if let Some(found) = expected.get_mut(name) {
                *found = true;
            }

            match name {
                "turnstone_server_connections_active" => {
                    let val = mf.get_metric()[0].get_gauge().get_value();
                    assert_eq!(val, 10.0);
                }
                "turnstone_db_connections" => {
                    let mut found = false;
                    for m in mf.get_metric() {
                        let db = m
                            .get_label()
                            .iter()
                            .find(|l| l.get_name() == "db")
                            .map(|l| l.get_value())
                            .unwrap_or("");
                        if db == "test_db" && m.get_gauge().get_value() == 42.0 {
                            found = true;
                        }
                    }
                    assert!(found, "expected db connections for test_db = 42");
                }
                "turnstone_db_log_bytes" => {
                    for m in mf.get_metric() {
                        assert!(m.get_gauge().get_value() >= 0.0);
                    }
                }
                _ => {}
            }
        }

        for (name, found) in expected {
            assert!(found, "expected metric family {name} was not collected");
        }
    }
}
