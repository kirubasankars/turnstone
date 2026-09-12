// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package metrics

import (
	"log/slog"
	"net/http"
	"strings"

	"turnstone/database"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "turnstone"

// ServerStatsProvider interface allows the collector to get stats from the Server
type ServerStatsProvider interface {
	ActiveConns() int64
	TotalConns() uint64
	ActiveTxs() int64
	DatabaseConns(dbName string) int64
}

type TurnstoneCollector struct {
	stores      map[string]*database.Database
	serverStats ServerStatsProvider

	// Server-wide metrics
	activeConns    *prometheus.Desc
	totalConns     *prometheus.Desc
	totalActiveTxs *prometheus.Desc

	// Per-database metrics
	dbConnections  *prometheus.Desc
	dbActiveTxs    *prometheus.Desc
	dbConflicts    *prometheus.Desc
	dbOffset       *prometheus.Desc
	dbReplicaLag   *prometheus.Desc
	dbLogBytes     *prometheus.Desc
	dbLogAllocated *prometheus.Desc
	dbKeyCount     *prometheus.Desc
}

func NewTurnstoneCollector(stores map[string]*database.Database, stats ServerStatsProvider) *TurnstoneCollector {
	return &TurnstoneCollector{
		stores:      stores,
		serverStats: stats,

		// Server Wide
		activeConns:    newDesc("server", "connections_active", "Active connections"),
		totalConns:     newDesc("server", "connections_accepted_total", "Total connections"),
		totalActiveTxs: newDesc("server", "transactions_active", "Total active transactions across server"),

		// Per Database
		dbConnections:  newDescWithLabels("db", "connections", "Active connections to this database", []string{"db"}),
		dbActiveTxs:    newDescWithLabels("db", "active_txs", "Active transactions in database", []string{"db"}),
		dbConflicts:    newDescWithLabels("db", "conflicts_total", "Total transaction conflicts in database", []string{"db"}),
		dbOffset:       newDescWithLabels("db", "offset", "Exclusive end byte offset of the WAL", []string{"db"}),
		dbReplicaLag:   newDescWithLabels("db", "replica_lag", "Lag of the slowest replica in bytes", []string{"db"}),
		dbLogBytes:     newDescWithLabels("db", "log_bytes", "Logical size of the WAL in bytes", []string{"db"}),
		dbLogAllocated: newDescWithLabels("db", "log_allocated_bytes", "Allocated on-disk bytes for WAL segments", []string{"db"}),
		dbKeyCount:     newDescWithLabels("db", "key_count", "Approximate number of live keys in database", []string{"db"}),
	}
}

func newDesc(sub, name, help string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, sub, name), help, nil, nil)
}

func newDescWithLabels(sub, name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, sub, name), help, labels, nil)
}

func (c *TurnstoneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeConns
	ch <- c.totalConns
	ch <- c.totalActiveTxs
	ch <- c.dbConnections
	ch <- c.dbActiveTxs
	ch <- c.dbConflicts
	ch <- c.dbOffset
	ch <- c.dbReplicaLag
	ch <- c.dbLogBytes
	ch <- c.dbLogAllocated
	ch <- c.dbKeyCount
}

func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	if c.serverStats != nil {
		ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(c.serverStats.ActiveConns()))
		ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(c.serverStats.TotalConns()))
		ch <- prometheus.MustNewConstMetric(c.totalActiveTxs, prometheus.GaugeValue, float64(c.serverStats.ActiveTxs()))
	}

	for name, db := range c.stores {
		stats := db.Stats()

		dbConns := float64(0)
		if c.serverStats != nil {
			dbConns = float64(c.serverStats.DatabaseConns(name))
		}

		ch <- prometheus.MustNewConstMetric(c.dbConnections, prometheus.GaugeValue, dbConns, name)
		ch <- prometheus.MustNewConstMetric(c.dbActiveTxs, prometheus.GaugeValue, float64(stats.ActiveTxs), name)
		ch <- prometheus.MustNewConstMetric(c.dbConflicts, prometheus.CounterValue, float64(stats.Conflicts), name)
		ch <- prometheus.MustNewConstMetric(c.dbOffset, prometheus.GaugeValue, float64(stats.Offset), name)
		ch <- prometheus.MustNewConstMetric(c.dbReplicaLag, prometheus.GaugeValue, float64(stats.ReplicaLag), name)
		ch <- prometheus.MustNewConstMetric(c.dbLogBytes, prometheus.GaugeValue, float64(stats.LogSize), name)
		ch <- prometheus.MustNewConstMetric(c.dbLogAllocated, prometheus.GaugeValue, float64(stats.LogAllocated), name)
		ch <- prometheus.MustNewConstMetric(c.dbKeyCount, prometheus.GaugeValue, float64(stats.KeyCount), name)
	}
}

func StartMetricsServer(addr string, stores map[string]*database.Database, serverStats ServerStatsProvider, logger *slog.Logger) {
	if addr == "" {
		return
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewTurnstoneCollector(stores, serverStats))
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	go func() {
		logger.Info("Metrics server starting", "addr", addr)
		http.ListenAndServe(addr, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}()
}
