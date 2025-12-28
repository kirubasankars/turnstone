package main

import (
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "turnstone"

// TurnstoneCollector implements the prometheus.Collector interface
// to expose internal store and server metrics.
type TurnstoneCollector struct {
	server *Server

	// Store Metrics
	keys            *prometheus.Desc
	indexSize       *prometheus.Desc
	activeSnapshots *prometheus.Desc
	walOffset       *prometheus.Desc
	conflictTotal   *prometheus.Desc
	recoverySeconds *prometheus.Desc
	pendingOps      *prometheus.Desc

	// Server Metrics
	activeConns *prometheus.Desc
	totalConns  *prometheus.Desc
	activeTxs   *prometheus.Desc
}

// NewTurnstoneCollector creates a new collector for the given server instance.
func NewTurnstoneCollector(s *Server) *TurnstoneCollector {
	return &TurnstoneCollector{
		server: s,
		keys: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "keys_total"),
			"Total number of keys.", nil, nil,
		),
		indexSize: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "index_size_bytes"),
			"Estimated memory usage of the in-memory index in bytes.", nil, nil,
		),
		activeSnapshots: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "active_snapshots"),
			"Number of active transaction snapshots.", nil, nil,
		),
		walOffset: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "wal_offset_bytes"),
			"Current byte offset in the Write-Ahead Log.", nil, nil,
		),
		conflictTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "conflicts_total"),
			"Total number of transaction conflicts detected.", nil, nil,
		),
		recoverySeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "recovery_duration_seconds"),
			"Time taken for the last WAL recovery process.", nil, nil,
		),
		pendingOps: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "pending_ops"),
			"Current number of pending operations in the write queue.", nil, nil,
		),
		activeConns: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "connections_active"),
			"Number of currently active client connections.", nil, nil,
		),
		totalConns: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "connections_accepted_total"),
			"Total number of connections accepted since startup.", nil, nil,
		),
		activeTxs: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "transactions_active"),
			"Number of currently active transactions.", nil, nil,
		),
	}
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (c *TurnstoneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.keys
	ch <- c.indexSize
	ch <- c.activeSnapshots
	ch <- c.walOffset
	ch <- c.conflictTotal
	ch <- c.recoverySeconds
	ch <- c.pendingOps
	ch <- c.activeConns
	ch <- c.totalConns
	ch <- c.activeTxs
}

// Collect is called by the Prometheus registry when collecting metrics.
func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	if c.server == nil || c.server.store == nil {
		return
	}

	stats := c.server.store.Stats()

	ch <- prometheus.MustNewConstMetric(c.keys, prometheus.GaugeValue, float64(stats.KeyCount))
	ch <- prometheus.MustNewConstMetric(c.indexSize, prometheus.GaugeValue, float64(stats.IndexSizeBytes))
	ch <- prometheus.MustNewConstMetric(c.activeSnapshots, prometheus.GaugeValue, float64(stats.ActiveSnapshots))
	ch <- prometheus.MustNewConstMetric(c.walOffset, prometheus.GaugeValue, float64(stats.Offset))
	ch <- prometheus.MustNewConstMetric(c.conflictTotal, prometheus.CounterValue, float64(stats.ConflictCount))
	ch <- prometheus.MustNewConstMetric(c.recoverySeconds, prometheus.GaugeValue, stats.RecoveryDuration.Seconds())
	ch <- prometheus.MustNewConstMetric(c.pendingOps, prometheus.GaugeValue, float64(stats.PendingOps))

	activeConns := atomic.LoadInt64(&c.server.activeConns)
	totalConns := atomic.LoadUint64(&c.server.totalConns)
	txs := atomic.LoadInt64(&c.server.activeTxs)

	ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(activeConns))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(totalConns))
	ch <- prometheus.MustNewConstMetric(c.activeTxs, prometheus.GaugeValue, float64(txs))
}

// StartMetricsServer starts an HTTP server for Prometheus scraping.
// It binds to localhost if no IP is specified for security.
func StartMetricsServer(addr string, s *Server, logger *slog.Logger) {
	if addr == "" {
		return
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
		logger.Info("Metrics address defaults to localhost for security", "addr", addr)
	}

	reg := prometheus.NewRegistry()
	if s != nil {
		reg.MustRegister(NewTurnstoneCollector(s))
	}

	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("Prometheus metrics server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server failed", "err", err)
		}
	}()
}
