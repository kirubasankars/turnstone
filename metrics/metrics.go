package metrics

import (
	"log/slog"
	"net/http"
	"strings"

	"turnstone/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "turnstone"

// ServerStatsProvider interface allows the collector to get stats from the Server
type ServerStatsProvider interface {
	ActiveConns() int64
	TotalConns() uint64
	ActiveTxs() int64
}

type TurnstoneCollector struct {
	stores      map[string]*store.Store
	serverStats ServerStatsProvider

	// Server-wide metrics
	activeConns    *prometheus.Desc
	totalConns     *prometheus.Desc
	totalActiveTxs *prometheus.Desc // Aggregate server-wide active transactions

	// Per-partition metrics
	partitionKeys      *prometheus.Desc
	partitionActiveTxs *prometheus.Desc
	partitionConflicts *prometheus.Desc
	partitionOffset    *prometheus.Desc
}

func NewTurnstoneCollector(stores map[string]*store.Store, stats ServerStatsProvider) *TurnstoneCollector {
	return &TurnstoneCollector{
		stores:      stores,
		serverStats: stats,

		activeConns:    newDesc("server", "connections_active", "Active connections"),
		totalConns:     newDesc("server", "connections_accepted_total", "Total connections"),
		totalActiveTxs: newDesc("server", "transactions_active", "Total active transactions across server"),

		partitionKeys:      newDescWithLabels("partition", "keys", "Total keys in partition", []string{"partition"}),
		partitionActiveTxs: newDescWithLabels("partition", "active_txs", "Active transactions in partition", []string{"partition"}),
		partitionConflicts: newDescWithLabels("partition", "conflicts_total", "Total transaction conflicts in partition", []string{"partition"}),
		partitionOffset:    newDescWithLabels("partition", "offset", "Current WAL/VLog offset (LogID)", []string{"partition"}),
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
	ch <- c.partitionKeys
	ch <- c.partitionActiveTxs
	ch <- c.partitionConflicts
	ch <- c.partitionOffset
}

func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	if c.serverStats != nil {
		ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(c.serverStats.ActiveConns()))
		ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(c.serverStats.TotalConns()))
		ch <- prometheus.MustNewConstMetric(c.totalActiveTxs, prometheus.GaugeValue, float64(c.serverStats.ActiveTxs()))
	}

	for name, db := range c.stores {
		stats := db.Stats()

		ch <- prometheus.MustNewConstMetric(c.partitionKeys, prometheus.GaugeValue, float64(stats.KeyCount), name)
		ch <- prometheus.MustNewConstMetric(c.partitionActiveTxs, prometheus.GaugeValue, float64(stats.ActiveTxs), name)
		ch <- prometheus.MustNewConstMetric(c.partitionConflicts, prometheus.CounterValue, float64(stats.Conflicts), name)
		ch <- prometheus.MustNewConstMetric(c.partitionOffset, prometheus.GaugeValue, float64(stats.Offset), name)
	}
}

func StartMetricsServer(addr string, stores map[string]*store.Store, serverStats ServerStatsProvider, logger *slog.Logger) {
	if addr == "" {
		return
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewTurnstoneCollector(stores, serverStats))
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	go func() {
		logger.Info("Metrics server starting", "addr", addr)
		http.ListenAndServe(addr, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}()
}
