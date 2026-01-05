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
	keys        *prometheus.Desc
	activeConns *prometheus.Desc
	totalConns  *prometheus.Desc
}

func NewTurnstoneCollector(stores map[string]*store.Store, stats ServerStatsProvider) *TurnstoneCollector {
	return &TurnstoneCollector{
		stores:      stores,
		serverStats: stats,
		keys:        newDesc("store", "keys_total", "Total keys"),
		activeConns: newDesc("server", "connections_active", "Active connections"),
		totalConns:  newDesc("server", "connections_accepted_total", "Total connections"),
	}
}

func newDesc(sub, name, help string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, sub, name), help, nil, nil)
}

func (c *TurnstoneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.keys
	ch <- c.activeConns
	ch <- c.totalConns
}

func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	var keys float64

	for _, db := range c.stores {
		stats := db.Stats()
		keys += float64(stats.KeyCount)
	}

	ch <- prometheus.MustNewConstMetric(c.keys, prometheus.GaugeValue, keys)

	if c.serverStats != nil {
		ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(c.serverStats.ActiveConns()))
		ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(c.serverStats.TotalConns()))
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
