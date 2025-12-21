package main

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "turnstone"

type TurnstoneCollector struct {
	server          *Server
	keys            *prometheus.Desc
	pendingBytes    *prometheus.Desc
	activeSnapshots *prometheus.Desc
	walOffset       *prometheus.Desc
	generation      *prometheus.Desc

	activeConns *prometheus.Desc
	totalConns  *prometheus.Desc
	activeSyncs *prometheus.Desc
	memoryUsed  *prometheus.Desc
	activeTxs   *prometheus.Desc
}

func NewTurnstoneCollector(s *Server) *TurnstoneCollector {
	return &TurnstoneCollector{
		server: s,
		keys: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "keys_total"),
			"Total number of keys.", []string{"db"}, nil,
		),
		pendingBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "pending_write_bytes"),
			"Bytes buffered.", []string{"db"}, nil,
		),
		activeSnapshots: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "active_snapshots"),
			"Number of snapshots.", []string{"db"}, nil,
		),
		walOffset: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "wal_offset_bytes"),
			"WAL offset.", []string{"db"}, nil,
		),
		generation: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "store", "generation"),
			"Current generation.", []string{"db"}, nil,
		),
		activeConns: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "connections_active"),
			"Active connections.", nil, nil,
		),
		totalConns: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "connections_accepted_total"),
			"Total connections.", nil, nil,
		),
		activeSyncs: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "cdc_clients_active"),
			"Active CDC clients.", nil, nil,
		),
		memoryUsed: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "memory_used_bytes"),
			"Memory used.", nil, nil,
		),
		activeTxs: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "server", "transactions_active"),
			"Active transactions.", nil, nil,
		),
	}
}

func (c *TurnstoneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.keys
	ch <- c.pendingBytes
	ch <- c.activeSnapshots
	ch <- c.walOffset
	ch <- c.generation
	ch <- c.activeConns
	ch <- c.totalConns
	ch <- c.activeSyncs
	ch <- c.memoryUsed
	ch <- c.activeTxs
}

func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	if c.server == nil {
		return
	}

	// Lock to safely iterate over stores which might be initializing
	c.server.storeMu.RLock()
	defer c.server.storeMu.RUnlock()

	// Collect metrics for each DB
	for i, store := range c.server.stores {
		var keys int
		var pending int64
		var snaps int
		var off int64
		var gen uint64

		// Check if initialized to get actual stats, otherwise report 0s
		if store != nil {
			keys, _, pending, snaps, off, gen = store.Stats()
		}

		dbLabel := strconv.Itoa(i)

		ch <- prometheus.MustNewConstMetric(c.keys, prometheus.GaugeValue, float64(keys), dbLabel)
		ch <- prometheus.MustNewConstMetric(c.pendingBytes, prometheus.GaugeValue, float64(pending), dbLabel)
		ch <- prometheus.MustNewConstMetric(c.activeSnapshots, prometheus.GaugeValue, float64(snaps), dbLabel)
		ch <- prometheus.MustNewConstMetric(c.walOffset, prometheus.GaugeValue, float64(off), dbLabel)
		ch <- prometheus.MustNewConstMetric(c.generation, prometheus.GaugeValue, float64(gen), dbLabel)
	}

	activeConns := atomic.LoadInt64(&c.server.activeConns)
	totalConns := atomic.LoadUint64(&c.server.totalConns)
	activeSyncs := atomic.LoadInt64(&c.server.activeSyncs)
	mem := atomic.LoadInt64(&c.server.usedMemory)
	txs := atomic.LoadInt64(&c.server.activeTxs)

	ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(activeConns))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(totalConns))
	ch <- prometheus.MustNewConstMetric(c.activeSyncs, prometheus.GaugeValue, float64(activeSyncs))
	ch <- prometheus.MustNewConstMetric(c.memoryUsed, prometheus.GaugeValue, float64(mem))
	ch <- prometheus.MustNewConstMetric(c.activeTxs, prometheus.GaugeValue, float64(txs))
}

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
