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

type TurnstoneCollector struct {
	server          *Server
	keys            *prometheus.Desc
	indexSize       *prometheus.Desc
	activeSnapshots *prometheus.Desc
	walOffset       *prometheus.Desc
	conflictTotal   *prometheus.Desc
	recoverySeconds *prometheus.Desc
	pendingOps      *prometheus.Desc
	bytesWritten    *prometheus.Desc
	bytesRead       *prometheus.Desc
	compactionDur   *prometheus.Desc
	activeConns     *prometheus.Desc
	totalConns      *prometheus.Desc
	activeTxs       *prometheus.Desc
}

func NewTurnstoneCollector(s *Server) *TurnstoneCollector {
	return &TurnstoneCollector{
		server:          s,
		keys:            newDesc("store", "keys_total", "Total keys"),
		indexSize:       newDesc("store", "index_size_bytes", "Index memory usage"),
		activeSnapshots: newDesc("store", "active_snapshots", "Active snapshots"),
		walOffset:       newDesc("store", "wal_offset_bytes", "WAL offset"),
		conflictTotal:   newDesc("store", "conflicts_total", "Total conflicts"),
		recoverySeconds: newDesc("store", "recovery_duration_seconds", "Recovery time"),
		pendingOps:      newDesc("store", "pending_ops", "Queue depth"),
		bytesWritten:    newDesc("store", "bytes_written_total", "Total bytes written to WAL"),
		bytesRead:       newDesc("store", "bytes_read_total", "Total bytes read from WAL"),
		compactionDur:   newDesc("store", "last_compaction_duration_seconds", "Duration of last compaction"),
		activeConns:     newDesc("server", "connections_active", "Active connections"),
		totalConns:      newDesc("server", "connections_accepted_total", "Total connections"),
		activeTxs:       newDesc("server", "transactions_active", "Active transactions"),
	}
}

func newDesc(sub, name, help string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, sub, name), help, nil, nil)
}

func (c *TurnstoneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.keys
	ch <- c.indexSize
	ch <- c.activeSnapshots
	ch <- c.walOffset
	ch <- c.conflictTotal
	ch <- c.recoverySeconds
	ch <- c.pendingOps
	ch <- c.bytesWritten
	ch <- c.bytesRead
	ch <- c.compactionDur
	ch <- c.activeConns
	ch <- c.totalConns
	ch <- c.activeTxs
}

func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	if c.server == nil || len(c.server.stores) == 0 {
		return
	}

	var keys, indexSz, snaps, offset, conflicts, pendingOps float64
	var bWritten, bRead, compactDur float64
	var recoverySec float64

	for _, db := range c.server.stores {
		stats := db.Stats()
		keys += float64(stats.KeyCount)
		indexSz += float64(stats.IndexSizeBytes)
		snaps += float64(stats.ActiveSnapshots)
		offset += float64(stats.Offset)
		conflicts += float64(stats.ConflictCount)
		pendingOps += float64(stats.PendingOps)
		bWritten += float64(stats.BytesWritten)
		bRead += float64(stats.BytesRead)
		if stats.LastCompactionDuration.Seconds() > compactDur {
			compactDur = stats.LastCompactionDuration.Seconds()
		}
		if stats.RecoveryDuration.Seconds() > recoverySec {
			recoverySec = stats.RecoveryDuration.Seconds()
		}
	}

	ch <- prometheus.MustNewConstMetric(c.keys, prometheus.GaugeValue, keys)
	ch <- prometheus.MustNewConstMetric(c.indexSize, prometheus.GaugeValue, indexSz)
	ch <- prometheus.MustNewConstMetric(c.activeSnapshots, prometheus.GaugeValue, snaps)
	ch <- prometheus.MustNewConstMetric(c.walOffset, prometheus.GaugeValue, offset)
	ch <- prometheus.MustNewConstMetric(c.conflictTotal, prometheus.CounterValue, conflicts)
	ch <- prometheus.MustNewConstMetric(c.recoverySeconds, prometheus.GaugeValue, recoverySec)
	ch <- prometheus.MustNewConstMetric(c.pendingOps, prometheus.GaugeValue, pendingOps)
	ch <- prometheus.MustNewConstMetric(c.bytesWritten, prometheus.CounterValue, bWritten)
	ch <- prometheus.MustNewConstMetric(c.bytesRead, prometheus.CounterValue, bRead)
	ch <- prometheus.MustNewConstMetric(c.compactionDur, prometheus.GaugeValue, compactDur)

	ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(atomic.LoadInt64(&c.server.activeConns)))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(atomic.LoadUint64(&c.server.totalConns)))
	ch <- prometheus.MustNewConstMetric(c.activeTxs, prometheus.GaugeValue, float64(atomic.LoadInt64(&c.server.activeTxs)))
}

func StartMetricsServer(addr string, s *Server, logger *slog.Logger) {
	if addr == "" {
		return
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewTurnstoneCollector(s))
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	go func() {
		logger.Info("Metrics server starting", "addr", addr)
		http.ListenAndServe(addr, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}()
}
