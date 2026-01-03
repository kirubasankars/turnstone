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
// without directly importing the server package, avoiding circular deps.
type ServerStatsProvider interface {
	ActiveConns() int64
	TotalConns() uint64
	ActiveTxs() int64
}

type TurnstoneCollector struct {
	stores      map[string]*store.Store
	serverStats ServerStatsProvider
	keys        *prometheus.Desc
	memSize     *prometheus.Desc
	logSeq      *prometheus.Desc
	walOffset   *prometheus.Desc
	conflict    *prometheus.Desc
	recoverySec *prometheus.Desc
	pendingOps  *prometheus.Desc
	bytesWrite  *prometheus.Desc
	bytesRead   *prometheus.Desc
	activeConns *prometheus.Desc
	totalConns  *prometheus.Desc
	activeTxs   *prometheus.Desc
}

func NewTurnstoneCollector(stores map[string]*store.Store, stats ServerStatsProvider) *TurnstoneCollector {
	return &TurnstoneCollector{
		stores:      stores,
		serverStats: stats,
		keys:        newDesc("store", "keys_total", "Total keys"),
		memSize:     newDesc("store", "memory_bytes", "Memory usage"),
		logSeq:      newDesc("store", "log_sequence", "Current Log Sequence"),
		walOffset:   newDesc("store", "wal_offset_bytes", "WAL offset"),
		conflict:    newDesc("store", "conflicts_total", "Total conflicts"),
		recoverySec: newDesc("store", "recovery_duration_seconds", "Recovery time"),
		pendingOps:  newDesc("store", "pending_ops", "Queue depth"),
		bytesWrite:  newDesc("store", "bytes_written_total", "Total bytes written"),
		bytesRead:   newDesc("store", "bytes_read_total", "Total bytes read"),
		activeConns: newDesc("server", "connections_active", "Active connections"),
		totalConns:  newDesc("server", "connections_accepted_total", "Total connections"),
		activeTxs:   newDesc("server", "transactions_active", "Active transactions"),
	}
}

func newDesc(sub, name, help string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, sub, name), help, nil, nil)
}

func (c *TurnstoneCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.keys
	ch <- c.memSize
	ch <- c.logSeq
	ch <- c.walOffset
	ch <- c.conflict
	ch <- c.recoverySec
	ch <- c.pendingOps
	ch <- c.bytesWrite
	ch <- c.bytesRead
	ch <- c.activeConns
	ch <- c.totalConns
	ch <- c.activeTxs
}

func (c *TurnstoneCollector) Collect(ch chan<- prometheus.Metric) {
	var keys, mem, lSeq, offset, conflicts, pendingOps float64
	var bWritten, bRead float64
	var recoverySec float64

	for _, db := range c.stores {
		stats := db.Stats()
		keys += float64(stats.KeyCount)
		mem += float64(stats.MemorySizeBytes)
		lSeq += float64(stats.NextLogSeq)
		offset += float64(stats.Offset)
		conflicts += float64(stats.ConflictCount)
		pendingOps += float64(stats.PendingOps)
		bWritten += float64(stats.BytesWritten)
		bRead += float64(stats.BytesRead)
		if stats.RecoveryDuration.Seconds() > recoverySec {
			recoverySec = stats.RecoveryDuration.Seconds()
		}
	}

	ch <- prometheus.MustNewConstMetric(c.keys, prometheus.GaugeValue, keys)
	ch <- prometheus.MustNewConstMetric(c.memSize, prometheus.GaugeValue, mem)
	ch <- prometheus.MustNewConstMetric(c.logSeq, prometheus.GaugeValue, lSeq)
	ch <- prometheus.MustNewConstMetric(c.walOffset, prometheus.GaugeValue, offset)
	ch <- prometheus.MustNewConstMetric(c.conflict, prometheus.CounterValue, conflicts)
	ch <- prometheus.MustNewConstMetric(c.recoverySec, prometheus.GaugeValue, recoverySec)
	ch <- prometheus.MustNewConstMetric(c.pendingOps, prometheus.GaugeValue, pendingOps)
	ch <- prometheus.MustNewConstMetric(c.bytesWrite, prometheus.CounterValue, bWritten)
	ch <- prometheus.MustNewConstMetric(c.bytesRead, prometheus.CounterValue, bRead)

	if c.serverStats != nil {
		ch <- prometheus.MustNewConstMetric(c.activeConns, prometheus.GaugeValue, float64(c.serverStats.ActiveConns()))
		ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.CounterValue, float64(c.serverStats.TotalConns()))
		ch <- prometheus.MustNewConstMetric(c.activeTxs, prometheus.GaugeValue, float64(c.serverStats.ActiveTxs()))
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
