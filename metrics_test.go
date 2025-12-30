package main

import (
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Helper to gather metrics from the server
func gatherMetrics(t *testing.T, srv *Server) map[string]float64 {
	collector := NewTurnstoneCollector(srv)
	reg := prometheus.NewRegistry()
	if err := reg.Register(collector); err != nil {
		t.Fatalf("Register collector failed: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	return parseMetrics(mfs)
}

func parseMetrics(mfs []*dto.MetricFamily) map[string]float64 {
	res := make(map[string]float64)
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			val := 0.0
			if m.Gauge != nil {
				val = *m.Gauge.Value
			} else if m.Counter != nil {
				val = *m.Counter.Value
			}
			res[*mf.Name] = val
		}
	}
	return res
}

// waitForMetric polls until a metric matches the predicate or timeouts
func waitForMetric(t *testing.T, srv *Server, metricName string, predicate func(float64) bool) {
	timeout := 2 * time.Second
	start := time.Now()
	for time.Since(start) < timeout {
		m := gatherMetrics(t, srv)
		if val, ok := m[metricName]; ok && predicate(val) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for metric %s to match predicate", metricName)
}

func TestMetrics_Connections(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// Initial State
	m1 := gatherMetrics(t, srv)
	initialAccepted := m1["turnstone_server_connections_accepted_total"]
	initialActive := m1["turnstone_server_connections_active"]

	// Connect Client
	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))

	// Check after connect
	m2 := gatherMetrics(t, srv)
	if m2["turnstone_server_connections_accepted_total"] != initialAccepted+1 {
		t.Errorf("Accepted connections did not increment")
	}
	if m2["turnstone_server_connections_active"] != initialActive+1 {
		t.Errorf("Active connections did not increment")
	}

	// Close Client
	client.Close()

	// Check after close (Poll for update)
	waitForMetric(t, srv, "turnstone_server_connections_active", func(val float64) bool {
		return val == initialActive
	})
}

func TestMetrics_Transactions(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Start Tx
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)

	m1 := gatherMetrics(t, srv)
	if m1["turnstone_server_transactions_active"] != 1 {
		t.Errorf("Expected 1 active transaction, got %v", m1["turnstone_server_transactions_active"])
	}

	// Commit Tx
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	m2 := gatherMetrics(t, srv)
	if m2["turnstone_server_transactions_active"] != 0 {
		t.Errorf("Expected 0 active transactions, got %v", m2["turnstone_server_transactions_active"])
	}
}

func TestMetrics_StorageIO(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Write Data
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	key := []byte("io_key")
	val := []byte("io_val")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)
	client.AssertStatus(OpCodeSet, setPayload, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	m1 := gatherMetrics(t, srv)

	if m1["turnstone_store_keys_total"] != 1 {
		t.Errorf("Expected 1 key, got %v", m1["turnstone_store_keys_total"])
	}
	if m1["turnstone_store_bytes_written_total"] <= 0 {
		t.Errorf("Expected bytes written > 0")
	}
	if m1["turnstone_store_wal_offset_bytes"] <= 0 {
		t.Errorf("Expected WAL offset > 0")
	}

	// Read Data
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeGet, key, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	m2 := gatherMetrics(t, srv)
	if m2["turnstone_store_bytes_read_total"] <= 0 {
		t.Errorf("Expected bytes read > 0")
	}
}

func TestMetrics_Conflicts(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	c1 := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer c1.Close()
	c2 := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer c2.Close()

	// Conflict Scenario
	c1.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	c2.AssertStatus(OpCodeBegin, nil, ResStatusOK)

	key := []byte("conflict")
	val := []byte("val")
	pl := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
	copy(pl[4:], key)
	copy(pl[4+len(key):], val)

	// C1 writes & commits
	c1.AssertStatus(OpCodeSet, pl, ResStatusOK)
	c1.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// C2 writes & commits (fails)
	c2.AssertStatus(OpCodeSet, pl, ResStatusOK)
	c2.AssertStatus(OpCodeCommit, nil, ResStatusTxConflict)

	m := gatherMetrics(t, srv)
	if m["turnstone_store_conflicts_total"] != 1 {
		t.Errorf("Expected 1 conflict, got %v", m["turnstone_store_conflicts_total"])
	}
}

func TestMetrics_Snapshots(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Start Tx (Acquires snapshot)
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)

	m1 := gatherMetrics(t, srv)
	if m1["turnstone_store_active_snapshots"] != 1 {
		t.Errorf("Expected 1 active snapshot, got %v", m1["turnstone_store_active_snapshots"])
	}

	// Commit (Releases snapshot)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	m2 := gatherMetrics(t, srv)
	if m2["turnstone_store_active_snapshots"] != 0 {
		t.Errorf("Expected 0 active snapshots, got %v", m2["turnstone_store_active_snapshots"])
	}
}

func TestMetrics_Compaction(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Trigger Compaction
	client.AssertStatus(OpCodeCompact, nil, ResStatusOK)

	// Poll for compaction duration metric to appear/update
	waitForMetric(t, srv, "turnstone_store_last_compaction_duration_seconds", func(val float64) bool {
		return val >= 0
	})
}
