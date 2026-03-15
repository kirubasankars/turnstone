package metrics

import (
	"io"
	"log/slog"
	"testing"

	"turnstone/store"

	"github.com/prometheus/client_golang/prometheus"
)

// MockServerStats implements ServerStatsProvider for testing
type MockServerStats struct {
	activeConns int64
	totalConns  uint64
	activeTxs   int64
	dbConns     map[string]int64
}

func (m *MockServerStats) ActiveConns() int64 { return m.activeConns }
func (m *MockServerStats) TotalConns() uint64 { return m.totalConns }
func (m *MockServerStats) ActiveTxs() int64   { return m.activeTxs }
func (m *MockServerStats) DatabaseConns(dbName string) int64 {
	if m.dbConns != nil {
		return m.dbConns[dbName]
	}
	return 0
}

func TestNewTurnstoneCollector(t *testing.T) {
	// 1. Setup Mock Server Stats with database specific data
	mockStats := &MockServerStats{
		activeConns: 10,
		totalConns:  100,
		activeTxs:   5,
		dbConns: map[string]int64{
			"test_db": 42,
		},
	}

	// 2. Initialize a real Store to trigger the loop in Collect
	// We use a temporary directory so we don't pollute the file system.
	tmpDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Create a new store (MinReplicas=0, System=false)
	st, err := store.NewStore(tmpDir, logger, 0, false, "time", 90)
	if err != nil {
		t.Fatalf("Failed to create test store: %v", err)
	}
	defer st.Close()

	stores := map[string]*store.Store{
		"test_db": st,
	}

	// 3. Create collector
	collector := NewTurnstoneCollector(stores, mockStats)

	// 4. Verify Registration
	reg := prometheus.NewRegistry()
	if err := reg.Register(collector); err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// 5. Gather metrics
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// 6. Verify Metrics Existence and Values
	if len(mfs) == 0 {
		t.Errorf("Expected metrics, got none")
	}

	// Map of expected metric names to verify existence
	expectedFamilies := map[string]bool{
		"turnstone_server_connections_active":         false,
		"turnstone_server_connections_accepted_total": false,
		"turnstone_server_transactions_active":        false,
		"turnstone_db_connections":                    false,
		"turnstone_db_active_txs":                     false,
		"turnstone_db_conflicts_total":                false,
		"turnstone_db_offset":                         false,
		"turnstone_db_replica_lag":                    false,
		"turnstone_db_wal_files":                      false,
		"turnstone_db_wal_bytes":                      false,
		"turnstone_db_vlog_files":                     false,
		"turnstone_db_vlog_bytes":                     false,
	}

	for _, mf := range mfs {
		name := *mf.Name
		if _, ok := expectedFamilies[name]; ok {
			expectedFamilies[name] = true
		}

		// Verify specific values for deterministically mocked data
		switch name {
		case "turnstone_server_connections_active":
			if len(mf.Metric) > 0 && mf.Metric[0].Gauge != nil {
				if val := *mf.Metric[0].Gauge.Value; val != 10 {
					t.Errorf("Expected server active conns 10, got %v", val)
				}
			}
		case "turnstone_db_connections":
			// Should find label db="test_db" with value 42
			found := false
			for _, m := range mf.Metric {
				for _, lbl := range m.Label {
					if *lbl.Name == "db" && *lbl.Value == "test_db" {
						if m.Gauge != nil && *m.Gauge.Value == 42 {
							found = true
						}
					}
				}
			}
			if !found {
				t.Errorf("Expected database connections for 'test_db' to be 42")
			}
		case "turnstone_db_wal_files":
			// Just ensure we are getting > 0 since a fresh store creates at least 1 WAL
			for _, m := range mf.Metric {
				if m.Gauge != nil && *m.Gauge.Value < 1 {
					t.Errorf("Expected at least 1 WAL file, got %v", *m.Gauge.Value)
				}
			}
		}
	}

	// Ensure all expected families were present
	for name, found := range expectedFamilies {
		if !found {
			t.Errorf("Expected metric family %s was not collected", name)
		}
	}
}
