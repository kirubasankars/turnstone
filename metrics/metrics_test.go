// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// MockServerStats implements ServerStatsProvider for testing
type MockServerStats struct {
	activeConns int64
	totalConns  uint64
	activeTxs   int64
}

func (m *MockServerStats) ActiveConns() int64 { return m.activeConns }
func (m *MockServerStats) TotalConns() uint64 { return m.totalConns }
func (m *MockServerStats) ActiveTxs() int64   { return m.activeTxs }

func TestNewTurnstoneCollector(t *testing.T) {
	// Mock dependencies
	mockStats := &MockServerStats{
		activeConns: 10,
		totalConns:  100,
		activeTxs:   5,
	}

	// Create collector
	// Passing nil for stores as initializing real stores is heavy for unit tests
	// and we primarily want to check if metrics are registered and collected.
	collector := NewTurnstoneCollector(nil, mockStats)

	// Verify we can register it with Prometheus
	reg := prometheus.NewRegistry()
	if err := reg.Register(collector); err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Gather metrics
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Basic check to see if we got some metrics back
	if len(mfs) == 0 {
		t.Errorf("Expected metrics, got none")
	}

	// Check for specific server metrics existence
	found := false
	for _, mf := range mfs {
		if *mf.Name == "turnstone_server_connections_active" {
			found = true
			// Validate value
			if len(mf.Metric) > 0 && mf.Metric[0].Gauge != nil {
				if *mf.Metric[0].Gauge.Value != 10 {
					t.Errorf("Expected active connections 10, got %v", *mf.Metric[0].Gauge.Value)
				}
			}
			break
		}
	}
	if !found {
		t.Errorf("Expected turnstone_server_connections_active metric not found")
	}
}
