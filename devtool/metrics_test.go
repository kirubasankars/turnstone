// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package devtool

import (
	"strings"
	"testing"
)

func TestParsePrometheusMetrics(t *testing.T) {
	input := `# HELP turnstone_server_connections_active Active connections
turnstone_server_connections_active 3
turnstone_db_key_count{db="0"} 42
turnstone_db_conflicts_total{db="1"} 5
go_goroutines 10
`
	out, err := parsePrometheusMetrics(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out.Server) != 1 {
		t.Fatalf("expected 1 server metric, got %d", len(out.Server))
	}
	if out.Server[0].Value != 3 {
		t.Fatalf("expected value 3, got %v", out.Server[0].Value)
	}
	if len(out.DB) != 2 {
		t.Fatalf("expected 2 db metrics, got %d", len(out.DB))
	}
	if out.DB[0].Labels["db"] != "0" {
		t.Fatalf("expected db label 0, got %v", out.DB[0].Labels)
	}
}
