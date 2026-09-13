// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package devtool

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"turnstone/database"
	"turnstone/metrics"
)

// Config configures the devtool HTTP server.
type Config struct {
	Addr        string
	MetricsAddr string
	Stores      map[string]*database.Database
	ServerStats metrics.ServerStatsProvider
	Logger      *slog.Logger
}

// Start launches the devtool HTTP server on localhost.
func Start(cfg Config) {
	if cfg.Addr == "" {
		return
	}
	if strings.HasPrefix(cfg.Addr, ":") {
		cfg.Addr = "127.0.0.1" + cfg.Addr
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	srv := &httpServer{
		stores:      cfg.Stores,
		serverStats: cfg.ServerStats,
		metricsAddr: cfg.MetricsAddr,
		logger:      logger,
	}

	mux := http.NewServeMux()
	mux.Handle("/static/", staticHandler())
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/api/databases", srv.handleDatabases)
	mux.HandleFunc("/api/databases/", srv.handleDatabase)
	mux.HandleFunc("/api/metrics", srv.handleMetrics)
	mux.HandleFunc("/api/server", srv.handleServer)

	go func() {
		logger.Info("Devtool UI starting", "addr", cfg.Addr)
		if err := http.ListenAndServe(cfg.Addr, mux); err != nil {
			logger.Error("Devtool server stopped", "err", err)
		}
	}()
}

type httpServer struct {
	stores      map[string]*database.Database
	serverStats metrics.ServerStatsProvider
	metricsAddr string
	logger      *slog.Logger
}

func (s *httpServer) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *httpServer) writeError(w http.ResponseWriter, status int, msg string) {
	s.writeJSON(w, status, map[string]string{"error": msg})
}

func (s *httpServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "index not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *httpServer) handleDatabases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	names := make([]string, 0, len(s.stores))
	for name := range s.stores {
		names = append(names, name)
	}
	sortStrings(names)
	s.writeJSON(w, http.StatusOK, map[string]any{"databases": names})
}

func (s *httpServer) handleDatabase(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/databases/")
	parts := strings.Split(path, "/")
	if len(parts) < 1 || parts[0] == "" {
		s.writeError(w, http.StatusBadRequest, "database required")
		return
	}
	dbName := parts[0]
	db, ok := s.stores[dbName]
	if !ok {
		s.writeError(w, http.StatusNotFound, "database not found")
		return
	}

	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleDBStats(w, db, dbName)
		return
	}

	switch parts[1] {
	case "stats":
		if r.Method != http.MethodGet {
			s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleDBStats(w, db, dbName)
	case "keys":
		if len(parts) == 2 {
			s.handleKeyList(w, r, db)
			return
		}
		key := strings.Join(parts[2:], "/")
		s.handleKeyValue(w, r, dbName, key)
	default:
		s.writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *httpServer) handleDBStats(w http.ResponseWriter, db *database.Database, dbName string) {
	stats := db.Stats()
	conns := int64(0)
	if s.serverStats != nil {
		conns = s.serverStats.DatabaseConns(dbName)
	}
	state := db.GetState()
	s.writeJSON(w, http.StatusOK, map[string]any{
		"db":                  dbName,
		"state":               state,
		"key_count":           stats.KeyCount,
		"conflicts":           stats.Conflicts,
		"active_connections":  conns,
		"log_bytes":           stats.LogSize,
		"log_allocated_bytes": stats.LogAllocated,
		"active_txs":          stats.ActiveTxs,
		"replica_lag":         stats.ReplicaLag,
		"uptime":              stats.Uptime,
		"min_replicas":        db.MinReplicas(),
	})
}

func (s *httpServer) handleKeyList(w http.ResponseWriter, r *http.Request, db *database.Database) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	cursor, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit == 0 {
		limit = 100
	}
	keys, err := db.ListKeys(prefix, cursor, limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	nextCursor := cursor + len(keys)
	s.writeJSON(w, http.StatusOK, map[string]any{
		"keys":        keys,
		"cursor":      cursor,
		"next_cursor": nextCursor,
		"has_more":    len(keys) == limit,
	})
}

func (s *httpServer) handleKeyValue(w http.ResponseWriter, r *http.Request, dbName, key string) {
	db := s.stores[dbName]
	switch r.Method {
	case http.MethodGet:
		value, err := getKey(db, key)
		if err != nil {
			status, msg := mapDataError(err)
			s.writeError(w, status, msg)
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": string(value)})
	case http.MethodPut:
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4*1024*1024+1024))
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		value := extractValue(body, r.Header.Get("Content-Type"))
		if err := setKey(db, key, value); err != nil {
			status, msg := mapDataError(err)
			s.writeError(w, status, msg)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := delKey(db, key); err != nil {
			status, msg := mapDataError(err)
			s.writeError(w, status, msg)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func extractValue(body []byte, contentType string) []byte {
	if strings.Contains(contentType, "application/json") {
		var payload struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(body, &payload); err == nil {
			return []byte(payload.Value)
		}
	}
	return body
}

func (s *httpServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	data, err := fetchMetrics(s.metricsAddr)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, data)
}

func (s *httpServer) handleServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	resp := map[string]any{}
	if s.serverStats != nil {
		resp["active_connections"] = s.serverStats.ActiveConns()
		resp["total_connections"] = s.serverStats.TotalConns()
		resp["active_transactions"] = s.serverStats.ActiveTxs()
	}
	s.writeJSON(w, http.StatusOK, resp)
}

func sortStrings(ss []string) {
	for i := 0; i < len(ss); i++ {
		for j := i + 1; j < len(ss); j++ {
			if ss[j] < ss[i] {
				ss[i], ss[j] = ss[j], ss[i]
			}
		}
	}
}
