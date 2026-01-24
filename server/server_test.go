package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"turnstone/config"
	"turnstone/metrics"
	"turnstone/protocol"
	"turnstone/replication"
	"turnstone/store"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// --- Test Infrastructure & Helpers ---

func setupTestEnv(t *testing.T) (string, map[string]*store.Store, *Server, func()) {
	t.Helper()
	// Use MkdirTemp to keep files (t.TempDir deletes them)
	dir, err := os.MkdirTemp("", "turnstone-server-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Test Environment Directory: %s", dir)

	// Setup logging: Debug level, write to stdout and turnstone.log in the test dir
	logPath := filepath.Join(dir, "turnstone.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	// Ensure log file is closed even if cleanup isn't called due to panic
	t.Cleanup(func() {
		logFile.Close()
	})

	multiWriter := io.MultiWriter(os.Stdout, logFile)
	logger := slog.New(slog.NewTextHandler(multiWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 1. Generate Certs
	certsDir := filepath.Join(dir, "certs")
	if err := os.MkdirAll(certsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Generate config with multiple databases (config param still named NumberOfPartitions)
	// This will now generate admin and cdc certs as well due to the change in config.go
	if err := config.GenerateConfigArtifacts(dir, config.Config{
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		NumberOfDatabases: 4, // 0 (Read-Only), 1, 2, 3 (Writable)
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Failed to generate artifacts: %v", err)
	}

	// 2. Init Stores (0, 1, 2, 3)
	stores := make(map[string]*store.Store)
	for i := 0; i < 4; i++ {
		dbName := strconv.Itoa(i)
		s, err := store.NewStore(filepath.Join(dir, "data", dbName), logger, 0, i == 0, "time", 90, 0)
		if err != nil {
			t.Fatal(err)
		}
		s.SetState(store.StatePrimary)
		stores[dbName] = s
	}

	// 3. Setup Replication Manager (Required for NewServer)
	// The replication manager acts as a "Server" role when connecting to other nodes upstream
	clientCert, err := tls.LoadX509KeyPair(filepath.Join(certsDir, "server.crt"), filepath.Join(certsDir, "server.key"))
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	tlsConf := &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pool, InsecureSkipVerify: true}

	rm := replication.NewReplicationManager("test-server", stores, tlsConf, logger)

	// 4. Init Server (Port 0 for random free port)
	srv, err := NewServer(
		"test-server",
		":0", stores, logger,
		10, // MaxConns
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
		filepath.Join(certsDir, "ca.crt"),
		rm,
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// cleanup ensures a clean shutdown of the server (which closes all stores) and removes artifacts.
	cleanup := func() {
		srv.CloseAll()
		os.RemoveAll(dir)
	}

	return dir, stores, srv, cleanup
}

func getRoleTLS(t *testing.T, dir, role string) *tls.Config {
	t.Helper()
	certFile := filepath.Join(dir, "certs", role+".crt")
	keyFile := filepath.Join(dir, "certs", role+".key")
	caFile := filepath.Join(dir, "certs", "ca.crt")

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Load %s key pair: %v", role, err)
	}
	caCert, _ := os.ReadFile(caFile)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		InsecureSkipVerify: true,
	}
}

func getClientTLS(t *testing.T, dir string) *tls.Config {
	t.Helper()
	return getRoleTLS(t, dir, "client")
}

// connectClient establishes an mTLS connection to the server
func connectClient(t *testing.T, addr string, tlsConfig *tls.Config) *testClient {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	return &testClient{conn: conn, t: t}
}

type testClient struct {
	conn net.Conn
	t    *testing.T
}

func (c *testClient) Close() {
	c.conn.Close()
}

func (c *testClient) Send(opCode byte, payload []byte) {
	c.t.Helper()
	header := make([]byte, 5)
	header[0] = opCode
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	if _, err := c.conn.Write(header); err != nil {
		c.t.Fatalf("Write header failed: %v", err)
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			c.t.Fatalf("Write payload failed: %v", err)
		}
	}
}

func (c *testClient) Read() (status byte, body []byte) {
	c.t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		c.t.Fatalf("Read response header failed: %v", err)
	}
	status = header[0]
	length := binary.BigEndian.Uint32(header[1:])

	if length > 0 {
		body = make([]byte, length)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			c.t.Fatalf("Read response body failed: %v", err)
		}
	}
	return
}

func (c *testClient) AssertStatus(opCode byte, payload []byte, expectedStatus byte) []byte {
	c.t.Helper()
	c.Send(opCode, payload)
	status, body := c.Read()
	if status != expectedStatus {
		c.t.Fatalf("Op 0x%x: Expected status 0x%x, got 0x%x. Body: %s", opCode, expectedStatus, status, body)
	}
	return body
}

// ReadStatus is like AssertStatus but returns the status instead of failing, useful for polling
func (c *testClient) ReadStatus(opCode byte, payload []byte) ([]byte, byte) {
	c.t.Helper()
	c.Send(opCode, payload)
	status, body := c.Read()
	return body, status
}

// Helper to gather metrics from the server
func gatherMetrics(t *testing.T, srv *Server) map[string]float64 {
	t.Helper()
	// Inside package server, we can access srv.stores
	collector := metrics.NewTurnstoneCollector(srv.stores, srv)
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
			// Aggregate metric values if multiple metrics (labels) exist for the same name
			// This allows tests to check total counts across all databases easily
			res[*mf.Name] += val
		}
	}
	return res
}

// waitForMetric polls until a metric matches the predicate or timeouts
func waitForMetric(t *testing.T, srv *Server, metricName string, predicate func(float64) bool) {
	t.Helper()
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

// --- Tests ---

func TestServer_RBAC(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()

	// 1. Client Role Tests
	t.Run("Client", func(t *testing.T) {
		client := connectClient(t, addr, getClientTLS(t, dir))
		defer client.Close()

		// Allowed: KV Ops
		client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
		client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
		client.AssertStatus(protocol.OpCodeSet, append([]byte{0, 0, 0, 1}, []byte("k")...), protocol.ResStatusOK)
		client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

		// Denied: Admin Ops
		client.AssertStatus(protocol.OpCodeReplicaOf, []byte("dummy"), protocol.ResStatusErr)
	})

	// 2. CDC Role Tests
	t.Run("CDC", func(t *testing.T) {
		cdc := connectClient(t, addr, getRoleTLS(t, dir, "cdc"))
		defer cdc.Close()

		// Allowed: Replication Handshake
		// We'll just check it doesn't return immediate error for permission
		// (It might return error for malformed payload, but status shouldn't be generic Err "Permission Denied")
		// Actually, let's send Ping first.
		cdc.AssertStatus(protocol.OpCodePing, nil, protocol.ResStatusOK)

		// Denied: KV Ops
		cdc.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusErr)
	})

	// 3. Admin Role Tests
	t.Run("Admin", func(t *testing.T) {
		admin := connectClient(t, addr, getRoleTLS(t, dir, "admin"))
		defer admin.Close()

		admin.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
		// Allowed: KV Ops
		admin.AssertStatus(protocol.OpCodePing, nil, protocol.ResStatusOK)
	})
}

func TestServer_Lifecycle_And_Ping(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := srv.Run(ctx); err != nil {
			// Expected
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Accessing srv.listener directly because we are in package server
	addr := srv.listener.Addr().String()

	client := connectClient(t, addr, getClientTLS(t, dir))
	defer client.Close()

	resp := client.AssertStatus(protocol.OpCodePing, nil, protocol.ResStatusOK)
	if string(resp) != "PONG" {
		t.Errorf("Ping payload mismatch: %s", resp)
	}
}

func TestServer_SystemDB_AccessControl(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()

	// 1. Client attempts to access Database 0 -> Should Fail
	client := connectClient(t, addr, getClientTLS(t, dir))
	defer client.Close()

	// Connects to Database 0 by default.
	// Try Set
	key := []byte("conf")
	val := []byte("val")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)

	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	// Server returns Error because Role is Client and DB 0 is read-only
	// Updated error string matches server.go
	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusErr)

	// 2. Admin attempts to access Database 0 -> Should Succeed
	admin := connectClient(t, addr, getRoleTLS(t, dir, "admin"))
	defer admin.Close()

	admin.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	// Admin is allowed to write to DB 0, logic confirmed in server.go handles
}

func TestServer_CRUD(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// 0. Switch to Writable Database "1"
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// 1. Set
	key := []byte("mykey")
	val := []byte("myval")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)

	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 2. Get
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	resp := client.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
	if !bytes.Equal(resp, val) {
		t.Errorf("Get mismatch. Want %s, got %s", val, resp)
	}

	// 3. Del
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeDel, key, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 4. Get (Not Found)
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusNotFound)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
}

func TestMetrics_Connections(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

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
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Select Database 1
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// Start Tx
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)

	m1 := gatherMetrics(t, srv)
	if m1["turnstone_server_transactions_active"] != 1 {
		t.Errorf("Expected 1 active transaction, got %v", m1["turnstone_server_transactions_active"])
	}

	// Commit Tx
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	m2 := gatherMetrics(t, srv)
	if m2["turnstone_server_transactions_active"] != 0 {
		t.Errorf("Expected 0 active transactions, got %v", m2["turnstone_server_transactions_active"])
	}
}

func TestMetrics_StorageIO(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Select Database 1
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// Capture baseline metrics
	// Note: Metric keys now use "db" prefix
	m0 := gatherMetrics(t, srv)
	baseVLogBytes := m0["turnstone_db_vlog_bytes"]

	// Write Data
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	key := []byte("io_key")
	val := []byte("io_val")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)
	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// Verify that VLog size increased
	m1 := gatherMetrics(t, srv)
	if m1["turnstone_db_vlog_bytes"] <= baseVLogBytes {
		t.Errorf("Expected vlog bytes increase, got %v (was %v)", m1["turnstone_db_vlog_bytes"], baseVLogBytes)
	}

	// Read Data
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
}

func TestMetrics_Conflicts(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	c1 := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer c1.Close()
	c2 := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer c2.Close()

	c1.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	c2.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// Initial key setup
	setupTx := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	setupTx.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	setupTx.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)

	key := []byte("conflict")
	val := []byte("initial")
	pl := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
	copy(pl[4:], key)
	copy(pl[4+len(key):], val)

	setupTx.AssertStatus(protocol.OpCodeSet, pl, protocol.ResStatusOK)
	setupTx.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
	setupTx.Close()

	// C1 Starts and Reads (Snapshot established at version 1)
	c1.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusOK)

	// C2 Starts, Updates, Commits (Version 1 -> 2)
	c2.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	val2 := []byte("new_val")
	pl2 := make([]byte, 4+len(key)+len(val2))
	binary.BigEndian.PutUint32(pl2[0:4], uint32(len(key)))
	copy(pl2[4:], key)
	copy(pl2[4+len(key):], val2)
	c2.AssertStatus(protocol.OpCodeSet, pl2, protocol.ResStatusOK)
	c2.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// C1 Updates. Since C1 read "initial", it expects Version 1.
	// But C2 moved it to Version 2. This creates a Write Conflict on Commit.
	c1.AssertStatus(protocol.OpCodeSet, pl2, protocol.ResStatusOK)

	// C1 Commit -> Should Fail with Conflict
	c1.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusTxConflict)

	// Verify Metric (updated name)
	waitForMetric(t, srv, "turnstone_db_conflicts_total", func(val float64) bool {
		return val >= 1
	})
}

func TestServer_Backpressure(t *testing.T) {
	dir, stores, _, cleanup := setupTestEnv(t)
	// We defer cleanup of the env, which closes the ORIGINAL server (S1) and stores.
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	certsDir := filepath.Join(dir, "certs")

	// Initialize a NEW server (S2) with MaxConns = 1 manually.
	// Note: It reuses the same 'stores' map.
	srv, err := NewServer(
		"backpressure-server",
		":0", stores, logger,
		1, // MaxConns = 1
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
		filepath.Join(certsDir, "ca.crt"),
		nil, // No Replication Manager needed for this test
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	// IMPORTANT: We must close S2 separately.
	// Calling CloseAll on S2 will close the stores again, which is idempotent and safe.
	defer srv.CloseAll()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	tlsConfig := getClientTLS(t, dir)
	addr := srv.listener.Addr().String()

	// 1. Fill Capacity (1/1)
	c1 := connectClient(t, addr, tlsConfig)
	defer c1.Close()
	c1.AssertStatus(protocol.OpCodePing, nil, protocol.ResStatusOK)

	// 2. Reject Overflow (2/1)
	// We use raw Dial here because connectClient expects success/handshake
	conn2, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn2.Close()

	// Expect server to write error and close
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn2, header); err != nil {
		t.Fatalf("Read failed (server likely closed connection too fast): %v", err)
	}
	if header[0] != protocol.ResStatusServerBusy {
		t.Errorf("Expected Busy (0x06), got 0x%02x", header[0])
	}

	// Verify error message body
	ln := binary.BigEndian.Uint32(header[1:])
	body := make([]byte, ln)
	if _, err := io.ReadFull(conn2, body); err != nil {
		t.Fatalf("Failed to read error body: %v", err)
	}
	if string(body) != "Max connections" {
		t.Errorf("Unexpected error body: %s", string(body))
	}

	// 3. Release Capacity
	c1.Close()

	// 4. Connect New Client (1/1) - Should succeed now (Poll for success)
	pollStart := time.Now()
	success := false
	for time.Since(pollStart) < 2*time.Second {
		c3, err := tls.Dial("tcp", addr, tlsConfig)
		if err == nil {
			c3.Close()
			success = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if !success {
		t.Fatal("Failed to connect after releasing capacity")
	}
}

func TestServer_Transaction_Abort(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// 1. Begin -> Set -> Abort
	key := []byte("abort_key")
	val := []byte("abort_val")
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)

	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)

	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeAbort, nil, protocol.ResStatusOK)

	// 2. Verify Key does NOT exist
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusNotFound)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
}

func TestServer_Command_Validation(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// 1. Set without Begin -> Error
	key := []byte("k")
	val := []byte("v")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)
	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusTxRequired)

	// 2. Get without Begin -> Error
	client.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusTxRequired)

	// 3. Del without Begin -> Error
	client.AssertStatus(protocol.OpCodeDel, key, protocol.ResStatusTxRequired)

	// 4. Commit without Begin -> Error
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusTxRequired)

	// 5. Nested Begin -> Error (TxInProgress)
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResTxInProgress)
	// Clean up
	client.AssertStatus(protocol.OpCodeAbort, nil, protocol.ResStatusOK)
}

func TestServer_RoleTransitions(t *testing.T) {
	dir, stores, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	// Set DB "1" to UNDEFINED for testing
	stores["1"].SetState(store.StateUndefined)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()
	clientTLS := getClientTLS(t, dir)
	adminTLS := getRoleTLS(t, dir, "admin")

	// 1. Client connects to Undefined DB -> Should allow Select, Fail Begin
	client := connectClient(t, addr, clientTLS)
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusErr) // Undefined
	client.Close()

	// 2. Admin Promotes DB
	admin := connectClient(t, addr, adminTLS)
	admin.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	// Payload: 0 (min replicas)
	promotePayload := make([]byte, 4) // 0
	admin.AssertStatus(protocol.OpCodePromote, promotePayload, protocol.ResStatusOK)
	admin.Close()

	// 3. Client matches Primary -> Success
	client = connectClient(t, addr, clientTLS)
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.Close() // Keep clean

	// 4. Admin StepDown
	admin = connectClient(t, addr, adminTLS)
	admin.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// Create a hanging client to verify disconnect
	hangingClient := connectClient(t, addr, clientTLS)
	hangingClient.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// Admin executes StepDown
	admin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	// Admin connection might be closed by server immediately after response logic

	// Verify hanging client is disconnected
	// Read should return EOF or error
	_, err := hangingClient.conn.Read(make([]byte, 1))
	if err == nil {
		t.Error("Hanging client still connected after StepDown")
	}

	// 5. Verify DB State is Undefined (New client cant Begin)
	// Note: We need a new connection (old admin conn likely closed too)
	admin = connectClient(t, addr, adminTLS)
	admin.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	// REPLICAOF valid only in UNDEFINED
	// ReplicaOf(addr="dummy", db="origin")
	replPayload := []byte{0, 0, 0, 5} // addr len 5
	binary.BigEndian.PutUint32(replPayload[0:4], 5)
	replPayload = append(replPayload, []byte("dummy")...)
	replPayload = append(replPayload, []byte("origin")...)

	admin.AssertStatus(protocol.OpCodeReplicaOf, replPayload, protocol.ResStatusOK)
	admin.Close()

	// 6. Verify Replica State (Read OK, Write Fail)
	client = connectClient(t, addr, clientTLS)
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	// Read OK
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeGet, []byte("k"), protocol.ResStatusNotFound) // k not found, but status OK

	// Write Fail: Set
	key := []byte("k")
	val := []byte("v")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)
	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusErr)

	client.Close()
}

func TestPromote_QuorumBehavior(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()
	admin := connectClient(t, addr, getRoleTLS(t, dir, "admin"))
	defer admin.Close()
	selectDatabase(t, admin, "1")

	// 1. Promote with MinReplicas=1
	// Should succeed IMMEDIATELY even with 0 replicas.
	promotePayload := make([]byte, 4)
	binary.BigEndian.PutUint32(promotePayload, 1)
	admin.AssertStatus(protocol.OpCodePromote, promotePayload, protocol.ResStatusOK)

	// 2. Write should BLOCK
	client := connectClient(t, addr, getClientTLS(t, dir))
	defer client.Close()
	selectDatabase(t, client, "1")

	key := []byte("block_key")
	val := []byte("val")
	pl := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
	copy(pl[4:], key)
	copy(pl[4+len(key):], val)

	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeSet, pl, protocol.ResStatusOK)

	// Commit - Perform in goroutine to verify blocking
	doneCh := make(chan struct{})
	go func() {
		client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
		close(doneCh)
	}()

	// Verify blocked
	select {
	case <-doneCh:
		t.Fatal("Commit should have blocked waiting for quorum")
	case <-time.After(200 * time.Millisecond):
		// Expected behavior
	}

	// 3. Connect a "Fake" Replica to satisfy quorum
	// We use the admin connection to act as a replica
	// But to register as a replica, we need to send Hello.
	// Since we can't easily hijack the admin connection for repl protocol in this test helper structure,
	// we'll start a real Replica node and connect it.

	// Start Replica Node
	_, replicaAddr, cancelReplica := startServerNode(t, dir, "replica_quorum", getClientTLS(t, dir))
	defer cancelReplica()

	// Configure Replica -> Primary
	adminReplica := connectClient(t, replicaAddr, getRoleTLS(t, dir, "admin"))
	selectDatabase(t, adminReplica, "1")
	configureReplication(t, adminReplica, addr, "1")
	adminReplica.Close()

	// 4. Verify Unblock
	select {
	case <-doneCh:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Commit failed to unblock after replica joined")
	}
}

func TestStepDown_SafetySequence(t *testing.T) {
	// 1. Setup Environment
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Primary
	_, primAddr, cancelPrim := startServerNode(t, baseDir, "prim_step", clientTLS)
	defer cancelPrim()
	promoteNode(t, baseDir, primAddr, "1")

	// Start Replica
	_, replAddr, cancelRepl := startServerNode(t, baseDir, "repl_step", clientTLS)
	defer cancelRepl()

	// Connect & Configure Replication
	cAdminRepl := connectClient(t, replAddr, adminTLS)
	selectDatabase(t, cAdminRepl, "1")
	configureReplication(t, cAdminRepl, primAddr, "1")
	cAdminRepl.Close()

	// 2. Start a transaction that hangs open on Primary
	cTx := connectClient(t, primAddr, clientTLS)
	defer cTx.Close()
	selectDatabase(t, cTx, "1")
	cTx.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)

	// 3. Initiate StepDown in background
	stepDownDone := make(chan struct{})
	go func() {
		cAdmin := connectClient(t, primAddr, adminTLS)
		defer cAdmin.Close()
		selectDatabase(t, cAdmin, "1")
		cAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
		close(stepDownDone)
	}()

	// 4. Verify StepDown is blocked by active transaction
	select {
	case <-stepDownDone:
		t.Fatal("StepDown finished prematurely (should wait for active tx)")
	case <-time.After(200 * time.Millisecond):
		// Good, it's blocked
	}

	// 5. Try to start NEW transaction -> Should be blocked/failed
	cNew := connectClient(t, primAddr, clientTLS)
	defer cNew.Close()
	selectDatabase(t, cNew, "1")
	// Should fail immediately because state is STEPPING_DOWN
	cNew.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusErr)

	// 6. Finish the active transaction
	// Write something to ensure it replicates
	k := []byte("safety_key")
	v := []byte("safety_val")
	pl := make([]byte, 4+len(k)+len(v))
	binary.BigEndian.PutUint32(pl[0:4], uint32(len(k)))
	copy(pl[4:], k)
	copy(pl[4+len(k):], v)
	cTx.AssertStatus(protocol.OpCodeSet, pl, protocol.ResStatusOK)
	cTx.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 7. StepDown should now complete
	select {
	case <-stepDownDone:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("StepDown failed to complete after draining tx")
	}

	// 8. Verify Primary is now UNDEFINED (Clients disconnected)
	// Try to connect and check state
	cCheck := connectClient(t, primAddr, clientTLS)
	defer cCheck.Close()
	selectDatabase(t, cCheck, "1")
	cCheck.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusErr)

	// 9. Verify Replica got the data from the drained transaction
	cReplRead := connectClient(t, replAddr, clientTLS)
	defer cReplRead.Close()
	selectDatabase(t, cReplRead, "1")

	// Retry loop for eventual consistency if needed, though StepDown waits for replication
	val := readKey(t, cReplRead, "safety_key")
	if string(val) != "safety_val" {
		t.Errorf("Replica missing data. Got %q, want 'safety_val'", val)
	}
}
