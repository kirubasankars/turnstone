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

func setupTestEnv(t *testing.T) (string, map[string]*store.Store, *Server) {
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
	// Generate config with multiple partitions to support testing Partition 1, 2, 3
	if err := config.GenerateConfigArtifacts(dir, config.Config{
		TLSCertFile:        "certs/server.crt",
		TLSKeyFile:         "certs/server.key",
		TLSCAFile:          "certs/ca.crt",
		NumberOfPartitions: 4, // 0 (Read-Only), 1, 2, 3 (Writable)
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Failed to generate artifacts: %v", err)
	}

	// 2. Init Stores (0, 1, 2)
	stores := make(map[string]*store.Store)
	for i := 0; i < 4; i++ {
		partitionName := strconv.Itoa(i)
		store, err := store.NewStore(filepath.Join(dir, "data", partitionName), logger, true, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		stores[partitionName] = store
	}

	// 3. Setup Replication Manager (Required for NewServer)
	clientCertPath := filepath.Join(certsDir, "client.crt")
	clientKeyPath := filepath.Join(certsDir, "client.key")
	caCertPath := filepath.Join(certsDir, "ca.crt")

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	tlsConf := &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pool, InsecureSkipVerify: true}

	rm := replication.NewReplicationManager(stores, tlsConf, logger)

	// 4. Init Server (Port 0 for random free port)
	srv, err := NewServer(
		":0", stores, logger,
		10, // MaxConns
		5*time.Second,
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
		filepath.Join(certsDir, "ca.crt"),
		rm,
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	return dir, stores, srv
}

func getClientTLS(t *testing.T, dir string) *tls.Config {
	certFile := filepath.Join(dir, "certs", "client.crt")
	keyFile := filepath.Join(dir, "certs", "client.key")
	caFile := filepath.Join(dir, "certs", "ca.crt")

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Load client key pair: %v", err)
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

// connectClient establishes an mTLS connection to the server
func connectClient(t *testing.T, addr string, tlsConfig *tls.Config) *testClient {
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
	c.Send(opCode, payload)
	status, body := c.Read()
	if status != expectedStatus {
		c.t.Fatalf("Op 0x%x: Expected status 0x%x, got 0x%x. Body: %s", opCode, expectedStatus, status, body)
	}
	return body
}

// ReadStatus is like AssertStatus but returns the status instead of failing, useful for polling
func (c *testClient) ReadStatus(opCode byte, payload []byte) ([]byte, byte) {
	c.Send(opCode, payload)
	status, body := c.Read()
	return body, status
}

// Helper to gather metrics from the server
func gatherMetrics(t *testing.T, srv *Server) map[string]float64 {
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

// --- Tests ---

func TestServer_Lifecycle_And_Ping(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()
	defer os.RemoveAll(dir)

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

func TestServer_ReadOnlySystemPartition(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// 1. Connection starts at Partition "0" by default
	// 2. Attempt to Write (Should fail)
	key := []byte("k")
	val := []byte("v")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)

	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	// Server returns Error on Set to Partition 0
	client.AssertStatus(protocol.OpCodeSet, setPayload, protocol.ResStatusErr)
}

func TestServer_CRUD(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// 0. Switch to Writable Partition "1"
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
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()
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
	defer stores["0"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Select Partition 1
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
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Select Partition 1
	client.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)

	// Capture baseline metrics (accounts for system keys)
	m0 := gatherMetrics(t, srv)
	baseKeys := m0["turnstone_store_keys_total"]

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

	m1 := gatherMetrics(t, srv)

	if m1["turnstone_store_keys_total"] != baseKeys+1 {
		t.Errorf("Expected %v keys, got %v", baseKeys+1, m1["turnstone_store_keys_total"])
	}
	if m1["turnstone_store_bytes_written_total"] <= 0 {
		t.Errorf("Expected bytes written > 0")
	}
	if m1["turnstone_store_wal_offset_bytes"] <= 0 {
		t.Errorf("Expected WAL offset > 0")
	}

	// Read Data
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeGet, key, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	m2 := gatherMetrics(t, srv)
	if m2["turnstone_store_bytes_read_total"] <= 0 {
		t.Errorf("Expected bytes read > 0")
	}
}

func TestMetrics_Conflicts(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()
	defer os.RemoveAll(dir)

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

	// Verify Metric
	waitForMetric(t, srv, "turnstone_store_conflicts_total", func(val float64) bool {
		return val >= 1
	})
}

func TestServer_Backpressure(t *testing.T) {
	dir, stores, _ := setupTestEnv(t)
	defer stores["0"].Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	certsDir := filepath.Join(dir, "certs")

	// Initialize server with MaxConns = 1 manually to override setupTestEnv default
	srv, err := NewServer(
		":0", stores, logger,
		1, // MaxConns = 1
		5*time.Second,
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
		filepath.Join(certsDir, "ca.crt"),
		nil, // No Replication Manager needed for this test
	)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

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
