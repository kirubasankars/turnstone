package main

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
	"strings"
	"testing"
	"time"
)

// --- Test Infrastructure & Helpers ---

func setupTestEnv(t *testing.T) (string, map[string]*Store, *Server) {
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
	// Generate config with multiple databases to support testing DB 1, 2, 3
	if err := GenerateConfigArtifacts(dir, Config{
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		NumberOfDatabases: 4, // 0 (Read-Only), 1, 2, 3 (Writable)
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Failed to generate artifacts: %v", err)
	}

	// 2. Init Stores (0, 1, 2)
	stores := make(map[string]*Store)
	for i := 0; i < 4; i++ {
		dbName := strconv.Itoa(i)
		store, err := NewStore(filepath.Join(dir, "data", dbName), logger, true, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		stores[dbName] = store
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

	rm := NewReplicationManager(stores, tlsConf, logger)

	// 4. Init Server (Port 0 for random free port)
	srv, err := NewServer(
		":0", "", stores, logger,
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

// --- Tests ---

func TestServer_Lifecycle_And_Ping(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := srv.Run(ctx); err != nil {
			// Expected
		}
	}()

	time.Sleep(100 * time.Millisecond)
	addr := srv.listener.Addr().String()

	client := connectClient(t, addr, getClientTLS(t, dir))
	defer client.Close()

	resp := client.AssertStatus(OpCodePing, nil, ResStatusOK)
	if string(resp) != "PONG" {
		t.Errorf("Ping payload mismatch: %s", resp)
	}
}

func TestServer_ReadOnlySystemDB(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// 1. Connection starts at DB "0" by default
	// 2. Attempt to Write (Should fail)
	key := []byte("k")
	val := []byte("v")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	// Server returns Error on Set to DB 0
	client.AssertStatus(OpCodeSet, setPayload, ResStatusErr)
}

func TestServer_CRUD(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// 0. Switch to Writable DB "1"
	client.AssertStatus(OpCodeSelect, []byte("1"), ResStatusOK)

	// 1. Set
	key := []byte("mykey")
	val := []byte("myval")
	setPayload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(key)))
	copy(setPayload[4:], key)
	copy(setPayload[4+len(key):], val)

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeSet, setPayload, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// 2. Get
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	resp := client.AssertStatus(OpCodeGet, key, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)
	if !bytes.Equal(resp, val) {
		t.Errorf("Get mismatch. Want %s, got %s", val, resp)
	}

	// 3. Del
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeDel, key, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// 4. Get (Not Found)
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeGet, key, ResStatusNotFound)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)
}

func TestServer_SelectDB(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// 1. Select DB "1" -> Set K=V1
	client.AssertStatus(OpCodeSelect, []byte("1"), ResStatusOK)

	k := []byte("k")
	v1 := []byte("v1")
	setPl1 := make([]byte, 4+len(k)+len(v1))
	binary.BigEndian.PutUint32(setPl1[:4], uint32(len(k)))
	copy(setPl1[4:], k)
	copy(setPl1[4+len(k):], v1)

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeSet, setPl1, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// 2. Select DB "2"
	client.AssertStatus(OpCodeSelect, []byte("2"), ResStatusOK)

	// 3. Verify Empty in DB "2"
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeGet, k, ResStatusNotFound)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// 4. DB "2": Set K=V2
	v2 := []byte("v2")
	setPl2 := make([]byte, 4+len(k)+len(v2))
	binary.BigEndian.PutUint32(setPl2[:4], uint32(len(k)))
	copy(setPl2[4:], k)
	copy(setPl2[4+len(k):], v2)

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeSet, setPl2, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// 5. Select DB "1" again
	client.AssertStatus(OpCodeSelect, []byte("1"), ResStatusOK)

	// 6. Verify V1 in DB "1"
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	resp := client.AssertStatus(OpCodeGet, k, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)
	if !bytes.Equal(resp, v1) {
		t.Errorf("DB switch fail. Want v1, got %s", resp)
	}
}

func TestServer_AdminOps(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Stats
	body := client.AssertStatus(OpCodeStat, nil, ResStatusOK)
	if len(body) == 0 {
		t.Error("Stats body empty")
	}

	// Compact - requires selecting a DB first implicitly (default 0)
	// Compact on read-only DB 0 is technically allowed as it is a maintenance op, not a client write.
	client.AssertStatus(OpCodeCompact, nil, ResStatusOK)
}

func TestServer_ErrorHandling_Protocol(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Unknown OpCode
	client.AssertStatus(0x99, nil, ResStatusErr)
}

func TestServer_Backpressure(t *testing.T) {
	dir, stores, _ := setupTestEnv(t)
	defer stores["0"].Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	certsDir := filepath.Join(dir, "certs")

	// Initialize server with MaxConns = 1 manually to override setupTestEnv default
	srv, err := NewServer(
		":0", "", stores, logger,
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
	c1.AssertStatus(OpCodePing, nil, ResStatusOK)

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
	if header[0] != ResStatusServerBusy {
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

// TestServer_ProtectedKeys verifies that clients cannot modify keys starting with "_".
func TestServer_ProtectedKeys(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["0"].Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Select DB 1
	client.AssertStatus(OpCodeSelect, []byte("1"), ResStatusOK)

	// Helper to create SET payload
	createSetPayload := func(k, v string) []byte {
		keyBytes := []byte(k)
		valBytes := []byte(v)
		buf := make([]byte, 4+len(keyBytes)+len(valBytes))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
		copy(buf[4:], keyBytes)
		copy(buf[4+len(keyBytes):], valBytes)
		return buf
	}

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)

	// 1. Try to SET a protected key
	// Should fail with ResStatusErr
	resp := client.AssertStatus(OpCodeSet, createSetPayload("_id", "hacked"), ResStatusErr)
	if !strings.Contains(string(resp), "read-only") {
		t.Errorf("Expected read-only error for protected key, got: %s", string(resp))
	}

	// 2. Try to DEL a protected key
	// Should fail with ResStatusErr
	resp = client.AssertStatus(OpCodeDel, []byte("_meta"), ResStatusErr)
	if !strings.Contains(string(resp), "read-only") {
		t.Errorf("Expected read-only error for protected key deletion, got: %s", string(resp))
	}

	// 3. Verify normal keys still work
	client.AssertStatus(OpCodeSet, createSetPayload("normal", "ok"), ResStatusOK)

	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)
}

// TestServer_ReplicaReadOnly verifies that a database in replica mode rejects writes
// but accepts compaction.
func TestServer_ReplicaReadOnly(t *testing.T) {
	// 1. Setup Environment with 2 nodes (Primary, Replica)
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Primary
	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_ro", tlsConfig)
	defer cancelPrimary()

	// Start Replica
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_ro", tlsConfig)
	defer cancelReplica()

	// Connect Client to Replica
	cReplica := connectClient(t, replicaAddr, tlsConfig)
	selectDB(t, cReplica, "1")

	// 2. Configure Replication (Replica -> Primary)
	configureReplication(t, cReplica, primaryAddr, "1")

	// Wait briefly for replication state to be registered in memory
	time.Sleep(200 * time.Millisecond)

	cReplica.AssertStatus(OpCodeBegin, nil, ResStatusOK)

	// 3. Attempt Write (SET) on Replica
	// Should fail because it is now a replica
	key := []byte("k")
	val := []byte("v")
	pl := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
	copy(pl[4:], key)
	copy(pl[4+len(key):], val)

	resp := cReplica.AssertStatus(OpCodeSet, pl, ResStatusErr)
	if !strings.Contains(string(resp), "Replica database is read-only") {
		t.Errorf("Expected replica read-only error, got: %s", string(resp))
	}

	// 4. Attempt Delete (DEL) on Replica
	resp = cReplica.AssertStatus(OpCodeDel, key, ResStatusErr)
	if !strings.Contains(string(resp), "Replica database is read-only") {
		t.Errorf("Expected replica read-only error for DEL, got: %s", string(resp))
	}

	// Abort the write transaction before attempting administrative operations
	cReplica.AssertStatus(OpCodeAbort, nil, ResStatusOK)
	cReplica.Close() // Close connection to ensure transaction state is definitely cleared

	// 5. Attempt Compact on Replica (New Connection)
	// Should SUCCEED (Compaction is allowed maintenance)
	cReplicaAdmin := connectClient(t, replicaAddr, tlsConfig)
	defer cReplicaAdmin.Close()
	selectDB(t, cReplicaAdmin, "1")
	cReplicaAdmin.AssertStatus(OpCodeCompact, nil, ResStatusOK)

	// 6. Stop Replication
	// REPLICAOF NO ONE
	// Payload: [0][][DBName]
	dbBytes := []byte("1")
	stopPl := make([]byte, 4+0+len(dbBytes))
	binary.BigEndian.PutUint32(stopPl[0:4], 0)
	copy(stopPl[4:], dbBytes)
	cReplicaAdmin.AssertStatus(OpCodeReplicaOf, stopPl, ResStatusOK)

	// Wait for state update
	time.Sleep(200 * time.Millisecond)

	// 7. Attempt Write again
	// Should SUCCEED now that it is no longer a replica
	cReplicaAdmin.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	cReplicaAdmin.AssertStatus(OpCodeSet, pl, ResStatusOK)
	cReplicaAdmin.AssertStatus(OpCodeCommit, nil, ResStatusOK)
}
