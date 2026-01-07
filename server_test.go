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
	"testing"
	"time"
)

// --- Test Infrastructure & Helpers ---

func setupTestEnv(t *testing.T) (string, map[string]*Store, *Server) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Generate Certs
	certsDir := filepath.Join(dir, "certs")
	if err := os.MkdirAll(certsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := GenerateConfigArtifacts(dir, Config{
		TLSCertFile: "certs/server.crt",
		TLSKeyFile:  "certs/server.key",
		TLSCAFile:   "certs/ca.crt",
		Databases:   []DatabaseConfig{{Name: "default"}},
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Failed to generate artifacts: %v", err)
	}

	// 2. Init Store (default)
	store, err := NewStore(filepath.Join(dir, "data", "default"), logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	stores := map[string]*Store{"default": store}

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
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

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

func TestServer_CRUD(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

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
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	// Create a second DB
	db2Dir := filepath.Join(dir, "db2")
	store2, _ := NewStore(db2Dir, srv.logger, true, 0, true)
	srv.stores["db2"] = store2
	defer store2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	client := connectClient(t, srv.listener.Addr().String(), getClientTLS(t, dir))
	defer client.Close()

	// Default DB: Set K=V1
	k := []byte("k")
	v1 := []byte("v1")
	setPl1 := make([]byte, 4+len(k)+len(v1))
	binary.BigEndian.PutUint32(setPl1[:4], uint32(len(k)))
	copy(setPl1[4:], k)
	copy(setPl1[4+len(k):], v1)

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeSet, setPl1, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// Select DB2
	client.AssertStatus(OpCodeSelect, []byte("db2"), ResStatusOK)

	// Verify Empty in DB2
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeGet, k, ResStatusNotFound)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// DB2: Set K=V2
	v2 := []byte("v2")
	setPl2 := make([]byte, 4+len(k)+len(v2))
	binary.BigEndian.PutUint32(setPl2[:4], uint32(len(k)))
	copy(setPl2[4:], k)
	copy(setPl2[4+len(k):], v2)

	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	client.AssertStatus(OpCodeSet, setPl2, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// Select Default again
	client.AssertStatus(OpCodeSelect, []byte("default"), ResStatusOK)

	// Verify V1 in Default
	client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	resp := client.AssertStatus(OpCodeGet, k, ResStatusOK)
	client.AssertStatus(OpCodeCommit, nil, ResStatusOK)
	if !bytes.Equal(resp, v1) {
		t.Errorf("DB switch fail. Want v1, got %s", resp)
	}
}

func TestServer_AdminOps(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

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

	// Compact - requires selecting a DB first implicitly (default)
	client.AssertStatus(OpCodeCompact, nil, ResStatusOK)
}

func TestServer_ErrorHandling_Protocol(t *testing.T) {
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

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
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

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
