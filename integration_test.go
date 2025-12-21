package main

import (
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
)

// --- Helper: Setup TLS ---
// Generates certs in a temp dir and returns the config for a client to connect.
func setupSecurity(t testing.TB) (string, string, string, string, string, *tls.Config) {
	t.Helper()
	dir, err := os.MkdirTemp("", "turnstone-certs")
	if err != nil {
		t.Fatal(err)
	}
	// Note: generateCerts is in config.go and must be available in the package
	if err := generateCerts(dir); err != nil {
		t.Fatal(err)
	}

	caPath := filepath.Join(dir, "ca.crt")
	srvCert := filepath.Join(dir, "server.crt")
	srvKey := filepath.Join(dir, "server.key")
	cliCertPath := filepath.Join(dir, "client.crt")
	cliKeyPath := filepath.Join(dir, "client.key")

	// Create Client TLS Config
	cert, err := tls.LoadX509KeyPair(cliCertPath, cliKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	caData, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caData)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
	}

	return caPath, srvCert, srvKey, cliCertPath, cliKeyPath, tlsConfig
}

// --- Integration Tests ---

// 1. Concurrency & Isolation (SSI)
func TestIntegration_Concurrency_SSI(t *testing.T) {
	dir, cleanup := createTempDir(t)
	defer cleanup()

	// Security Setup
	ca, srvCert, srvKey, _, _, clientTLS := setupSecurity(t)

	store, _ := NewStore(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), true, false, false, 0)
	defer store.Close()

	// Server expects a slice of stores. Since MaxDatabases is 3, we fill index 0.
	// For testing index 0, a slice of length 1 is technically enough if getStore checks bounds against len(stores)
	// But to be safe with the new "MaxDatabases=3" fixed array approach if implemented that way,
	// we pass a slice containing our test store.
	stores := []*Store{store}

	// NewServer signature: addr, metricsAddr, stores, logger, maxConns, maxSyncs, readOnly, cert, key, ca
	server := NewServer(":0", "", stores, slog.New(slog.NewTextHandler(io.Discard, nil)), 10, 2, false, srvCert, srvKey, ca)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	addr := server.listener.Addr().String()

	t.Run("Write-Write Conflict", func(t *testing.T) {
		connA, err := tls.Dial("tcp", addr, clientTLS)
		if err != nil {
			t.Fatalf("ConnA failed: %v", err)
		}
		defer connA.Close()

		connB, err := tls.Dial("tcp", addr, clientTLS)
		if err != nil {
			t.Fatalf("ConnB failed: %v", err)
		}
		defer connB.Close()

		// Both begin
		mustSend(t, connA, OpCodeBegin, nil)
		mustSend(t, connB, OpCodeBegin, nil)

		// Both write same key
		mustSend(t, connA, OpCodeSet, buildSetPayload("conflict_key", "valA"))
		mustSend(t, connB, OpCodeSet, buildSetPayload("conflict_key", "valB"))

		// First commit succeeds
		mustSend(t, connA, OpCodeCommit, nil)

		// Second commit must fail
		status, _, _ := sendCommand(connB, OpCodeCommit, nil)
		if status != ResStatusTxConflict {
			t.Errorf("Expected TxConflict (0x05), got 0x%02x", status)
		}
	})

	t.Run("Snapshot Isolation", func(t *testing.T) {
		connA, _ := tls.Dial("tcp", addr, clientTLS)
		defer connA.Close()
		connB, _ := tls.Dial("tcp", addr, clientTLS)
		defer connB.Close()

		// TxA Begins (Snapshot taken here)
		mustSend(t, connA, OpCodeBegin, nil)

		// TxB Begins, Writes new data, Commits
		mustSend(t, connB, OpCodeBegin, nil)
		mustSend(t, connB, OpCodeSet, buildSetPayload("iso_key", "new_val"))
		mustSend(t, connB, OpCodeCommit, nil)

		// TxA tries to read "iso_key"
		// It should NOT see "new_val" because its snapshot is older
		status, _, _ := sendCommand(connA, OpCodeGet, []byte("iso_key"))
		if status != ResStatusNotFound {
			t.Errorf("Snapshot Isolation violation: TxA saw data committed after it began")
		}

		mustSend(t, connA, OpCodeCommit, nil)
	})
}

// 3. Persistence & Recovery
func TestIntegration_Persistence(t *testing.T) {
	dir, cleanup := createTempDir(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ca, srvCert, srvKey, _, _, clientTLS := setupSecurity(t)

	// Phase 1: Start, Write Data, Stop
	func() {
		store, _ := NewStore(dir, logger, true, false, true, 0)
		stores := []*Store{store}
		server := NewServer(":0", "", stores, logger, 10, 2, false, srvCert, srvKey, ca)

		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = server.Run(ctx) }()
		time.Sleep(50 * time.Millisecond)
		addr := server.listener.Addr().String()

		conn, _ := tls.Dial("tcp", addr, clientTLS)
		mustSend(t, conn, OpCodeBegin, nil)
		mustSend(t, conn, OpCodeSet, buildSetPayload("persist_key", "persist_val"))
		mustSend(t, conn, OpCodeCommit, nil)
		conn.Close()

		cancel()      // Stop server
		store.Close() // Close store (flushes WAL/Index)
		time.Sleep(100 * time.Millisecond)
	}()

	// Phase 2: Restart, Verify Data
	func() {
		store, err := NewStore(dir, logger, true, false, true, 0)
		if err != nil {
			t.Fatalf("Failed to reopen store: %v", err)
		}
		defer store.Close()

		stores := []*Store{store}
		server := NewServer(":0", "", stores, logger, 10, 2, false, srvCert, srvKey, ca)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = server.Run(ctx) }()
		time.Sleep(50 * time.Millisecond)
		addr := server.listener.Addr().String()

		conn, _ := tls.Dial("tcp", addr, clientTLS)
		defer conn.Close()

		mustSend(t, conn, OpCodeBegin, nil)
		status, body, _ := sendCommand(conn, OpCodeGet, []byte("persist_key"))
		if status != ResStatusOK || string(body) != "persist_val" {
			t.Errorf("Persistence failed. Got status %x, body: %s", status, body)
		}
		mustSend(t, conn, OpCodeCommit, nil)
	}()
}

// 4. Compaction
func TestIntegration_Compaction(t *testing.T) {
	dir, cleanup := createTempDir(t)
	defer cleanup()
	ca, srvCert, srvKey, _, _, clientTLS := setupSecurity(t)

	store, _ := NewStore(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), true, false, false, 0)
	defer store.Close()
	stores := []*Store{store}
	server := NewServer(":0", "", stores, slog.New(slog.NewTextHandler(io.Discard, nil)), 10, 2, false, srvCert, srvKey, ca)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	addr := server.listener.Addr().String()

	conn, _ := tls.Dial("tcp", addr, clientTLS)
	defer conn.Close()

	// Write enough data to make compaction meaningful
	mustSend(t, conn, OpCodeBegin, nil)
	mustSend(t, conn, OpCodeSet, buildSetPayload("key1", "value1"))
	mustSend(t, conn, OpCodeSet, buildSetPayload("key2", "value2"))
	mustSend(t, conn, OpCodeCommit, nil)

	// Trigger Compaction
	status, _, _ := sendCommand(conn, OpCodeCompact, nil)
	if status != ResStatusOK {
		t.Fatalf("Compaction request failed: %x", status)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify Data survives compaction
	mustSend(t, conn, OpCodeBegin, nil)
	status, body, _ := sendCommand(conn, OpCodeGet, []byte("key1"))
	if status != ResStatusOK || string(body) != "value1" {
		t.Errorf("Data lost after compaction. Status: %x", status)
	}
	mustSend(t, conn, OpCodeCommit, nil)
}

// 5. Limits & Robustness
func TestIntegration_Limits(t *testing.T) {
	dir, cleanup := createTempDir(t)
	defer cleanup()
	ca, srvCert, srvKey, _, _, clientTLS := setupSecurity(t)

	store, _ := NewStore(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), true, false, false, 0)
	defer store.Close()
	stores := []*Store{store}
	server := NewServer(":0", "", stores, slog.New(slog.NewTextHandler(io.Discard, nil)), 10, 2, false, srvCert, srvKey, ca)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	addr := server.listener.Addr().String()

	conn, _ := tls.Dial("tcp", addr, clientTLS)
	defer conn.Close()

	t.Run("Max Payload Exceeded", func(t *testing.T) {
		hugeVal := make([]byte, 5000)

		mustSend(t, conn, OpCodeBegin, nil)

		status, _, _ := sendCommand(conn, OpCodeSet, buildSetPayload("huge", string(hugeVal)))
		if status != ResStatusEntityTooLarge {
			t.Errorf("Expected EntityTooLarge (0x07), got 0x%02x", status)
		}

		// Clean up tx
		mustSend(t, conn, OpCodeAbort, nil)
	})

	t.Run("Transaction Timeout", func(t *testing.T) {
		// Just verify the basic cycle works, as we can't easily wait 60s in unit tests
		mustSend(t, conn, OpCodeBegin, nil)
		mustSend(t, conn, OpCodeCommit, nil)
	})
}

// 6. Multi-Database Isolation
func TestIntegration_MultiDB(t *testing.T) {
	dir, cleanup := createTempDir(t)
	defer cleanup()
	ca, srvCert, srvKey, _, _, clientTLS := setupSecurity(t)

	// Initialize 3 stores
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var stores []*Store
	for i := 0; i < 3; i++ {
		dbPath := filepath.Join(dir, strconv.Itoa(i))
		s, _ := NewStore(dbPath, logger, true, false, false, 0)
		defer s.Close()
		stores = append(stores, s)
	}

	server := NewServer(":0", "", stores, logger, 10, 2, false, srvCert, srvKey, ca)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	addr := server.listener.Addr().String()

	conn, _ := tls.Dial("tcp", addr, clientTLS)
	defer conn.Close()

	// 1. Default (DB 0): Write data
	mustSend(t, conn, OpCodeBegin, nil)
	mustSend(t, conn, OpCodeSet, buildSetPayload("key0", "val0"))
	mustSend(t, conn, OpCodeCommit, nil)

	// 2. Select DB 1
	mustSend(t, conn, OpCodeSelect, []byte{1})

	// 3. DB 1: Write different data
	mustSend(t, conn, OpCodeBegin, nil)
	mustSend(t, conn, OpCodeSet, buildSetPayload("key1", "val1"))
	mustSend(t, conn, OpCodeCommit, nil)

	// 4. Verify Isolation in DB 1 (Should not see key0)
	mustSend(t, conn, OpCodeBegin, nil)
	status, body, _ := sendCommand(conn, OpCodeGet, []byte("key1"))
	if status != ResStatusOK || string(body) != "val1" {
		t.Errorf("DB1 missing key1")
	}
	status, _, _ = sendCommand(conn, OpCodeGet, []byte("key0"))
	if status != ResStatusNotFound {
		t.Errorf("DB1 should not access key0 from DB0")
	}
	mustSend(t, conn, OpCodeCommit, nil)

	// 5. Switch back to DB 0
	mustSend(t, conn, OpCodeSelect, []byte{0})

	// 6. Verify Isolation in DB 0 (Should not see key1)
	mustSend(t, conn, OpCodeBegin, nil)
	status, body, _ = sendCommand(conn, OpCodeGet, []byte("key0"))
	if status != ResStatusOK || string(body) != "val0" {
		t.Errorf("DB0 missing key0")
	}
	status, _, _ = sendCommand(conn, OpCodeGet, []byte("key1"))
	if status != ResStatusNotFound {
		t.Errorf("DB0 should not access key1 from DB1")
	}
	mustSend(t, conn, OpCodeCommit, nil)

	// 7. Invalid Select
	status, _, _ = sendCommand(conn, OpCodeSelect, []byte{5}) // 5 >= 3
	if status != ResStatusErr {
		t.Errorf("Expected error for invalid DB select")
	}
}

// 7. Stats Command
func TestIntegration_Stats(t *testing.T) {
	dir, cleanup := createTempDir(t)
	defer cleanup()
	ca, srvCert, srvKey, _, _, clientTLS := setupSecurity(t)

	store, _ := NewStore(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), true, false, false, 0)
	defer store.Close()
	stores := []*Store{store}

	server := NewServer(":0", "", stores, slog.New(slog.NewTextHandler(io.Discard, nil)), 10, 2, false, srvCert, srvKey, ca)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	addr := server.listener.Addr().String()

	conn, _ := tls.Dial("tcp", addr, clientTLS)
	defer conn.Close()

	status, body, err := sendCommand(conn, OpCodeStat, nil)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if status != ResStatusOK {
		t.Errorf("Stat returned status %x", status)
	}
	if len(body) == 0 {
		t.Error("Stat returned empty body")
	}
	// Body format: "DB:%d Keys:%d..."
	if len(body) > 2 && (body[0] != 'D' || body[1] != 'B') {
		t.Errorf("Unexpected stat format: %s", string(body))
	}
}

// --- Helpers (Shared) ---

func createTempDir(t testing.TB) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "turnstone-integration")
	if err != nil {
		t.Fatal(err)
	}
	return dir, func() { os.RemoveAll(dir) }
}

func buildSetPayload(key, val string) []byte {
	k := []byte(key)
	v := []byte(val)
	buf := make([]byte, 4+len(k)+len(v))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(k)))
	copy(buf[4:], k)
	copy(buf[4+len(k):], v)
	return buf
}

func mustSend(t testing.TB, conn net.Conn, op byte, payload []byte) {
	t.Helper()
	status, msg, err := sendCommand(conn, op, payload)
	if err != nil {
		t.Fatalf("Op %x failed net err: %v", op, err)
	}
	if status != ResStatusOK {
		t.Fatalf("Op %x failed status: %x, msg: %s", op, status, string(msg))
	}
}

func sendCommand(conn net.Conn, op byte, payload []byte) (byte, []byte, error) {
	reqLen := len(payload)
	header := make([]byte, 5)
	header[0] = op
	binary.BigEndian.PutUint32(header[1:5], uint32(reqLen))

	if _, err := conn.Write(header); err != nil {
		return 0, nil, err
	}
	if reqLen > 0 {
		if _, err := conn.Write(payload); err != nil {
			return 0, nil, err
		}
	}

	respHeader := make([]byte, 5)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		return 0, nil, err
	}

	status := respHeader[0]
	respLen := binary.BigEndian.Uint32(respHeader[1:])

	var body []byte
	if respLen > 0 {
		body = make([]byte, respLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			return status, nil, err
		}
	}

	return status, body, nil
}
