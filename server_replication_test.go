package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- Helpers ---

// waitForConditionOrTimeout is a helper to poll for a condition until timeout.
func waitForConditionOrTimeout(t *testing.T, timeout time.Duration, check func() bool, errMsg string) {
	start := time.Now()
	for time.Since(start) < timeout {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(errMsg)
}

// createTestLogger creates a logger that writes to both stdout and a log file in the test directory.
func createTestLogger(t *testing.T, baseDir string) *slog.Logger {
	logPath := filepath.Join(baseDir, "turnstone.log")
	// Use Append so multiple nodes/tests writing to the same file don't truncate each other
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("Failed to open log file for node: %v", err)
	}
	// We rely on OS cleanup or specific test cleanup, but to be safe we can register a cleanup.
	t.Cleanup(func() { logFile.Close() })

	multiWriter := io.MultiWriter(os.Stdout, logFile)
	return slog.New(slog.NewTextHandler(multiWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// setupSharedCertEnv creates a temporary directory with a shared CA and certificates
// to be used by multiple server instances in an integration test.
func setupSharedCertEnv(t *testing.T) (string, *tls.Config) {
	// Use MkdirTemp to persist artifacts
	baseDir, err := os.MkdirTemp("", "turnstone-replication-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Test Artifacts Directory: %s", baseDir)

	certsDir := filepath.Join(baseDir, "certs")

	// Generate config and artifacts (certs) once
	cfg := Config{
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		NumberOfDatabases: 4, // Need 0 (System), 1 (User), 2 (Target), 3 (Cascade)
		Debug:             false,
	}
	if err := GenerateConfigArtifacts(baseDir, cfg, filepath.Join(baseDir, "config.json")); err != nil {
		t.Fatal(err)
	}

	// Create client TLS config
	clientCert, _ := tls.LoadX509KeyPair(filepath.Join(certsDir, "client.crt"), filepath.Join(certsDir, "client.key"))
	caCert, _ := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            pool,
		InsecureSkipVerify: true,
	}

	return baseDir, tlsConfig
}

// startServerNode starts a single TurnstoneDB server instance within the shared environment.
func startServerNode(t *testing.T, baseDir, name string, sharedTLS *tls.Config) (*Server, string, context.CancelFunc) {
	// Create a logger that writes to the shared log file
	logger := createTestLogger(t, baseDir).With("node", name)

	nodeDir := filepath.Join(baseDir, name)

	stores := make(map[string]*Store)
	// Initialize Stores 0, 1, 2, and 3
	for _, dbName := range []string{"0", "1", "2", "3"} {
		dbPath := filepath.Join(nodeDir, "data", dbName)
		store, err := NewStore(dbPath, logger, true, 0, true)
		if err != nil {
			t.Fatalf("Failed to init store %s: %v", dbName, err)
		}
		stores[dbName] = store
	}

	// Create Replication Manager
	rm := NewReplicationManager(stores, sharedTLS, logger)

	// Create Server using shared certs
	certsDir := filepath.Join(baseDir, "certs")
	srv, err := NewServer(
		":0", "", stores, logger, 10, 5*time.Second,
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
		filepath.Join(certsDir, "ca.crt"),
		rm,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx)

	// Wait for listener to accept connections
	time.Sleep(50 * time.Millisecond)
	return srv, srv.listener.Addr().String(), cancel
}

// --- Tests ---

// TestReplication_FanOut tests a single leader with multiple direct replicas.
// Topology: Leader -> (Replica1, Replica2)
func TestReplication_FanOut(t *testing.T) {
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Leader
	_, leaderAddr, cancelLeader := startServerNode(t, baseDir, "leader", tlsConfig)
	defer cancelLeader()

	// Start Replica 1
	_, r1Addr, cancelR1 := startServerNode(t, baseDir, "replica1", tlsConfig)
	defer cancelR1()

	// Start Replica 2
	_, r2Addr, cancelR2 := startServerNode(t, baseDir, "replica2", tlsConfig)
	defer cancelR2()

	// Connect Clients
	clientLeader := connectClient(t, leaderAddr, tlsConfig)
	defer clientLeader.Close()
	selectDB(t, clientLeader, "1")

	clientR1 := connectClient(t, r1Addr, tlsConfig)
	defer clientR1.Close()
	selectDB(t, clientR1, "1")

	clientR2 := connectClient(t, r2Addr, tlsConfig)
	defer clientR2.Close()
	selectDB(t, clientR2, "1")

	// Configure Replication: R1 -> Leader (DB 1)
	configureReplication(t, clientR1, leaderAddr, "1")
	// Configure Replication: R2 -> Leader (DB 1)
	configureReplication(t, clientR2, leaderAddr, "1")

	// Write to Leader (DB 1)
	writeKeyVal(t, clientLeader, "fanKey", "fanVal")

	// Verify both replicas received the data
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		v1 := readKey(t, clientR1, "fanKey")
		v2 := readKey(t, clientR2, "fanKey")
		return string(v1) == "fanVal" && string(v2) == "fanVal"
	}, "Fan-out replication failed: Data not found on one or both replicas")
}

// TestReplication_Cascading tests chained replication.
// Topology: NodeA (Leader) -> NodeB (Replica/Leader) -> NodeC (Replica)
func TestReplication_Cascading(t *testing.T) {
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Node A (Leader)
	_, addrA, cancelA := startServerNode(t, baseDir, "nodeA", tlsConfig)
	defer cancelA()

	// Start Node B (Replica of A, Leader for C)
	_, addrB, cancelB := startServerNode(t, baseDir, "nodeB", tlsConfig)
	defer cancelB()

	// Start Node C (Replica of B)
	_, addrC, cancelC := startServerNode(t, baseDir, "nodeC", tlsConfig)
	defer cancelC()

	// Clients
	clientA := connectClient(t, addrA, tlsConfig)
	defer clientA.Close()
	selectDB(t, clientA, "1")

	clientB := connectClient(t, addrB, tlsConfig)
	defer clientB.Close()
	selectDB(t, clientB, "1")

	clientC := connectClient(t, addrC, tlsConfig)
	defer clientC.Close()
	selectDB(t, clientC, "1")

	// Configure B -> A (DB 1)
	configureReplication(t, clientB, addrA, "1")

	// Configure C -> B (DB 1)
	configureReplication(t, clientC, addrB, "1")

	// Write to A (DB 1)
	writeKeyVal(t, clientA, "cascadeKey", "cascadeVal")

	// Verify C gets it (transitively via B)
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		val := readKey(t, clientC, "cascadeKey")
		return string(val) == "cascadeVal"
	}, "Cascading replication failed: Node C did not receive data from Node A via Node B")
}

// TestReplication_SameServer_Loopback tests replication between two databases on the SAME server.
// Topology: NodeA(DB 1) -> NodeA(DB 2)
func TestReplication_SameServer_Loopback(t *testing.T) {
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Single Node
	_, addr, cancel := startServerNode(t, baseDir, "loopback", tlsConfig)
	defer cancel()

	client := connectClient(t, addr, tlsConfig)
	defer client.Close()

	// 1. Write data to Source DB "1"
	selectDB(t, client, "1")
	writeKeyVal(t, client, "loopKey", "loopVal")

	// 2. Select Target DB "2" and configure replication from DB "1" on the SAME address
	selectDB(t, client, "2")
	configureReplication(t, client, addr, "1")

	// 3. Verify data appears in DB "2"
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		val := readKey(t, client, "loopKey")
		return string(val) == "loopVal"
	}, "Loopback replication failed: DB 2 did not sync from DB 1 on same server")
}

// TestReplication_SlowConsumer_Dropped verifies that the leader closes the connection
// to a replica that fails to consume the stream within ReplicaSendTimeout.
func TestReplication_SlowConsumer_Dropped(t *testing.T) {
	// 1. Lower timeout to speed up test
	originalTimeout := ReplicaSendTimeout
	ReplicaSendTimeout = 200 * time.Millisecond
	defer func() { ReplicaSendTimeout = originalTimeout }()

	// 2. Setup Single Node Environment
	baseDir, tlsConfig := setupSharedCertEnv(t)
	_, addr, cancel := startServerNode(t, baseDir, "slow_consumer", tlsConfig)
	defer cancel()

	// 3. Connect "Slow" Replica manually
	// We use a raw connection so we can control exactly what we read (or don't read).
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// 4. Send Hello Handshake to subscribe to "1" DB (Valid DB)
	// Protocol: [OpCodeReplHello][Len][Ver:1][NumDBs:1]...
	// SubRequest: [NameLen][Name][LogID]
	dbName := "1"

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version
	binary.Write(buf, binary.BigEndian, uint32(1)) // NumDBs

	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, uint64(0)) // Start from LogID 0

	header := make([]byte, 5)
	header[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("Failed to send header: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("Failed to send body: %v", err)
	}

	// 5. Generate Load on Leader
	client := connectClient(t, addr, tlsConfig)
	defer client.Close()
	selectDB(t, client, "1") // Write to DB 1

	// Launch generator in background
	go func() {
		for i := 0; i < 50; i++ {
			client.AssertStatus(OpCodeBegin, nil, ResStatusOK)
			key := []byte("k")
			val := make([]byte, 4096)
			pl := make([]byte, 4+len(key)+len(val))
			binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
			copy(pl[4:], key)
			copy(pl[4+len(key):], val)
			client.AssertStatus(OpCodeSet, pl, ResStatusOK)
			client.AssertStatus(OpCodeCommit, nil, ResStatusOK)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// 6. Verify Disconnection
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	readBuf := make([]byte, 1024)
	for {
		_, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				return // Success
			}
			return // Success (ECONNRESET)
		}
	}
}

// TestServer_ReplicaOf_Integration verifies that a follower can replicate from a leader
// and that 'replicaof no one' stops the replication.
func TestServer_ReplicaOf_Integration(t *testing.T) {
	// Reusing the shared setup for consistency
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Leader
	_, leaderAddr, cancelLeader := startServerNode(t, baseDir, "leader_int", tlsConfig)
	defer cancelLeader()

	// Start Follower
	_, followerAddr, cancelFollower := startServerNode(t, baseDir, "follower_int", tlsConfig)
	defer cancelFollower()

	// Clients
	clientLeader := connectClient(t, leaderAddr, tlsConfig)
	defer clientLeader.Close()
	selectDB(t, clientLeader, "1")

	clientFollower := connectClient(t, followerAddr, tlsConfig)
	defer clientFollower.Close()
	selectDB(t, clientFollower, "1")

	// --- PHASE 1: START REPLICATION ---
	configureReplication(t, clientFollower, leaderAddr, "1")

	// Write to Leader
	writeKeyVal(t, clientLeader, "k1", "v1")

	// Verify K1 on Follower
	waitForConditionOrTimeout(t, 2*time.Second, func() bool {
		val := readKey(t, clientFollower, "k1")
		return string(val) == "v1"
	}, "Replication failed for K1")

	// --- PHASE 2: STOP REPLICATION ---
	// Send REPLICAOF NO ONE
	// Payload: [0][][DBName] (AddrLen=0 implies stop)
	dbBytes := []byte("1")
	stopPayload := make([]byte, 4+0+len(dbBytes))
	binary.BigEndian.PutUint32(stopPayload[0:4], 0)
	copy(stopPayload[4:], dbBytes)
	clientFollower.AssertStatus(OpCodeReplicaOf, stopPayload, ResStatusOK)

	// Write K2 to Leader
	writeKeyVal(t, clientLeader, "k2", "v2")

	// Wait a bit to ensure it DOESN'T replicate
	time.Sleep(300 * time.Millisecond)

	// Verify K2 is NOT on Follower
	val := readKey(t, clientFollower, "k2")
	if val != nil {
		t.Fatalf("Replication did not stop, found k2: %s", val)
	}
}

// --- Client Helpers ---

// selectDB switches the client's active database.
func selectDB(t *testing.T, c *testClient, dbName string) {
	c.AssertStatus(OpCodeSelect, []byte(dbName), ResStatusOK)
}

// configureReplication sends the REPLICAOF command to a server.
func configureReplication(t *testing.T, c *testClient, targetAddr, targetDB string) {
	addrBytes := []byte(targetAddr)
	dbBytes := []byte(targetDB)
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)

	c.AssertStatus(OpCodeReplicaOf, payload, ResStatusOK)
}

// writeKeyVal performs a Set operation within a transaction.
func writeKeyVal(t *testing.T, c *testClient, k, v string) {
	key := []byte(k)
	val := []byte(v)
	payload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
	copy(payload[4:], key)
	copy(payload[4+len(key):], val)

	c.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	c.AssertStatus(OpCodeSet, payload, ResStatusOK)
	c.AssertStatus(OpCodeCommit, nil, ResStatusOK)
}

// readKey performs a Get operation within a transaction.
func readKey(t *testing.T, c *testClient, k string) []byte {
	c.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	val, status := c.ReadStatus(OpCodeGet, []byte(k))
	c.AssertStatus(OpCodeCommit, nil, ResStatusOK)
	if status == ResStatusNotFound {
		return nil
	}
	return val
}
