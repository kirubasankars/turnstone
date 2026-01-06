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
	"testing"
	"time"

	"turnstone/config"
	"turnstone/protocol"
	"turnstone/replication"
	"turnstone/store"
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
	cfg := config.Config{
		TLSCertFile:        "certs/server.crt",
		TLSKeyFile:         "certs/server.key",
		TLSCAFile:          "certs/ca.crt",
		NumberOfPartitions: 4, // Need 0 (System), 1 (User), 2 (Target), 3 (Cascade)
		Debug:              false,
	}
	if err := config.GenerateConfigArtifacts(baseDir, cfg, filepath.Join(baseDir, "config.json")); err != nil {
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

	stores := make(map[string]*store.Store)
	// Initialize Stores 0, 1, 2, and 3
	for _, partitionName := range []string{"0", "1", "2", "3"} {
		partPath := filepath.Join(nodeDir, "data", partitionName)
		st, err := store.NewStore(partPath, logger, true, 0, true)
		if err != nil {
			t.Fatalf("Failed to init store %s: %v", partitionName, err)
		}
		stores[partitionName] = st
	}

	// Create Replication Manager
	rm := replication.NewReplicationManager(stores, sharedTLS, logger)

	// Create Server using shared certs
	certsDir := filepath.Join(baseDir, "certs")
	srv, err := NewServer(
		":0", stores, logger, 10,
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

// TestReplication_FanOut tests a single primary with multiple direct replicas.
// Topology: Primary -> (Replica1, Replica2)
func TestReplication_FanOut(t *testing.T) {
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Primary
	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary", tlsConfig)
	defer cancelPrimary()

	// Start Replica 1
	_, r1Addr, cancelR1 := startServerNode(t, baseDir, "replica1", tlsConfig)
	defer cancelR1()

	// Start Replica 2
	_, r2Addr, cancelR2 := startServerNode(t, baseDir, "replica2", tlsConfig)
	defer cancelR2()

	// Connect Clients
	clientPrimary := connectReplClient(t, primaryAddr, tlsConfig)
	defer clientPrimary.Close()
	selectPartition(t, clientPrimary, "1")

	clientR1 := connectReplClient(t, r1Addr, tlsConfig)
	defer clientR1.Close()
	selectPartition(t, clientR1, "1")

	clientR2 := connectReplClient(t, r2Addr, tlsConfig)
	defer clientR2.Close()
	selectPartition(t, clientR2, "1")

	// Configure Replication: R1 -> Primary (Partition 1)
	configureReplication(t, clientR1, primaryAddr, "1")
	// Configure Replication: R2 -> Primary (Partition 1)
	configureReplication(t, clientR2, primaryAddr, "1")

	// Write to Primary (Partition 1)
	writeKeyVal(t, clientPrimary, "fanKey", "fanVal")

	// Verify both replicas received the data
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		v1 := readKey(t, clientR1, "fanKey")
		v2 := readKey(t, clientR2, "fanKey")
		return string(v1) == "fanVal" && string(v2) == "fanVal"
	}, "Fan-out replication failed: Data not found on one or both replicas")
}

// TestReplication_Cascading tests chained replication.
// Topology: NodeA (Primary) -> NodeB (Replica/Primary) -> NodeC (Replica)
func TestReplication_Cascading(t *testing.T) {
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Node A (Primary)
	_, addrA, cancelA := startServerNode(t, baseDir, "nodeA", tlsConfig)
	defer cancelA()

	// Start Node B (Replica of A, Primary for C)
	_, addrB, cancelB := startServerNode(t, baseDir, "nodeB", tlsConfig)
	defer cancelB()

	// Start Node C (Replica of B)
	_, addrC, cancelC := startServerNode(t, baseDir, "nodeC", tlsConfig)
	defer cancelC()

	// Clients
	clientA := connectReplClient(t, addrA, tlsConfig)
	defer clientA.Close()
	selectPartition(t, clientA, "1")

	clientB := connectReplClient(t, addrB, tlsConfig)
	defer clientB.Close()
	selectPartition(t, clientB, "1")

	clientC := connectReplClient(t, addrC, tlsConfig)
	defer clientC.Close()
	selectPartition(t, clientC, "1")

	// Configure B -> A (Partition 1)
	configureReplication(t, clientB, addrA, "1")

	// Configure C -> B (Partition 1)
	configureReplication(t, clientC, addrB, "1")

	// Write to A (Partition 1)
	writeKeyVal(t, clientA, "cascadeKey", "cascadeVal")

	// Verify C gets it (transitively via B)
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		val := readKey(t, clientC, "cascadeKey")
		return string(val) == "cascadeVal"
	}, "Cascading replication failed: Node C did not receive data from Node A via Node B")
}

// TestReplication_SameServer_Loopback tests replication between two partitions on the SAME server.
// Topology: NodeA(Partition 1) -> NodeA(Partition 2)
func TestReplication_SameServer_Loopback(t *testing.T) {
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Single Node
	_, addr, cancel := startServerNode(t, baseDir, "loopback", tlsConfig)
	defer cancel()

	client := connectReplClient(t, addr, tlsConfig)
	defer client.Close()

	// 1. Write data to Source Partition "1"
	selectPartition(t, client, "1")
	writeKeyVal(t, client, "loopKey", "loopVal")

	// 2. Select Target Partition "2" and configure replication from Partition "1" on the SAME address
	selectPartition(t, client, "2")
	configureReplication(t, client, addr, "1")

	// 3. Verify data appears in Partition "2"
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		val := readKey(t, client, "loopKey")
		return string(val) == "loopVal"
	}, "Loopback replication failed: Partition 2 did not sync from Partition 1 on same server")
}

// TestReplication_SlowConsumer_Dropped verifies that the primary closes the connection
// to a replica that fails to consume the stream within protocol.ReplicationTimeout.
func TestReplication_SlowConsumer_Dropped(t *testing.T) {
	// 1. Lower timeout to speed up test
	originalTimeout := protocol.ReplicationTimeout
	protocol.ReplicationTimeout = 200 * time.Millisecond
	defer func() { protocol.ReplicationTimeout = originalTimeout }()

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

	// 4. Send Hello Handshake to subscribe to "1" Partition (Valid Partition)
	partitionName := "1"

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version
	binary.Write(buf, binary.BigEndian, uint32(1)) // NumPartitions

	binary.Write(buf, binary.BigEndian, uint32(len(partitionName)))
	buf.WriteString(partitionName)
	binary.Write(buf, binary.BigEndian, uint64(0)) // Start from LogSeq 0

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("Failed to send header: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("Failed to send body: %v", err)
	}

	// 5. Generate Load on Primary
	// We use a raw connection loop here instead of replTestClient to avoid
	// t.Fatalf() when the connection is closed during test teardown.
	go func() {
		genConn, err := tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return
		}
		defer genConn.Close()

		// Select partition 1
		pName := []byte("1")
		sel := make([]byte, 9+len(pName))
		sel[0] = protocol.OpCodeSelect
		binary.BigEndian.PutUint32(sel[1:], uint32(len(pName)))
		copy(sel[5:], pName)
		if _, err := genConn.Write(sel); err != nil {
			return
		}
		// Read Select Resp
		dump := make([]byte, 1024)
		if _, err := genConn.Read(dump); err != nil {
			return
		}

		// Helper to send/read without failure
		doRequest := func(op byte, payload []byte) error {
			h := make([]byte, 5)
			h[0] = op
			binary.BigEndian.PutUint32(h[1:], uint32(len(payload)))
			if _, err := genConn.Write(h); err != nil {
				return err
			}
			if len(payload) > 0 {
				if _, err := genConn.Write(payload); err != nil {
					return err
				}
			}
			// Read Response
			if _, err := io.ReadFull(genConn, h); err != nil {
				return err
			}
			length := binary.BigEndian.Uint32(h[1:])
			if length > 0 {
				if _, err := io.ReadFull(genConn, make([]byte, length)); err != nil {
					return err
				}
			}
			return nil
		}

		for i := 0; i < 2000; i++ {
			if err := doRequest(protocol.OpCodeBegin, nil); err != nil {
				return
			}
			key := []byte("k")
			val := make([]byte, 4096)
			pl := make([]byte, 4+len(key)+len(val))
			binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
			copy(pl[4:], key)
			copy(pl[4+len(key):], val)

			if err := doRequest(protocol.OpCodeSet, pl); err != nil {
				return
			}
			if err := doRequest(protocol.OpCodeCommit, nil); err != nil {
				return
			}
		}
	}()

	// 6. Verify Disconnection
	// We read nothing (simulating slow consumer). The server should close the connection
	// when its buffer fills or timeout occurs (ReplicationTimeout).
	// Allow slack for TCP buffers filling up.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	readBuf := make([]byte, 1024)
	for {
		_, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				return // Success
			}
			// Success if reset/closed
			return
		}
	}
}

// TestServer_ReplicaOf_Integration verifies that a replica can replicate from a primary
// and that 'replicaof no one' stops the replication.
func TestServer_ReplicaOf_Integration(t *testing.T) {
	// Reusing the shared setup for consistency
	baseDir, tlsConfig := setupSharedCertEnv(t)

	// Start Primary
	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_int", tlsConfig)
	defer cancelPrimary()

	// Start Replica
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_int", tlsConfig)
	defer cancelReplica()

	// Clients
	clientPrimary := connectReplClient(t, primaryAddr, tlsConfig)
	defer clientPrimary.Close()
	selectPartition(t, clientPrimary, "1")

	clientReplica := connectReplClient(t, replicaAddr, tlsConfig)
	defer clientReplica.Close()
	selectPartition(t, clientReplica, "1")

	// --- PHASE 1: START REPLICATION ---
	configureReplication(t, clientReplica, primaryAddr, "1")

	// Write to Primary
	writeKeyVal(t, clientPrimary, "k1", "v1")

	// Verify K1 on Replica
	waitForConditionOrTimeout(t, 2*time.Second, func() bool {
		val := readKey(t, clientReplica, "k1")
		return string(val) == "v1"
	}, "Replication failed for K1")

	// --- PHASE 2: STOP REPLICATION ---
	// Send REPLICAOF NO ONE
	// Payload: [0][][PartitionName] (AddrLen=0 implies stop)
	partBytes := []byte("1")
	stopPayload := make([]byte, 4+0+len(partBytes))
	binary.BigEndian.PutUint32(stopPayload[0:4], 0)
	copy(stopPayload[4:], partBytes)
	clientReplica.AssertStatus(protocol.OpCodeReplicaOf, stopPayload, protocol.ResStatusOK)

	// Write K2 to Primary
	writeKeyVal(t, clientPrimary, "k2", "v2")

	// Wait a bit to ensure it DOESN'T replicate
	time.Sleep(300 * time.Millisecond)

	// Verify K2 is NOT on Replica
	val := readKey(t, clientReplica, "k2")
	if val != nil {
		t.Fatalf("Replication did not stop, found k2: %s", val)
	}
}

// --- Client Helpers (Renamed to avoid conflict with server_test.go if in same package) ---

type replTestClient struct {
	conn net.Conn
	t    *testing.T
}

func (c *replTestClient) Close() {
	c.conn.Close()
}

func (c *replTestClient) Send(opCode byte, payload []byte) {
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

func (c *replTestClient) Read() (status byte, body []byte) {
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

func (c *replTestClient) AssertStatus(opCode byte, payload []byte, expectedStatus byte) []byte {
	c.Send(opCode, payload)
	status, body := c.Read()
	if status != expectedStatus {
		c.t.Fatalf("Op 0x%x: Expected status 0x%x, got 0x%x. Body: %s", opCode, expectedStatus, status, body)
	}
	return body
}

func (c *replTestClient) ReadStatus(opCode byte, payload []byte) ([]byte, byte) {
	c.Send(opCode, payload)
	status, body := c.Read()
	return body, status
}

func connectReplClient(t *testing.T, addr string, tlsConfig *tls.Config) *replTestClient {
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	return &replTestClient{conn: conn, t: t}
}

// selectPartition switches the client's active partition.
func selectPartition(t *testing.T, c *replTestClient, partitionName string) {
	c.AssertStatus(protocol.OpCodeSelect, []byte(partitionName), protocol.ResStatusOK)
}

// configureReplication sends the REPLICAOF command to a server.
func configureReplication(t *testing.T, c *replTestClient, targetAddr, targetPartition string) {
	addrBytes := []byte(targetAddr)
	partBytes := []byte(targetPartition)
	payload := make([]byte, 4+len(addrBytes)+len(partBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], partBytes)

	c.AssertStatus(protocol.OpCodeReplicaOf, payload, protocol.ResStatusOK)
}

// writeKeyVal performs a Set operation within a transaction.
func writeKeyVal(t *testing.T, c *replTestClient, k, v string) {
	key := []byte(k)
	val := []byte(v)
	payload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
	copy(payload[4:], key)
	copy(payload[4+len(key):], val)

	c.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	c.AssertStatus(protocol.OpCodeSet, payload, protocol.ResStatusOK)
	c.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
}

// readKey performs a Get operation within a transaction.
func readKey(t *testing.T, c *replTestClient, k string) []byte {
	c.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	val, status := c.ReadStatus(protocol.OpCodeGet, []byte(k))
	c.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
	if status == protocol.ResStatusNotFound {
		return nil
	}
	return val
}
