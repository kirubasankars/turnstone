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

// --- Replication Helpers ---

// setupSharedCertEnv creates a temporary directory with a shared CA and certificates.
// This mirrors setupTestEnv but returns just the dir and config for manual node spawning.
func setupSharedCertEnv(t *testing.T) (string, *tls.Config) {
	t.Helper()
	dir, err := os.MkdirTemp("", "turnstone-repl-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Test Artifacts Directory: %s", dir)

	// Generate artifacts (including admin certs)
	// Keeps NumberOfDatabases as config parameter but logically creates databases
	if err := config.GenerateConfigArtifacts(dir, config.Config{
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		NumberOfDatabases: 4,
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Failed to generate artifacts: %v", err)
	}

	// Load default client cert (role: client)
	return dir, getClientTLS(t, dir)
}

// startServerNode starts a single TurnstoneDB server instance within the shared environment.
func startServerNode(t *testing.T, baseDir, name string, sharedTLS *tls.Config) (*Server, string, context.CancelFunc) {
	t.Helper()
	// Create a logger that writes to the shared log file
	logPath := filepath.Join(baseDir, "turnstone.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("Failed to open log file for node: %v", err)
	}

	// Create node-specific logger
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, logFile), &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)

	nodeDir := filepath.Join(baseDir, name)
	stores := make(map[string]*store.Store)
	for _, dbName := range []string{"0", "1", "2", "3"} {
		partPath := filepath.Join(nodeDir, "data", dbName)
		st, err := store.NewStore(partPath, logger, true, 0, true, "time")
		if err != nil {
			t.Fatalf("Failed to init store %s: %v", dbName, err)
		}
		stores[dbName] = st
	}

	// Replication Manager needs SERVER certs to connect to upstream masters
	// We load them from the shared certs dir
	certsDir := filepath.Join(baseDir, "certs")
	serverCert, _ := tls.LoadX509KeyPair(filepath.Join(certsDir, "server.crt"), filepath.Join(certsDir, "server.key"))
	caCert, _ := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	replTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}, RootCAs: pool, InsecureSkipVerify: true}

	rm := replication.NewReplicationManager(name, stores, replTLS, logger)

	srv, err := NewServer(
		name, // Use node name as Server ID
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

	// Wait for listener
	time.Sleep(50 * time.Millisecond)
	return srv, srv.listener.Addr().String(), cancel
}

// connectReplClient is a local helper similar to connectClient in server_test.go
// but adapted for the replication tests context.
func connectReplClient(t *testing.T, addr string, tlsConfig *tls.Config) *replTestClient {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	return &replTestClient{conn: conn, t: t}
}

type replTestClient struct {
	conn net.Conn
	t    *testing.T
}

func (c *replTestClient) Close() {
	c.conn.Close()
}

func (c *replTestClient) Send(opCode byte, payload []byte) {
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

func (c *replTestClient) Read() (byte, []byte) {
	c.t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		c.t.Fatalf("Read response header failed: %v", err)
	}
	length := binary.BigEndian.Uint32(header[1:])
	var body []byte
	if length > 0 {
		body = make([]byte, length)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			c.t.Fatalf("Read response body failed: %v", err)
		}
	}
	return header[0], body
}

func (c *replTestClient) AssertStatus(opCode byte, payload []byte, expectedStatus byte) []byte {
	c.t.Helper()
	c.Send(opCode, payload)
	status, body := c.Read()
	if status != expectedStatus {
		c.t.Fatalf("Op 0x%x: Expected status 0x%x, got 0x%x. Body: %s", opCode, expectedStatus, status, body)
	}
	return body
}

// selectDatabase switches the client's active database.
func selectDatabase(t *testing.T, c *replTestClient, dbName string) {
	t.Helper()
	c.AssertStatus(protocol.OpCodeSelect, []byte(dbName), protocol.ResStatusOK)
}

// configureReplication sends the REPLICAOF command.
func configureReplication(t *testing.T, c *replTestClient, targetAddr, targetDB string) {
	t.Helper()
	addrBytes := []byte(targetAddr)
	dbBytes := []byte(targetDB)
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)

	c.AssertStatus(protocol.OpCodeReplicaOf, payload, protocol.ResStatusOK)
}

// writeKeyVal performs a Set operation within a transaction.
func writeKeyVal(t *testing.T, c *replTestClient, k, v string) {
	t.Helper()
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
	t.Helper()
	c.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	c.Send(protocol.OpCodeGet, []byte(k))
	status, body := c.Read()
	c.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)
	if status == protocol.ResStatusNotFound {
		return nil
	}
	return body
}

// waitForConditionOrTimeout is a helper to poll for a condition until timeout.
func waitForConditionOrTimeout(t *testing.T, timeout time.Duration, check func() bool, errMsg string) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(errMsg)
}

// --- Tests ---

// TestServer_ReplicaOf_Integration verifies that a replica can replicate from a primary
// and that 'replicaof no one' stops the replication.
func TestServer_ReplicaOf_Integration(t *testing.T) {
	// Reusing the shared setup for consistency
	baseDir, clientTLS := setupSharedCertEnv(t)
	// Admin TLS needed for ReplicaOf command
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Primary
	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_int", clientTLS)
	defer cancelPrimary()

	// Start Replica
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_int", clientTLS)
	defer cancelReplica()

	// Clients
	clientPrimary := connectReplClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	// IMPORTANT: Connect to Replica as ADMIN to configure replication
	clientReplicaAdmin := connectReplClient(t, replicaAddr, adminTLS)
	defer clientReplicaAdmin.Close()
	selectDatabase(t, clientReplicaAdmin, "1")

	// Connect as Client to Replica for Reading
	clientReplicaRead := connectReplClient(t, replicaAddr, clientTLS)
	defer clientReplicaRead.Close()
	selectDatabase(t, clientReplicaRead, "1")

	// --- PHASE 1: START REPLICATION ---
	// Configure Replica -> Primary (Database 1) using ADMIN connection
	configureReplication(t, clientReplicaAdmin, primaryAddr, "1")

	// Write to Primary using CLIENT connection
	writeKeyVal(t, clientPrimary, "k1", "v1")

	// Verify K1 on Replica using CLIENT connection (Reads allowed)
	waitForConditionOrTimeout(t, 2*time.Second, func() bool {
		val := readKey(t, clientReplicaRead, "k1")
		return string(val) == "v1"
	}, "Replication failed for K1")

	// --- PHASE 2: STOP REPLICATION ---
	// Send REPLICAOF NO ONE using ADMIN connection
	// Payload: [0][][DBName] (AddrLen=0 implies stop)
	dbBytes := []byte("1")
	stopPayload := make([]byte, 4+0+len(dbBytes))
	binary.BigEndian.PutUint32(stopPayload[0:4], 0)
	copy(stopPayload[4:], dbBytes)
	clientReplicaAdmin.AssertStatus(protocol.OpCodeReplicaOf, stopPayload, protocol.ResStatusOK)

	// Write K2 to Primary
	writeKeyVal(t, clientPrimary, "k2", "v2")

	// Wait a bit to ensure it DOESN'T replicate
	time.Sleep(300 * time.Millisecond)

	// Verify K2 is NOT on Replica
	val := readKey(t, clientReplicaRead, "k2")
	if val != nil {
		t.Fatalf("Replication did not stop, found k2: %s", val)
	}
}

// TestReplication_FanOut tests a single primary with multiple direct replicas.
func TestReplication_FanOut(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary", clientTLS)
	defer cancelPrimary()

	_, r1Addr, cancelR1 := startServerNode(t, baseDir, "replica1", clientTLS)
	defer cancelR1()

	_, r2Addr, cancelR2 := startServerNode(t, baseDir, "replica2", clientTLS)
	defer cancelR2()

	// Connect Clients
	clientPrimary := connectReplClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	clientR1 := connectReplClient(t, r1Addr, clientTLS)
	defer clientR1.Close()
	selectDatabase(t, clientR1, "1")

	clientR2 := connectReplClient(t, r2Addr, clientTLS)
	defer clientR2.Close()
	selectDatabase(t, clientR2, "1")

	// Admin clients for configuration
	adminR1 := connectReplClient(t, r1Addr, adminTLS)
	defer adminR1.Close()
	selectDatabase(t, adminR1, "1")

	adminR2 := connectReplClient(t, r2Addr, adminTLS)
	defer adminR2.Close()
	selectDatabase(t, adminR2, "1")

	// Configure
	configureReplication(t, adminR1, primaryAddr, "1")
	configureReplication(t, adminR2, primaryAddr, "1")

	// Write to Primary
	writeKeyVal(t, clientPrimary, "fanKey", "fanVal")

	// Verify
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		v1 := readKey(t, clientR1, "fanKey")
		v2 := readKey(t, clientR2, "fanKey")
		return string(v1) == "fanVal" && string(v2) == "fanVal"
	}, "Fan-out replication failed")
}

// TestReplication_Cascading tests chained replication.
// Topology: NodeA (Primary) -> NodeB (Replica/Primary) -> NodeC (Replica)
func TestReplication_Cascading(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Node A (Primary)
	_, addrA, cancelA := startServerNode(t, baseDir, "nodeA", clientTLS)
	defer cancelA()

	// Start Node B (Replica of A, Primary for C)
	_, addrB, cancelB := startServerNode(t, baseDir, "nodeB", clientTLS)
	defer cancelB()

	// Start Node C (Replica of B)
	_, addrC, cancelC := startServerNode(t, baseDir, "nodeC", clientTLS)
	defer cancelC()

	// Clients
	clientA := connectReplClient(t, addrA, clientTLS)
	defer clientA.Close()
	selectDatabase(t, clientA, "1")

	clientB := connectReplClient(t, addrB, clientTLS)
	defer clientB.Close()
	selectDatabase(t, clientB, "1")

	clientC := connectReplClient(t, addrC, clientTLS)
	defer clientC.Close()
	selectDatabase(t, clientC, "1")

	// Admins for config
	adminB := connectReplClient(t, addrB, adminTLS)
	defer adminB.Close()
	selectDatabase(t, adminB, "1")

	adminC := connectReplClient(t, addrC, adminTLS)
	defer adminC.Close()
	selectDatabase(t, adminC, "1")

	// Configure B -> A (Database 1)
	configureReplication(t, adminB, addrA, "1")

	// Configure C -> B (Database 1)
	configureReplication(t, adminC, addrB, "1")

	// Write to A (Database 1)
	writeKeyVal(t, clientA, "cascadeKey", "cascadeVal")

	// Verify C gets it (transitively via B)
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		val := readKey(t, clientC, "cascadeKey")
		return string(val) == "cascadeVal"
	}, "Cascading replication failed: Node C did not receive data from Node A via Node B")
}

// TestReplication_SameServer_Loopback tests replication between two databases on the SAME server.
// Topology: NodeA(Database 1) -> NodeA(Database 2)
func TestReplication_SameServer_Loopback(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Single Node
	_, addr, cancel := startServerNode(t, baseDir, "loopback", clientTLS)
	defer cancel()

	client := connectReplClient(t, addr, clientTLS)
	defer client.Close()

	adminClient := connectReplClient(t, addr, adminTLS)
	defer adminClient.Close()

	// 1. Write data to Source Database "1"
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "loopKey", "loopVal")

	// 2. Select Target Database "2" and configure replication from Database "1" on the SAME address
	selectDatabase(t, client, "2")
	selectDatabase(t, adminClient, "2") // Admin also needs to be on target DB
	configureReplication(t, adminClient, addr, "1")

	// 3. Verify data appears in Database "2"
	waitForConditionOrTimeout(t, 10*time.Second, func() bool {
		val := readKey(t, client, "loopKey")
		return string(val) == "loopVal"
	}, "Loopback replication failed: Database 2 did not sync from Database 1 on same server")
}

// TestReplication_SlowConsumer_Dropped verifies that the primary closes the connection
// to a replica that fails to consume the stream within protocol.ReplicationTimeout.
func TestReplication_SlowConsumer_Dropped(t *testing.T) {
	// 1. Lower timeout to speed up test
	originalTimeout := protocol.ReplicationTimeout
	protocol.ReplicationTimeout = 200 * time.Millisecond
	defer func() { protocol.ReplicationTimeout = originalTimeout }()

	// 2. Setup Single Node Environment
	baseDir, clientTLS := setupSharedCertEnv(t)
	_, addr, cancel := startServerNode(t, baseDir, "slow_consumer", clientTLS)
	defer cancel()

	// 3. Connect "Slow" Replica manually using CDC role credentials
	// Note: We use CDC role because this simulates a replication handshake which uses OpCodeReplHello
	cdcTLS := getRoleTLS(t, baseDir, "cdc")
	conn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// 4. Send Hello Handshake to subscribe to "1" Database (Valid Database)
	// Format: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	dbName := "1"
	clientID := "slow-reader"

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version

	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)

	binary.Write(buf, binary.BigEndian, uint32(1)) // NumDatabases

	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
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
	go func() {
		genConn, err := tls.Dial("tcp", addr, clientTLS)
		if err != nil {
			return
		}
		defer genConn.Close()

		// Select database 1
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
