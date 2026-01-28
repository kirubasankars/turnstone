package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
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
// This is distinct from setupTestEnv in server_test.go as it prepares for multiple nodes.
func setupSharedCertEnv(t *testing.T) (string, *tls.Config) {
	t.Helper()
	dir, err := os.MkdirTemp("", "turnstone-repl-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Test Artifacts Directory: %s", dir)

	// Generate artifacts (including admin certs)
	if err := config.GenerateConfigArtifacts(dir, config.Config{
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		NumberOfDatabases: 4,
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Failed to generate artifacts: %v", err)
	}

	// Reuse getClientTLS from server_test.go (same package)
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

	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, logFile), &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)

	nodeDir := filepath.Join(baseDir, name)
	stores := make(map[string]*store.Store)
	for _, dbName := range []string{"0", "1", "2", "3"} {
		partPath := filepath.Join(nodeDir, "data", dbName)
		// Removed isSystem (4th arg), using default 0 minReplicas
		st, err := store.NewStore(partPath, logger, 0, "time", 90, 0)
		if err != nil {
			t.Fatalf("Failed to init store %s: %v", dbName, err)
		}
		stores[dbName] = st
	}

	// Replication Manager needs SERVER certs to connect to upstream masters
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

func promoteNode(t *testing.T, baseDir, addr string, databases ...string) {
	t.Helper()
	adminTLS := getRoleTLS(t, baseDir, "admin")
	c := connectClient(t, addr, adminTLS)
	defer c.Close()

	if len(databases) == 0 {
		// Default to all if none specified? Or no-op?
		// Retain compatibility with my previous "Promote All" intent?
		// No, explicit is better.
		databases = []string{"0", "1", "2", "3"}
	}

	for _, dbName := range databases {
		selectDatabase(t, c, dbName)
		// Promote Payload: [MinReplicas(4)] = 0
		c.AssertStatus(protocol.OpCodePromote, make([]byte, 4), protocol.ResStatusOK)
	}
}

// Helper wrappers using functions from server_test.go

func selectDatabase(t *testing.T, c *testClient, dbName string) {
	t.Helper()
	c.AssertStatus(protocol.OpCodeSelect, []byte(dbName), protocol.ResStatusOK)
}

func configureReplication(t *testing.T, c *testClient, targetAddr, targetDB string) {
	t.Helper()
	addrBytes := []byte(targetAddr)
	dbBytes := []byte(targetDB)
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)

	c.AssertStatus(protocol.OpCodeReplicaOf, payload, protocol.ResStatusOK)
}

func stepDownNode(t *testing.T, c *testClient) {
	t.Helper()
	c.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
}

func writeKeyVal(t *testing.T, c *testClient, k, v string) {
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

func readKey(t *testing.T, c *testClient, k string) []byte {
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

func TestServer_ReplicaOf_Integration(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	// Reuse getRoleTLS from server_test.go
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Primary
	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_int", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	// Start Replica
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_int", clientTLS)
	defer cancelReplica()

	// Clients (Using connectClient from server_test.go)
	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	// IMPORTANT: Connect to Replica as ADMIN to configure replication
	clientReplicaAdmin := connectClient(t, replicaAddr, adminTLS)
	defer clientReplicaAdmin.Close()
	selectDatabase(t, clientReplicaAdmin, "1")

	// Connect as Client to Replica for Reading
	clientReplicaRead := connectClient(t, replicaAddr, clientTLS)
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
	// Use STEPDOWN instead of REPLICAOF NO ONE
	clientReplicaAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)

	// Write K2 to Primary
	writeKeyVal(t, clientPrimary, "k2", "v2")

	// Wait a bit
	time.Sleep(300 * time.Millisecond)

	// Verify clientReplicaRead was disconnected
	// A Read attempt should fail
	_, err := clientReplicaRead.conn.Read(make([]byte, 1))
	if err == nil {
		t.Error("Client should have been disconnected after StepDown")
	}

	// Verify we can reconnect but DB is Undefined
	cNew := connectClient(t, replicaAddr, clientTLS)
	cNew.AssertStatus(protocol.OpCodeSelect, []byte("1"), protocol.ResStatusOK)
	cNew.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusErr) // Undefined
	cNew.Close()
}

func TestReplication_FanOut(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	_, r1Addr, cancelR1 := startServerNode(t, baseDir, "replica1", clientTLS)
	defer cancelR1()

	_, r2Addr, cancelR2 := startServerNode(t, baseDir, "replica2", clientTLS)
	defer cancelR2()

	// Connect Clients
	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	clientR1 := connectClient(t, r1Addr, clientTLS)
	defer clientR1.Close()
	selectDatabase(t, clientR1, "1")

	clientR2 := connectClient(t, r2Addr, clientTLS)
	defer clientR2.Close()
	selectDatabase(t, clientR2, "1")

	// Admin clients for configuration
	adminR1 := connectClient(t, r1Addr, adminTLS)
	defer adminR1.Close()
	selectDatabase(t, adminR1, "1")

	adminR2 := connectClient(t, r2Addr, adminTLS)
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

func TestReplication_Cascading_Rejected(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Node A (Primary)
	_, addrA, cancelA := startServerNode(t, baseDir, "nodeA", clientTLS)
	defer cancelA()
	promoteNode(t, baseDir, addrA, "1")

	// Start Node B (Replica of A, Primary for C)
	_, addrB, cancelB := startServerNode(t, baseDir, "nodeB", clientTLS)
	defer cancelB()

	// Start Node C (Replica of B)
	_, addrC, cancelC := startServerNode(t, baseDir, "nodeC", clientTLS)
	defer cancelC()

	// Clients
	clientA := connectClient(t, addrA, clientTLS)
	defer clientA.Close()
	selectDatabase(t, clientA, "1")

	clientC := connectClient(t, addrC, clientTLS)
	defer clientC.Close()
	selectDatabase(t, clientC, "1")

	// Admins for config
	adminB := connectClient(t, addrB, adminTLS)
	defer adminB.Close()
	selectDatabase(t, adminB, "1")

	adminC := connectClient(t, addrC, adminTLS)
	defer adminC.Close()
	selectDatabase(t, adminC, "1")

	// Configure B -> A (Database 1)
	configureReplication(t, adminB, addrA, "1")

	// Configure C -> B (Database 1)
	// This should send the config command, but since AddReplica now performs
	// a synchronous handshake check, and B detects it is a replica, B will reject
	// the handshake. Thus, `REPLICAOF` should return an ERROR.
	//
	// Note: B is a Replica of A. A replica cannot accept downstream replicas (Cascading disabled).

	// Construct manual payload to Assert Err status
	addrBytes := []byte(addrB)
	dbBytes := []byte("1")
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)

	adminC.AssertStatus(protocol.OpCodeReplicaOf, payload, protocol.ResStatusErr)

	// Write to A (Database 1)
	writeKeyVal(t, clientA, "cascadeKey", "cascadeVal")

	// Verify C does NOT get it (transitively via B)
	// We wait a bit to give it a chance to fail if it were going to work.
	time.Sleep(2 * time.Second)

	val := readKey(t, clientC, "cascadeKey")
	if val != nil {
		t.Fatalf("Cascading replication should be rejected, but Node C received data: %s", val)
	}
}

func TestReplication_SameServer_Loopback(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Single Node
	_, addr, cancel := startServerNode(t, baseDir, "loopback", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()

	adminClient := connectClient(t, addr, adminTLS)
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

func TestReplication_SlowConsumer_Dropped(t *testing.T) {
	// 1. Lower timeout to speed up test
	originalTimeout := protocol.ReplicationTimeout
	protocol.ReplicationTimeout = 200 * time.Millisecond
	defer func() { protocol.ReplicationTimeout = originalTimeout }()

	// 2. Setup Single Node Environment
	baseDir, clientTLS := setupSharedCertEnv(t)
	_, addr, cancel := startServerNode(t, baseDir, "slow_consumer", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	// 3. Connect "Slow" Replica manually using CDC role credentials
	cdcTLS := getRoleTLS(t, baseDir, "cdc")
	conn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// 4. Send Hello Handshake
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
		genConn.Read(dump)

		// Helper to send/read
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
			if _, err := io.ReadFull(genConn, h); err != nil {
				return err
			}
			length := binary.BigEndian.Uint32(h[1:])
			if length > 0 {
				io.ReadFull(genConn, make([]byte, length))
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

	// 6. Verify Disconnection (Timeout or EOF)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	readBuf := make([]byte, 1024)
	for {
		_, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				return // Success
			}
			return // Success if reset/closed
		}
	}
}

// TestReplication_FullSync_Integration verifies that a Replica server can correctly
// ingest a full snapshot from a Primary when the WAL logs are missing, and then
// seamlessly transition to receiving live updates.
func TestReplication_FullSync_Integration(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// 1. Start Primary
	primarySrv, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_fs", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	// 2. Populate Primary with data that will be "snapshotted"
	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	writeKeyVal(t, clientPrimary, "snapKey", "snapVal")

	// 3. Force WAL Purge on Primary to make logs unavailable using Promote (Forces rotation)
	st1 := primarySrv.stores["1"]
	// StepDown first because Promote now requires it if already Primary
	cAdmin := connectClient(t, primaryAddr, adminTLS)
	selectDatabase(t, cAdmin, "1")
	cAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	cAdmin.Close()

	if err := st1.Promote(); err != nil {
		t.Fatalf("Promote failed: %v", err)
	}

	// Purge WAL.
	// We purge up to currentOpID + 1 to ensure the old log file is eligible.
	currentOpID := st1.DB.LastOpID()
	if err := st1.DB.PurgeWAL(currentOpID + 1); err != nil {
		t.Fatalf("PurgeWAL failed: %v", err)
	}

	// 4. Start Replica (Empty)
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_fs", clientTLS)
	defer cancelReplica()

	clientReplica := connectClient(t, replicaAddr, clientTLS)
	defer clientReplica.Close()
	selectDatabase(t, clientReplica, "1")

	adminReplica := connectClient(t, replicaAddr, adminTLS)
	defer adminReplica.Close()
	selectDatabase(t, adminReplica, "1")

	// 5. Configure Replication
	// Primary should detect missing WAL and send Snapshot.
	configureReplication(t, adminReplica, primaryAddr, "1")

	// 6. Verify Snapshot Data Arrives
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, clientReplica, "snapKey")
		return string(val) == "snapVal"
	}, "Replica failed to receive snapshot data (snapKey)")

	// 7. Verify Transition to Live Streaming
	// Write new data to Primary
	writeKeyVal(t, clientPrimary, "liveKey", "liveVal")

	// Check Replica
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, clientReplica, "liveKey")
		return string(val) == "liveVal"
	}, "Replica failed to receive live stream data (liveKey) after snapshot")
}

func TestReplication_KeyCount_Match(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Start Primary
	primarySrv, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_kc", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	// Start Replica
	replicaSrv, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_kc", clientTLS)
	defer cancelReplica()

	// Clients
	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	adminReplica := connectClient(t, replicaAddr, adminTLS)
	defer adminReplica.Close()
	selectDatabase(t, adminReplica, "1")

	// 1. Write initial keys to Primary
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		writeKeyVal(t, clientPrimary, fmt.Sprintf("k%d", i), "val")
	}

	// 2. Configure Replication
	configureReplication(t, adminReplica, primaryAddr, "1")

	// 3. Verify Key Count on Replica matches Primary
	// We check specific metrics "turnstone_db_key_count"

	// Wait for Primary to definitely have the count updated in metrics (it might be cached/background)
	waitForMetric(t, primarySrv, "turnstone_db_key_count", func(val float64) bool {
		return int(val) == numKeys
	})

	// Wait for Replica to sync and update its key count
	waitForMetric(t, replicaSrv, "turnstone_db_key_count", func(val float64) bool {
		return int(val) == numKeys
	})
}

func TestReplication_TimelineFork_Recovery(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// 1. Start Primary
	primarySrv, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_tl", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	// 2. Start Replica
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_tl", clientTLS)
	defer cancelReplica()

	// Clients
	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	clientReplica := connectClient(t, replicaAddr, clientTLS)
	defer clientReplica.Close()
	selectDatabase(t, clientReplica, "1")

	adminReplica := connectClient(t, replicaAddr, adminTLS)
	defer adminReplica.Close()
	selectDatabase(t, adminReplica, "1")

	// 3. Write on Timeline 1
	writeKeyVal(t, clientPrimary, "t1_key", "val1")

	// 4. Force Timeline Switch on Primary
	// This simulates a promotion event or a history fork.
	st1 := primarySrv.stores["1"]

	// StepDown first because Promote now requires it if already Primary
	cAdmin := connectClient(t, primaryAddr, adminTLS)
	selectDatabase(t, cAdmin, "1")
	cAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	cAdmin.Close()

	if err := st1.Promote(); err != nil {
		t.Fatalf("Primary promote failed: %v", err)
	}

	// 5. Write on Timeline 2
	writeKeyVal(t, clientPrimary, "t2_key", "val2")

	// 6. Configure Replication (Replica connects to Primary)
	// The replica should pull T1 data, cross the timeline boundary, and pull T2 data.
	configureReplication(t, adminReplica, primaryAddr, "1")

	// 7. Verify Data
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		v1 := readKey(t, clientReplica, "t1_key")
		v2 := readKey(t, clientReplica, "t2_key")
		return string(v1) == "val1" && string(v2) == "val2"
	}, "Replica failed to sync across timeline fork")
}

// startServerNodeWithReplicas starts a single TurnstoneDB server instance with specific minReplicas.
func startServerNodeWithReplicas(t *testing.T, baseDir, name string, sharedTLS *tls.Config, minReplicas int) (*Server, string, context.CancelFunc) {
	t.Helper()
	logPath := filepath.Join(baseDir, "turnstone.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("Failed to open log file for node: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, logFile), &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)

	nodeDir := filepath.Join(baseDir, name)
	stores := make(map[string]*store.Store)
	for _, dbName := range []string{"0", "1", "2", "3"} {
		partPath := filepath.Join(nodeDir, "data", dbName)
		// Use minReplicas here
		st, err := store.NewStore(partPath, logger, minReplicas, "time", 90, 0)
		if err != nil {
			t.Fatalf("Failed to init store %s: %v", dbName, err)
		}
		stores[dbName] = st
	}

	certsDir := filepath.Join(baseDir, "certs")
	serverCert, _ := tls.LoadX509KeyPair(filepath.Join(certsDir, "server.crt"), filepath.Join(certsDir, "server.key"))
	caCert, _ := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	replTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}, RootCAs: pool, InsecureSkipVerify: true}

	rm := replication.NewReplicationManager(name, stores, replTLS, logger)

	srv, err := NewServer(
		name,
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

	time.Sleep(50 * time.Millisecond)
	return srv, srv.listener.Addr().String(), cancel
}

func TestReplication_KeyCount_SyncAndAsync(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// 1. Start Primary with MinReplicas=1 (Sync Replication)
	// Writes to this server will block until at least 1 replica acknowledges.
	primarySrv, primaryAddr, cancelPrimary := startServerNodeWithReplicas(t, baseDir, "primary_sync", clientTLS, 1)
	defer cancelPrimary()

	// FIX: Promote Primary Node so it accepts writes.
	// Wait briefly to ensure the promote call connects after server starts.
	time.Sleep(50 * time.Millisecond)
	promoteNode(t, baseDir, primaryAddr, "1")

	// 2. Start Sync Replica (This will satisfy the quorum)
	replicaSyncSrv, replicaSyncAddr, cancelReplicaSync := startServerNode(t, baseDir, "replica_sync", clientTLS)
	defer cancelReplicaSync()

	// 3. Start Async Replica (This is an extra observer, not strictly required for quorum if Sync is present, but receives data)
	replicaAsyncSrv, replicaAsyncAddr, cancelReplicaAsync := startServerNode(t, baseDir, "replica_async", clientTLS)
	defer cancelReplicaAsync()

	// Clients
	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	adminReplicaSync := connectClient(t, replicaSyncAddr, adminTLS)
	defer adminReplicaSync.Close()
	selectDatabase(t, adminReplicaSync, "1")

	adminReplicaAsync := connectClient(t, replicaAsyncAddr, adminTLS)
	defer adminReplicaAsync.Close()
	selectDatabase(t, adminReplicaAsync, "1")

	// 4. Configure Replication
	// Connect Sync Replica -> Primary
	configureReplication(t, adminReplicaSync, primaryAddr, "1")

	// Connect Async Replica -> Primary
	configureReplication(t, adminReplicaAsync, primaryAddr, "1")

	// 5. Write keys to Primary
	// Since MinReplicas=1, this would hang if no replicas were connected.
	// Success here implies quorum was met.
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		writeKeyVal(t, clientPrimary, fmt.Sprintf("k%d", i), "val")
	}

	// 6. Verify Key Counts on all nodes
	// The Primary metric updates on write.
	waitForMetric(t, primarySrv, "turnstone_db_key_count", func(val float64) bool {
		return int(val) == numKeys
	})

	// The Sync Replica must have the keys to have acknowledged the writes.
	waitForMetric(t, replicaSyncSrv, "turnstone_db_key_count", func(val float64) bool {
		return int(val) == numKeys
	})

	// The Async Replica should also eventually catch up.
	waitForMetric(t, replicaAsyncSrv, "turnstone_db_key_count", func(val float64) bool {
		return int(val) == numKeys
	})
}

func TestReplication_Retention_LeaderProtectsSlowFollower(t *testing.T) {
	// Force tiny WAL files (1KB) so we generate multiple files quickly
	os.Setenv("TS_TEST_WAL_SIZE", "1024")
	defer os.Unsetenv("TS_TEST_WAL_SIZE")

	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// 1. Start Leader
	leaderSrv, leaderAddr, cancelLeader := startServerNode(t, baseDir, "leader_ret", clientTLS)
	defer cancelLeader()

	// FIX: Promote Leader so it accepts writes
	time.Sleep(50 * time.Millisecond)
	promoteNode(t, baseDir, leaderAddr, "1")

	// 2. Start Follower (Using CDC for manual control simulation, or standard replica)
	// We use standard replica to ensure it registers a slot properly
	_, replicaAddr, cancelReplica := startServerNode(t, baseDir, "replica_ret", clientTLS)
	defer cancelReplica()

	// Connect
	adminReplica := connectClient(t, replicaAddr, adminTLS)
	defer adminReplica.Close()
	selectDatabase(t, adminReplica, "1")
	configureReplication(t, adminReplica, leaderAddr, "1")

	// 3. Write data to generate multiple WAL files
	// Entry ~50 bytes. 100 entries = 5KB -> ~5 WAL files.
	clientLeader := connectClient(t, leaderAddr, clientTLS)
	defer clientLeader.Close()
	selectDatabase(t, clientLeader, "1")

	for i := 0; i < 100; i++ {
		writeKeyVal(t, clientLeader, fmt.Sprintf("k%d", i), "val")
	}

	// 4. Wait for sync
	clientReplica := connectClient(t, replicaAddr, clientTLS)
	defer clientReplica.Close()
	selectDatabase(t, clientReplica, "1")
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, clientReplica, "k99")
		return string(val) == "val"
	}, "Initial sync failed")

	// 5. STOP the replica (cancel context)
	// Important: Explicitly stop replication on the replica first to cut the connection immediately.
	// Simply canceling the server context might leave the outgoing replication loop running for a bit.
	adminReplica2 := connectClient(t, replicaAddr, adminTLS)
	selectDatabase(t, adminReplica2, "1")
	// Use StepDown to stop replication properly
	stepDownNode(t, adminReplica2)
	adminReplica2.Close()

	cancelReplica()

	// 6. Write MORE data to Leader (Another 100 entries -> ~5 more WAL files)
	for i := 100; i < 200; i++ {
		writeKeyVal(t, clientLeader, fmt.Sprintf("k%d", i), "val")
	}

	// 7. Force Checkpoint on Leader (Enables purging of old WALs if no replicas needed them)
	st1 := leaderSrv.stores["1"]
	if err := st1.DB.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 8. Run Retention Check Manually
	st1.EnforceRetentionPolicy()

	// 9. Verify WAL files are RETAINED
	// The follower disconnected around OpID ~100.
	// We wrote up to ~200.
	// If retention works, files containing 100..200 must exist.
	// If it failed, older files would be deleted because Leader checkpointed.
	walDir := filepath.Join(baseDir, "leader_ret", "data", "1", "wal")
	files, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))

	// With 1KB limit and 200 writes, we expect roughly 10 files.
	if len(files) < 5 {
		t.Errorf("Leader deleted WAL files too aggressively! Found %d files", len(files))
	}

	// 10. Delete the Replica Slot manually on Leader
	if err := st1.DeleteReplica("replica_ret"); err != nil {
		t.Fatalf("Failed to delete replica slot: %v", err)
	}

	// 11. Run Retention again
	st1.EnforceRetentionPolicy()

	// 12. Verify WAL files are PURGED
	// Now that the slot is gone, Leader should only keep files needed for its own crash recovery (latest ones)
	filesAfter, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if len(filesAfter) >= len(files) {
		t.Errorf("Leader failed to purge WAL files after slot deletion. Before: %d, After: %d", len(files), len(filesAfter))
	}
}

func TestReplication_Retention_FollowerRespectsLeaderSafePoint(t *testing.T) {
	// Force tiny WAL files
	os.Setenv("TS_TEST_WAL_SIZE", "1024")
	defer os.Unsetenv("TS_TEST_WAL_SIZE")

	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	// Topology: Leader -> Follower1
	//           Leader -> Follower2 (Simulated)

	// 1. Start Leader
	_, leaderAddr, cancelLeader := startServerNode(t, baseDir, "leader_safe", clientTLS)
	defer cancelLeader()
	promoteNode(t, baseDir, leaderAddr, "1")

	// 2. Start Follower1 (The one we are testing)
	follower1Srv, follower1Addr, cancelFollower1 := startServerNode(t, baseDir, "follower1", clientTLS)
	defer cancelFollower1()

	// Connect F1 -> Leader
	adminF1 := connectClient(t, follower1Addr, adminTLS)
	defer adminF1.Close()
	selectDatabase(t, adminF1, "1")
	configureReplication(t, adminF1, leaderAddr, "1")

	// 3. Start Follower2 (To create a straggler)
	_, follower2Addr, cancelFollower2 := startServerNode(t, baseDir, "follower_straggler", clientTLS)
	defer cancelFollower2()

	// Connect F2 -> Leader
	adminF2 := connectClient(t, follower2Addr, adminTLS)
	defer adminF2.Close()
	selectDatabase(t, adminF2, "1")
	configureReplication(t, adminF2, leaderAddr, "1")

	clientLeader := connectClient(t, leaderAddr, clientTLS)
	defer clientLeader.Close()
	selectDatabase(t, clientLeader, "1")

	// 4. Write Batch 1 (0..100) -> Both Followers sync
	for i := 0; i < 100; i++ {
		writeKeyVal(t, clientLeader, fmt.Sprintf("k%d", i), "val")
	}

	// Wait for F1 to sync
	clientF1 := connectClient(t, follower1Addr, clientTLS)
	defer clientF1.Close()
	selectDatabase(t, clientF1, "1")
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, clientF1, "k99")
		return string(val) == "val"
	}, "F1 sync failed")

	// Wait for F2 to sync
	clientF2 := connectClient(t, follower2Addr, clientTLS)
	defer clientF2.Close()
	selectDatabase(t, clientF2, "1")
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, clientF2, "k99")
		return string(val) == "val"
	}, "F2 sync failed")

	// 5. STOP Follower 2 (Straggler)
	// Explicitly STOP REPLICATION first to ensure it stops pulling/ACKing
	adminF2ForStop := connectClient(t, follower2Addr, adminTLS)
	selectDatabase(t, adminF2ForStop, "1")
	// Use StepDown to stop replication properly
	stepDownNode(t, adminF2ForStop)
	adminF2ForStop.Close()

	cancelFollower2()

	// 6. Write Batch 2 (100..200) -> F1 syncs, F2 is dead
	for i := 100; i < 200; i++ {
		writeKeyVal(t, clientLeader, fmt.Sprintf("k%d", i), "val")
	}

	// Verify F1 sync
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, clientF1, "k199")
		return string(val) == "val"
	}, "F1 batch 2 sync failed")

	// 7. Trigger Leader to broadcast SafePoint
	// Leader sees F2 at ~100. Leader sends SafePoint(~100) to F1.
	// We wait a bit for the periodic broadcast (1s interval in replication.go).
	time.Sleep(2 * time.Second)

	// 8. Force Checkpoint on F1
	// This would normally allow F1 to delete logs 0..100 IF it had no constraints.
	stF1 := follower1Srv.stores["1"]
	if err := stF1.DB.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 9. Run Retention on F1
	stF1.EnforceRetentionPolicy()

	// 10. Verify F1 RETAINS logs
	// F1 has no downstream replicas (minReplicaSeq=Max).
	// But it has Upstream SafePoint = ~100.
	// It should NOT delete files containing OpID 100.
	f1WalDir := filepath.Join(baseDir, "follower1", "data", "1", "wal")
	files, _ := filepath.Glob(filepath.Join(f1WalDir, "*.wal"))

	// 200 writes @ 1KB limit = ~10 files.
	// If it deleted everything older than checkpoint (200), we'd have 1-2 files.
	// If it respected SafePoint(100), we should have ~5 files.
	if len(files) < 4 {
		t.Errorf("Follower 1 purged logs despite Leader SafePoint! Files: %d", len(files))
	}
}
