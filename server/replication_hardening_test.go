// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

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
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/database"
	"turnstone/internal/backup"
	"turnstone/protocol"
	"turnstone/repl"
)

func startServerNodeWithRetention(t *testing.T, baseDir, name string, sharedTLS *tls.Config, minReplicas int) (*Server, string, context.CancelFunc) {
	t.Helper()
	logPath := filepath.Join(baseDir, "turnstone.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("Failed to open log file for node: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, logFile), &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)

	nodeDir := filepath.Join(baseDir, name)
	stores := make(map[string]*database.Database)
	for _, dbName := range []string{"0", "1", "2", "3"} {
		partPath := filepath.Join(nodeDir, "data", dbName)
		st, err := database.Open(context.Background(), partPath, logger, minReplicas, "replication", 90)
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

	rm := repl.NewManager(name, stores, replTLS, logger)

	srv, err := NewServer(
		name,
		":0", stores, logger, 10,
		filepath.Join(certsDir, "server.crt"),
		filepath.Join(certsDir, "server.key"),
		filepath.Join(certsDir, "ca.crt"),
		rm,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx)

	time.Sleep(50 * time.Millisecond)
	return srv, srv.Addr().String(), cancel
}

func sendReplHello(t *testing.T, conn *tls.Conn, clientID, dbName string, offset uint64) {
	t.Helper()
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, offset)

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))
	if _, err := conn.Write(header); err != nil {
		t.Fatalf("write hello header: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write hello body: %v", err)
	}
}

func readReplStatus(t *testing.T, conn *tls.Conn) (uint8, []byte) {
	t.Helper()
	head := make([]byte, 5)
	if _, err := io.ReadFull(conn, head); err != nil {
		t.Fatalf("read status header: %v", err)
	}
	ln := binary.BigEndian.Uint32(head[1:])
	body := make([]byte, ln)
	if ln > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatalf("read status body: %v", err)
		}
	}
	return head[0], body
}

func TestReplication_InvalidCursorHandshake(t *testing.T) {
	baseDir, _ := setupSharedCertEnv(t)
	replTLS := getRoleTLS(t, baseDir, "server")

	_, addr, cancel := startServerNode(t, baseDir, "cursor_primary", getClientTLS(t, baseDir))
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	conn, err := tls.Dial("tcp", addr, replTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sendReplHello(t, conn, "bad-cursor", "1", 1)
	status, body := readReplStatus(t, conn)
	if status != protocol.ResStatusErr {
		t.Fatalf("expected handshake error for mid-frame cursor, got status %d body %q", status, body)
	}
}

func TestReplication_StreamDisconnectsOnPurge(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	replTLS := getRoleTLS(t, baseDir, "server")

	primarySrv, addr, cancel := startServerNode(t, baseDir, "purge_primary", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "purge-key", "purge-val")

	st1 := primarySrv.stores["1"]
	if err := st1.DB.MarkRetention(); err != nil {
		t.Fatal(err)
	}
	st1.RemoveAllReplicas()
	st1.EnforceRetentionPolicy()

	walPath := filepath.Join(baseDir, "purge_primary", "data", "1", "wal", "seg-000001.wal")
	if err := os.Remove(walPath); err != nil {
		t.Fatalf("remove wal segment: %v", err)
	}

	conn, err := tls.Dial("tcp", addr, replTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sendReplHello(t, conn, "purge-reader", "1", 0)
	status, _ := readReplStatus(t, conn)
	if status != protocol.ResStatusOK {
		t.Fatalf("expected handshake OK, got %d", status)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	head := make([]byte, 5)
	_, err = io.ReadFull(conn, head)
	if err == nil {
		t.Fatal("expected stream disconnect after purged log, got frame")
	}
}

func TestReplication_PartialCatchUp_MidLog(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	primarySrv, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "partial_primary", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")

	for i := 0; i < 5; i++ {
		writeKeyVal(t, clientPrimary, fmt.Sprintf("pk%d", i), "v")
	}

	stPrimary := primarySrv.stores["1"]
	head := int64(stPrimary.LastLogOffset())
	var partial []byte
	var partialEnd int64
	offset := int64(0)
	for offset < head {
		seg, next, err := stPrimary.ReadLogRange(offset, 512)
		if err != nil {
			t.Fatal(err)
		}
		if len(seg) == 0 {
			break
		}
		partial = append(partial, seg...)
		partialEnd = next
		if partialEnd >= head/2 {
			break
		}
		offset = next
	}
	if partialEnd <= 0 || len(partial) == 0 {
		t.Fatal("failed to capture partial log prefix")
	}

	replicaSrv, replicaAddr, cancelReplica := startServerNode(t, baseDir, "partial_replica", clientTLS)
	defer cancelReplica()

	if _, err := replicaSrv.stores["1"].ApplyLogRange(partial); err != nil {
		t.Fatalf("seed partial log on replica: %v", err)
	}

	adminReplica := connectClient(t, replicaAddr, adminTLS)
	defer adminReplica.Close()
	selectDatabase(t, adminReplica, "1")
	configureReplication(t, adminReplica, primaryAddr, "1")

	clientReplica := connectClient(t, replicaAddr, clientTLS)
	defer clientReplica.Close()
	selectDatabase(t, clientReplica, "1")

	for i := 5; i < 10; i++ {
		writeKeyVal(t, clientPrimary, fmt.Sprintf("pk%d", i), "v")
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("pk%d", i)
		waitForConditionOrTimeout(t, 5*time.Second, func() bool {
			val := readKey(t, clientReplica, key)
			return string(val) == "v"
		}, "partial catch-up failed for "+key)
	}
}

func TestReplication_QuorumFailure_ServerBusy(t *testing.T) {
	os.Setenv("TS_TEST_QUORUM_TIMEOUT", "200ms")
	defer os.Unsetenv("TS_TEST_QUORUM_TIMEOUT")

	baseDir, clientTLS := setupSharedCertEnv(t)
	_, primaryAddr, cancelPrimary := startServerNodeWithReplicas(t, baseDir, "quorum_fail", clientTLS, 1)
	defer cancelPrimary()
	promoteNodeWithMinReplicas(t, baseDir, primaryAddr, 1, "1")

	client := connectClient(t, primaryAddr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")

	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	key := []byte("qk")
	val := []byte("qv")
	payload := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
	copy(payload[4:], key)
	copy(payload[4+len(key):], val)
	client.AssertStatus(protocol.OpCodeSet, payload, protocol.ResStatusOK)
	client.Send(protocol.OpCodeCommit, nil)
	status, body := client.Read()
	if status != protocol.ResStatusServerBusy {
		t.Fatalf("expected SERVER_BUSY without quorum replica, got %d: %s", status, body)
	}
}

func TestBackup_DoesNotPinRetention(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	primarySrv, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "backup_ret", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	client := connectClient(t, primaryAddr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "bk", "bv")

	st1 := primarySrv.stores["1"]
	if got := st1.MinReplicaOffset(); got != math.MaxUint64 {
		t.Fatalf("expected no retention pin before backup, got %d", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backupDir := filepath.Join(baseDir, "backup_pin")
	_, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:     primaryAddr,
		DBName:   "1",
		OutDir:   backupDir,
		Type:     backup.TypeFull,
		WaitIdle: 100 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	if got := st1.MinReplicaOffset(); got != math.MaxUint64 {
		t.Fatalf("backup consumer must not pin retention, MinReplicaOffset=%d", got)
	}

	if role, ok := st1.ReplicaRole("turnstone-backup"); ok && role != RoleBackup {
		t.Fatalf("expected backup role, got %q", role)
	}
}

func TestReplication_ProductionRetention_ZombieEviction(t *testing.T) {
	os.Setenv("TS_TEST_REPLICA_TIMEOUT", "50ms")
	defer os.Unsetenv("TS_TEST_REPLICA_TIMEOUT")

	baseDir, clientTLS := setupSharedCertEnv(t)
	primarySrv, primaryAddr, cancel := startServerNodeWithRetention(t, baseDir, "zombie_ret", clientTLS, 0)
	defer cancel()
	promoteNode(t, baseDir, primaryAddr, "1")

	st1 := primarySrv.stores["1"]
	client := connectClient(t, primaryAddr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "z", "v")

	head := st1.LastLogOffset()
	st1.RegisterReplica("zombie", uint64(head/2), database.ReplicaRoleServer)
	st1.SetReplicaLastSeenForTest("zombie", time.Now().Add(-time.Minute))

	if st1.MinReplicaOffset() == math.MaxUint64 {
		t.Fatal("expected zombie slot to constrain retention before eviction")
	}

	st1.EvictZombieReplicasNow()

	if st1.MinReplicaOffset() != math.MaxUint64 {
		t.Fatalf("expected zombie evicted, MinReplicaOffset=%d", st1.MinReplicaOffset())
	}
}

func TestReplication_ProductionRetention_SlotsSurviveRestart(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, primaryAddr, cancelPrimary := startServerNodeWithRetention(t, baseDir, "slot_restart", clientTLS, 0)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	_, replicaAddr, cancelReplica := startServerNodeWithRetention(t, baseDir, "slot_follower", clientTLS, 0)
	defer cancelReplica()

	adminReplica := connectClient(t, replicaAddr, adminTLS)
	defer adminReplica.Close()
	selectDatabase(t, adminReplica, "1")
	configureReplication(t, adminReplica, primaryAddr, "1")

	clientPrimary := connectClient(t, primaryAddr, clientTLS)
	defer clientPrimary.Close()
	selectDatabase(t, clientPrimary, "1")
	writeKeyVal(t, clientPrimary, "slot-key", "slot-val")

	slotsPath := filepath.Join(baseDir, "slot_restart", "data", "1", "repl.slots")
	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		_, err := os.Stat(slotsPath)
		return err == nil
	}, "repl.slots not persisted on leader")

	cancelPrimary()

	restartedSrv, _, cancelRestart := startServerNodeWithRetention(t, baseDir, "slot_restart", clientTLS, 0)
	defer cancelRestart()

	data, err := os.ReadFile(slotsPath)
	if err != nil {
		t.Fatalf("repl.slots missing after restart: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("repl.slots empty after restart")
	}

	st1 := restartedSrv.stores["1"]
	if got := st1.MinReplicaOffset(); got == math.MaxUint64 {
		t.Fatal("expected persisted replica slot offset after restart")
	}
	if role, ok := st1.ReplicaRole("slot_follower"); !ok || role != database.ReplicaRoleServer {
		t.Fatalf("expected reloaded server slot, ok=%v role=%q", ok, role)
	}
}
