package main

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// TestIntegration_Replication performs an end-to-end test of the Leader-Follower architecture.
func TestIntegration_Replication(t *testing.T) {
	// 0. Setup TLS
	ca, srvCert, srvKey, cliCert, cliKey, clientTLS := setupSecurity(t)

	// 1. Setup Leader
	leaderDir, cleanupLeader := createTempDir(t)
	defer cleanupLeader()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	leaderStore, _ := NewStore(leaderDir, logger, true, false, false, 0)
	defer leaderStore.Close()

	leaderServer := NewServer(":0", "", []*Store{leaderStore}, logger, 10, 10, false, srvCert, srvKey, ca)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = leaderServer.Run(ctx) }()

	// Wait for Leader
	time.Sleep(50 * time.Millisecond)
	leaderAddr := leaderServer.listener.Addr().String()

	// 2. Setup Follower
	followerDir, cleanupFollower := createTempDir(t)
	defer cleanupFollower()

	followerStore, _ := NewStore(followerDir, logger, true, false, false, 0)
	defer followerStore.Close()

	// Initialize Meta bucket explicitly so CDC can save state immediately.
	followerStore.bolt.Update(func(tx *bbolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte(BoltBucketMeta))
		return nil
	})

	// Follower Server (Read-Only)
	followerServer := NewServer(":0", "", []*Store{followerStore}, logger, 10, 10, true, srvCert, srvKey, ca)
	go func() { _ = followerServer.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	followerAddr := followerServer.listener.Addr().String()

	// 3. Start CDC Client (Replication Link)
	handler := NewReplicaHandler(followerStore)
	// Pass TLS cert paths to CDC Client
	cdcClient := NewCDCClient(leaderAddr, handler, 0, 0, 0, cliCert, cliKey, ca)
	go func() {
		if err := cdcClient.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("CDC Client stopped: %v", err)
		}
	}()

	// Allow connection time
	time.Sleep(100 * time.Millisecond)

	// 4. Client writes to Leader (via mTLS)
	lConn, err := tls.Dial("tcp", leaderAddr, clientTLS)
	if err != nil {
		t.Fatalf("Failed to dial leader: %v", err)
	}
	defer lConn.Close()

	mustSend(t, lConn, OpCodeBegin, nil)
	mustSend(t, lConn, OpCodeSet, buildSetPayload("repl_key", "repl_val"))
	mustSend(t, lConn, OpCodeCommit, nil)

	// 5. Wait for replication (async)
	time.Sleep(200 * time.Millisecond)

	// 6. Verify Data on Follower (Direct Store Access for verification)
	ver, gen := followerStore.AcquireSnapshot()
	defer followerStore.ReleaseSnapshot(ver)

	val, err := followerStore.Get("repl_key", ver, gen)
	if err != nil {
		t.Fatalf("Follower Direct Get failed: %v", err)
	}
	if string(val) != "repl_val" {
		t.Errorf("Follower data mismatch. Want 'repl_val', got '%s'", string(val))
	}

	// 7. Verify Follower Enforces Read-Only
	fConn, err := tls.Dial("tcp", followerAddr, clientTLS)
	if err != nil {
		t.Fatalf("Failed to dial follower: %v", err)
	}
	defer fConn.Close()

	// Start Tx on Follower
	mustSend(t, fConn, OpCodeBegin, nil)

	// Try SET -> Should fail with Err (ReadOnly)
	status, _, _ := sendCommand(fConn, OpCodeSet, buildSetPayload("fail", "fail"))
	if status == ResStatusOK {
		t.Errorf("Follower accepted OpCodeSet! Expected error (Server is read-only).")
	}
}
