package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// TestIntegration_Replication performs an end-to-end test of the Leader-Follower architecture.
func TestIntegration_Replication(t *testing.T) {
	// 1. Setup Leader
	leaderDir, cleanupLeader := createTempDir(t)
	defer cleanupLeader()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	leaderStore, _ := NewStore(leaderDir, logger, true, false, false, 0)
	defer leaderStore.Close()

	leaderServer := NewServer(":0", leaderStore, logger, 10, 10, "", false)
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

	// FIX: Initialize Meta bucket explicitly so CDC can save state immediately.
	// In a real run, this might happen on first flush, but CDC needs it instantly.
	followerStore.bolt.Update(func(tx *bbolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte(BoltBucketMeta))
		return nil
	})

	// Follower Server (Read-Only)
	followerServer := NewServer(":0", followerStore, logger, 10, 10, "", true)
	go func() { _ = followerServer.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	followerAddr := followerServer.listener.Addr().String()

	// 3. Start CDC Client (Replication Link)
	// We mimic the logic in main.go
	handler := NewReplicaHandler(followerStore)
	cdcClient := NewCDCClient(leaderAddr, "", handler, 0, 0)
	go func() {
		if err := cdcClient.Run(ctx); err != nil && err != context.Canceled {
			t.Logf("CDC Client stopped: %v", err)
		}
	}()

	// Allow connection time
	time.Sleep(100 * time.Millisecond)

	// 4. Client writes to Leader
	lConn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("Failed to dial leader: %v", err)
	}
	defer lConn.Close()

	mustSend(t, lConn, OpCodeBegin, nil)
	mustSend(t, lConn, OpCodeSet, buildSetPayload("repl_key", "repl_val"))
	mustSend(t, lConn, OpCodeCommit, nil)

	// 5. Wait for replication (async)
	time.Sleep(200 * time.Millisecond)

	// 6. Verify Data on Follower
	fConn, err := net.Dial("tcp", followerAddr)
	if err != nil {
		t.Fatalf("Failed to dial follower: %v", err)
	}
	defer fConn.Close()

	// FIX: Use Direct Store Access to verify replication.
	// The current Server implementation blocks OpCodeBegin entirely in ReadOnly mode,
	// preventing standard network reads. We verify the data reached the disk directly.
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
	// We expect OpCodeBegin to fail because the server is ReadOnly.
	status, _, _ := sendCommand(fConn, OpCodeBegin, nil)
	if status == ResStatusOK {
		t.Errorf("Follower accepted OpCodeBegin! Expected error (Server is read-only).")
	}
}
