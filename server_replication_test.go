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

// Helper for polling
func waitForCondition(t *testing.T, timeout time.Duration, check func() bool, errMsg string) {
	start := time.Now()
	for time.Since(start) < timeout {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(errMsg)
}

// TestServer_ReplicaOf_Integration verifies that a follower can replicate from a leader
// and that 'replicaof no one' stops the replication.
func TestServer_ReplicaOf_Integration(t *testing.T) {
	// 1. Shared Environment Setup (Same CA for Leader and Follower)
	baseDir := t.TempDir()
	certsDir := filepath.Join(baseDir, "certs")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Generate shared certs once
	cfg := Config{
		TLSCertFile: "certs/server.crt",
		TLSKeyFile:  "certs/server.key",
		TLSCAFile:   "certs/ca.crt",
		Databases:   []DatabaseConfig{{Name: "default"}},
	}
	if err := GenerateConfigArtifacts(baseDir, cfg, filepath.Join(baseDir, "config.json")); err != nil {
		t.Fatal(err)
	}

	// Helper to create a server instance sharing the certs
	createSrv := func(subDir string) (*Server, string) {
		dir := filepath.Join(baseDir, subDir)
		dbPath := filepath.Join(dir, "data", "default")
		store, err := NewStore(dbPath, logger, true, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		stores := map[string]*Store{"default": store}

		// Setup RM
		clientCert, _ := tls.LoadX509KeyPair(filepath.Join(certsDir, "client.crt"), filepath.Join(certsDir, "client.key"))
		caCert, _ := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCert)
		tlsConf := &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pool, InsecureSkipVerify: true}
		rm := NewReplicationManager(stores, tlsConf, logger)

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
		return srv, dir
	}

	// 2. Start Leader
	leaderSrv, _ := createSrv("leader")
	leaderCtx, leaderCancel := context.WithCancel(context.Background())
	defer leaderCancel()
	go leaderSrv.Run(leaderCtx)
	time.Sleep(50 * time.Millisecond) // Wait for listen
	leaderAddr := leaderSrv.listener.Addr().String()

	// 3. Start Follower
	followerSrv, followerDir := createSrv("follower")
	followerCtx, followerCancel := context.WithCancel(context.Background())
	defer followerCancel()
	go followerSrv.Run(followerCtx)
	time.Sleep(50 * time.Millisecond)
	followerAddr := followerSrv.listener.Addr().String()

	// 4. Connect Client to Follower
	tlsConfig := getClientTLS(t, followerDir) // Uses shared CA from baseDir/certs copy in setup?
	// Note: createSrv points to baseDir/certs, so we can use baseDir for client config
	tlsConfig = getClientTLS(t, baseDir)
	clientFollower := connectClient(t, followerAddr, tlsConfig)
	defer clientFollower.Close()

	// 5. Connect Client to Leader
	clientLeader := connectClient(t, leaderAddr, tlsConfig)
	defer clientLeader.Close()

	// --- PHASE 1: START REPLICATION ---

	// Send REPLICAOF LeaderAddr Default to Follower
	// Protocol: [AddrLen][Addr][DBName]
	addrBytes := []byte(leaderAddr)
	dbBytes := []byte("default")
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)

	clientFollower.AssertStatus(OpCodeReplicaOf, payload, ResStatusOK)

	// Write K1=V1 to Leader
	k1 := []byte("k1")
	v1 := []byte("v1")
	setPayload := make([]byte, 4+len(k1)+len(v1))
	binary.BigEndian.PutUint32(setPayload[0:4], uint32(len(k1)))
	copy(setPayload[4:], k1)
	copy(setPayload[4+len(k1):], v1)

	clientLeader.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	clientLeader.AssertStatus(OpCodeSet, setPayload, ResStatusOK)
	clientLeader.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// Verify K1 on Follower (Polling)
	waitForCondition(t, 2*time.Second, func() bool {
		clientFollower.AssertStatus(OpCodeBegin, nil, ResStatusOK)
		got, status := clientFollower.ReadStatus(OpCodeGet, k1)
		clientFollower.AssertStatus(OpCodeCommit, nil, ResStatusOK)
		return status == ResStatusOK && bytes.Equal(got, v1)
	}, "Replication failed for K1")

	// --- PHASE 2: STOP REPLICATION ---

	// Send REPLICAOF NO ONE (Empty addr string)
	// Payload: [0000][][default]
	stopPayload := make([]byte, 4+0+len(dbBytes))
	binary.BigEndian.PutUint32(stopPayload[0:4], 0)
	copy(stopPayload[4:], dbBytes)

	clientFollower.AssertStatus(OpCodeReplicaOf, stopPayload, ResStatusOK)

	// Write K2=V2 to Leader
	k2 := []byte("k2")
	v2 := []byte("v2")
	setPayload2 := make([]byte, 4+len(k2)+len(v2))
	binary.BigEndian.PutUint32(setPayload2[0:4], uint32(len(k2)))
	copy(setPayload2[4:], k2)
	copy(setPayload2[4+len(k2):], v2)

	clientLeader.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	clientLeader.AssertStatus(OpCodeSet, setPayload2, ResStatusOK)
	clientLeader.AssertStatus(OpCodeCommit, nil, ResStatusOK)

	// Ensure we wait long enough to be sure it DOESN'T replicate
	time.Sleep(300 * time.Millisecond)

	// Verify K2 is NOT on Follower
	clientFollower.AssertStatus(OpCodeBegin, nil, ResStatusOK)
	clientFollower.AssertStatus(OpCodeGet, k2, ResStatusNotFound)
	clientFollower.AssertStatus(OpCodeCommit, nil, ResStatusOK)
}
