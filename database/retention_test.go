// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package database

import (
	"context"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"
)

func openTestDB(t *testing.T, dir string) *Database {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return s
}

func TestEnforceRetentionPolicy_LeaderConstraintSetsScanFloor(t *testing.T) {
	dir := t.TempDir()
	s := openTestDB(t, dir)
	defer s.Close()

	putKV(t, s, "k", "v")
	if err := s.DB.MarkRetention(); err != nil {
		t.Fatal(err)
	}
	head := s.LastLogOffset()
	leaderSafe := head / 2
	if leaderSafe <= 0 {
		leaderSafe = 1
	}

	s.SetLeaderRetainOffset(uint64(leaderSafe))
	s.EnforceRetentionPolicy()

	if got := s.DB.ScanFloor(); got != int64(leaderSafe) {
		t.Fatalf("ScanFloor=%d want leader safe point %d", got, leaderSafe)
	}
}

func TestEnforceRetentionPolicy_ReplicaLagSetsScanFloor(t *testing.T) {
	dir := t.TempDir()
	s := openTestDB(t, dir)
	defer s.Close()

	putKV(t, s, "k1", "v1")
	lagPoint := s.LastLogOffset()
	putKV(t, s, "k2", "v2")
	if err := s.DB.MarkRetention(); err != nil {
		t.Fatal(err)
	}
	retentionMark := s.DB.RetentionOffset()

	s.RegisterReplica("lagging", uint64(lagPoint), "server")
	s.EnforceRetentionPolicy()

	want := int64(lagPoint)
	if want > retentionMark {
		want = retentionMark
	}
	if got := s.DB.ScanFloor(); got != want {
		t.Fatalf("ScanFloor=%d want replica-lag floor %d (retention=%d)", got, want, retentionMark)
	}
}

func TestEvictZombieReplica_UnblocksMinReplicaOffset(t *testing.T) {
	dir := t.TempDir()
	s := openTestDB(t, dir)
	defer s.Close()

	putKV(t, s, "k", "v")
	head := s.LastLogOffset()
	s.RegisterReplica("zombie", head/2, "server")

	s.mu.Lock()
	slot := s.replicas["zombie"]
	slot.LastSeen = time.Now().Add(-2 * time.Minute)
	slot.Offset = head / 3
	s.mu.Unlock()

	if s.MinReplicaOffset() == math.MaxUint64 {
		t.Fatal("expected registered replica offset")
	}

	s.evictZombieReplicas()

	if s.MinReplicaOffset() != math.MaxUint64 {
		t.Fatalf("expected zombie evicted, MinReplicaOffset=%d", s.MinReplicaOffset())
	}
}

func TestEnforceRetentionPolicy_RunsWalMaintenance(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k", "keep")
	if err := s.DB.MarkRetention(); err != nil {
		t.Fatal(err)
	}
	beforeFloor := s.DB.ScanFloor()

	s.EnforceRetentionPolicy()

	if s.DB.ScanFloor() < beforeFloor {
		t.Fatalf("expected scan floor to advance, before=%d after=%d", beforeFloor, s.DB.ScanFloor())
	}

	val, err := s.Get("k")
	if err != nil || string(val) != "keep" {
		t.Fatalf("expected readable value after maintenance, err=%v val=%q", err, val)
	}
}
