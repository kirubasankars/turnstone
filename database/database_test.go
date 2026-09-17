// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package database

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/protocol"
)

func walLogicalUsed(t *testing.T, f *os.File) int64 {
	t.Helper()
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() < 16 {
		return stat.Size()
	}
	buf := make([]byte, 16)
	if _, err := f.ReadAt(buf, stat.Size()-16); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(buf[0:4]) != 0x54534631 {
		return stat.Size()
	}
	return int64(binary.BigEndian.Uint64(buf[4:12]))
}

func putKV(t *testing.T, db *Database, key, val string) {
	t.Helper()
	tx := db.NewTransaction(true)
	if err := tx.Put([]byte(key), []byte(val)); err != nil {
		tx.Discard()
		t.Fatalf("put %s: %v", key, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s: %v", key, err)
	}
}

func TestDatabase_Recover_Basic(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s1, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Failed to create initial database: %v", err)
	}

	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		putKV(t, s1, k, "val-"+k)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("Failed to close database 1: %v", err)
	}

	s2, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Failed to create recovered database: %v", err)
	}
	defer s2.Close()

	for _, k := range keys {
		val, err := s2.Get(k)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", k, err)
		}
		expected := "val-" + k
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", k, expected, string(val))
		}
	}
}

func TestDatabase_Recover_CRC_Corruption(t *testing.T) {
	os.Setenv("TS_TEST_LOG_TRUNCATE", "true")
	defer os.Unsetenv("TS_TEST_LOG_TRUNCATE")
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s1, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	putKV(t, s1, "key1", "val1")
	putKV(t, s1, "key2", "val2")
	s1.Close()

	logPath := filepath.Join(dir, "wal", "seg-000001.wal")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("No wal segment found in %s: %v", dir, err)
	}

	f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	used := walLogicalUsed(t, f)
	if used < 2 {
		t.Fatalf("expected WAL used bytes, got %d", used)
	}
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, used-1); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, used-1); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Failed to recover database: %v", err)
	}
	defer s2.Close()

	val, err := s2.Get("key1")
	if err != nil {
		t.Errorf("Expected key1 to survive corruption, got error: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("Expected val1, got %s", val)
	}

	_, err = s2.Get("key2")
	if err != protocol.ErrKeyNotFound {
		t.Errorf("Expected key2 to be dropped due to CRC failure, got: %v", err)
	}
}

func TestDatabase_Recover_PartialWrite(t *testing.T) {
	os.Setenv("TS_TEST_LOG_TRUNCATE", "true")
	defer os.Unsetenv("TS_TEST_LOG_TRUNCATE")
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s1, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	putKV(t, s1, "key1", "val1")
	s1.Close()

	logPath := filepath.Join(dir, "wal", "seg-000001.wal")
	f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	used := walLogicalUsed(t, f)
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, used); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], 0x54534631)
	binary.BigEndian.PutUint64(buf[4:12], uint64(used+4))
	binary.BigEndian.PutUint32(buf[12:16], crc32.Checksum(buf[:12], crc32.MakeTable(crc32.Castagnoli)))
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(buf, stat.Size()-16); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Recovery failed on partial write: %v", err)
	}
	defer s2.Close()

	if _, err := s2.Get("key1"); err != nil {
		t.Error("key1 lost during partial write recovery")
	}
}

func TestDatabase_Replication_Quorum(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s, err := Open(context.Background(), dir, logger, 1, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.RegisterReplica("replica-1", 0, "server")

	done := make(chan error)
	go func() {
		tx := s.NewTransaction(true)
		if err := tx.Put([]byte("k"), []byte("v")); err != nil {
			tx.Discard()
			done <- err
			return
		}
		if err := tx.Commit(); err != nil {
			done <- err
			return
		}
		done <- s.WaitForQuorum(s.LastLogOffset(), 0, nil)
	}()

	select {
	case err := <-done:
		t.Fatalf("Write returned before quorum was met. Err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	s.UpdateReplicaOffset("replica-1", s.LastLogOffset())

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Write timed out after quorum met")
	}
}

func TestDatabase_CommitPuts(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k1", "v1")
	putKV(t, s, "k2", "v2")

	val, err := s.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Errorf("Want v1, got %s", val)
	}

	val2, err := s.Get("k2")
	if err != nil {
		t.Fatal(err)
	}
	if string(val2) != "v2" {
		t.Errorf("Want v2, got %s", val2)
	}
}

func TestStats_ConflictsAndStorage(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	stats := s.Stats()
	if stats.Conflicts != 0 {
		t.Errorf("expected 0 conflicts, got %d", stats.Conflicts)
	}

	tx1 := s.DB.NewTransaction(true)
	tx1.Put([]byte("key"), []byte("val1"))

	tx2 := s.DB.NewTransaction(true)
	tx2.Put([]byte("key"), []byte("val2"))

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit failed: %v", err)
	}

	if err := tx2.Commit(); err == nil {
		t.Error("tx2 should have failed with conflict")
	}

	stats = s.Stats()
	if stats.Conflicts != 1 {
		t.Errorf("expected 1 conflict, got %d", stats.Conflicts)
	}

	putKV(t, s, "k1", "v1")
	putKV(t, s, "k2", "v2")

	stats = s.Stats()
	if stats.LogSize == 0 {
		t.Error("expected >0 bytes log logical size")
	}
	if stats.LogSize != stats.Offset {
		t.Errorf("before truncation log_bytes should equal offset, log=%d offset=%d", stats.LogSize, stats.Offset)
	}
	if stats.LogAllocated == 0 {
		t.Error("expected >0 allocated log bytes")
	}

	detail := s.StorageDetail()
	if len(detail.Segments) == 0 {
		t.Fatal("expected at least one wal segment")
	}
	if detail.WalLiveBytes == 0 {
		t.Fatal("expected wal live bytes after writes")
	}
	if detail.IndexAllocatedBytes <= detail.IndexLiveBytes {
		t.Fatalf("allocated buffers should exceed live payload, allocated=%d live=%d", detail.IndexAllocatedBytes, detail.IndexLiveBytes)
	}
	if detail.IndexArenaBytes < detail.IndexLiveBytes {
		t.Fatalf("bump arena should be at least live payload, arena=%d live=%d", detail.IndexArenaBytes, detail.IndexLiveBytes)
	}
	if detail.HashShards < 1 {
		t.Fatalf("expected nonempty hash shards, got %d", detail.HashShards)
	}
	hash := s.IndexHashMetrics()
	if hash.ShardsUsed != detail.HashShards || hash.ArenaBytes != detail.IndexArenaBytes || hash.AllocatedBytes != detail.IndexAllocatedBytes || hash.LiveBytes != detail.IndexLiveBytes {
		t.Fatalf("IndexHashMetrics mismatch: %+v detail shards=%d arena=%d allocated=%d live=%d", hash, detail.HashShards, detail.IndexArenaBytes, detail.IndexAllocatedBytes, detail.IndexLiveBytes)
	}
	if s.WalSegmentCount() != len(detail.Segments) {
		t.Fatalf("segment count %d != %d", s.WalSegmentCount(), len(detail.Segments))
	}
}

func TestDatabase_ReplicaLag(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	putKV(t, s, "k1", "v1")
	head := s.LastLogOffset()

	s.RegisterReplica("r1", head, "server")
	stats := s.Stats()
	if stats.ReplicaLag != 0 {
		t.Errorf("expected 0 lag, got %d", stats.ReplicaLag)
	}

	putKV(t, s, "k1", "v1")
	newHead := s.LastLogOffset()

	stats = s.Stats()
	expectedLag := newHead - head
	if stats.ReplicaLag != expectedLag {
		t.Errorf("expected lag %d, got %d", expectedLag, stats.ReplicaLag)
	}
	if len(stats.Replicas) != 1 {
		t.Fatalf("expected 1 replica in stats, got %d", len(stats.Replicas))
	}
	if stats.Replicas[0].ID != "r1" || stats.Replicas[0].Lag != expectedLag {
		t.Fatalf("unexpected replica info: %+v", stats.Replicas[0])
	}
	if stats.ServerReplicas != 1 {
		t.Fatalf("expected 1 server replica, got %d", stats.ServerReplicas)
	}
}

func TestDatabase_ReplicaLag_ReportsSlowestReplica(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k1", "v1")
	head := s.LastLogOffset()

	s.RegisterReplica("caught-up", head, "server")
	s.RegisterReplica("slow", head/2, "server")

	putKV(t, s, "k2", "v2")
	newHead := s.LastLogOffset()
	wantSlowLag := newHead - head/2

	stats := s.Stats()
	if stats.ReplicaLag != wantSlowLag {
		t.Fatalf("replica_lag=%d want slowest lag %d", stats.ReplicaLag, wantSlowLag)
	}
	if len(stats.Replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(stats.Replicas))
	}
	if stats.ServerReplicas != 2 {
		t.Fatalf("expected 2 server replicas, got %d", stats.ServerReplicas)
	}
}

func TestDatabase_ReplicaLag_IgnoresNonServerRoles(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k1", "v1")
	head := s.LastLogOffset()
	s.RegisterReplica("backup", head/2, ReplicaRoleBackup)
	s.RegisterReplica("admin", head/3, ReplicaRoleAdmin)

	putKV(t, s, "k2", "v2")
	stats := s.Stats()
	if stats.ReplicaLag != 0 {
		t.Fatalf("backup/admin should not set replica_lag, got %d", stats.ReplicaLag)
	}
	if stats.ServerReplicas != 0 {
		t.Fatalf("expected 0 server replicas, got %d", stats.ServerReplicas)
	}

	s.RegisterReplica("server", head, ReplicaRoleServer)
	stats = s.Stats()
	newHead := s.LastLogOffset()
	want := uint64(newHead) - uint64(head)
	if stats.ReplicaLag != want {
		t.Fatalf("replica_lag=%d want server lag %d", stats.ReplicaLag, want)
	}
	if stats.ServerReplicas != 1 {
		t.Fatalf("expected 1 server replica, got %d", stats.ServerReplicas)
	}
}

func TestIsValidReplicationCursor_HeadAndZero(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k", "v")
	head := uint64(s.LastLogOffset())

	if !s.IsValidReplicationCursor(0) {
		t.Fatal("offset 0 should be valid for full sync")
	}
	if !s.IsValidReplicationCursor(head) {
		t.Fatal("head offset should be valid")
	}
	if s.IsValidReplicationCursor(head + 1) {
		t.Fatal("offset beyond head should be invalid")
	}
}

func TestIsValidReplicationCursor_RejectsMidFrame(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k", "v")
	head := s.LastLogOffset()
	if head <= 1 {
		t.Fatalf("expected head > 1, got %d", head)
	}
	if s.IsValidReplicationCursor(1) {
		t.Fatal("mid-frame offset should be rejected")
	}
	if !s.DB.IsValidFrameOffset(0) {
		t.Fatal("frame boundary at 0 expected")
	}
}

func TestMinReplicaOffset_ExcludesBackupRole(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k", "v")
	head := uint64(s.LastLogOffset())

	s.RegisterReplica("backup", head/2, ReplicaRoleBackup)
	if got := s.MinReplicaOffset(); got != math.MaxUint64 {
		t.Fatalf("backup role should not pin retention, got %d", got)
	}

	s.RegisterReplica("server", head/3, ReplicaRoleServer)
	if got := s.MinReplicaOffset(); got != head/3 {
		t.Fatalf("server role min offset = %d, want %d", got, head/3)
	}
}

func TestWaitForQuorum_IgnoresNonServerRoles(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 1, "none", 90, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	putKV(t, s, "k", "v")
	target := uint64(s.LastLogOffset())

	s.RegisterReplica("admin-only", target, ReplicaRoleAdmin)
	s.RegisterReplica("backup-only", target, ReplicaRoleBackup)

	done := make(chan error, 1)
	go func() {
		done <- s.WaitForQuorum(target, 200*time.Millisecond, nil)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected quorum timeout when only non-server roles acked")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("WaitForQuorum did not return")
	}

	s.RegisterReplica("server", target, ReplicaRoleServer)
	if err := s.WaitForQuorum(target, time.Second, nil); err != nil {
		t.Fatalf("server role should satisfy quorum: %v", err)
	}
}

func TestDatabase_BasicInit(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s, err := Open(context.Background(), dir, logger, 0, "none", 90, 0)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer s.Close()

	putKV(t, s, "cache_test", "value")

	val, err := s.Get("cache_test")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Unexpected value: %s", val)
	}
}
