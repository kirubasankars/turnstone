// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package database

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/protocol"
)

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

	s1, err := Open(context.Background(), dir, logger, 0, "none", 90)
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

	s2, err := Open(context.Background(), dir, logger, 0, "none", 90)
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

	s1, err := Open(context.Background(), dir, logger, 0, "none", 90)
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
	stat, _ := f.Stat()
	size := stat.Size()

	b := make([]byte, 1)
	if _, err := f.ReadAt(b, size-1); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, size-1); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(context.Background(), dir, logger, 0, "none", 90)
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

	s1, err := Open(context.Background(), dir, logger, 0, "none", 90)
	if err != nil {
		t.Fatal(err)
	}
	putKV(t, s1, "key1", "val1")
	s1.Close()

	logPath := filepath.Join(dir, "wal", "seg-000001.wal")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(context.Background(), dir, logger, 0, "none", 90)
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

	s, err := Open(context.Background(), dir, logger, 1, "none", 90)
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

	s, err := Open(context.Background(), dir, logger, 0, "none", 90)
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
	s, err := Open(context.Background(), dir, logger, 0, "none", 90)
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
	if stats.LogAllocated == 0 {
		t.Error("expected >0 allocated log bytes")
	}
}

func TestDatabase_ReplicaLag(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := Open(context.Background(), dir, logger, 0, "none", 90)
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
}

func TestDatabase_BasicInit(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s, err := Open(context.Background(), dir, logger, 0, "none", 90)
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
