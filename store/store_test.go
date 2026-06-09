// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package store

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

// NOTE: Isolation tests (Snapshot Isolation, Write Skew, etc.) have been removed
// because the Store API currently abstracts away the internal Engine's transaction
// handles (AcquireSnapshot/ReleaseSnapshot). The Store exposes atomic Batch application
// and Latest-Committed reads.

// TestStore_Recover_Basic verifies that the store can recover data from the WAL.
func TestStore_Recover_Basic(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Initialize Store and write data
	// Signature: (dir, logger, minReplicas, walStrategy, maxDiskUsage, blockCacheSize)
	s1, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("Failed to create initial store: %v", err)
	}

	keys := []string{"alpha", "beta", "gamma"}

	for _, k := range keys {
		entry := protocol.LogEntry{
			OpCode: protocol.OpJournalSet,
			Key:    []byte(k),
			Value:  []byte("val-" + k),
		}
		if err := s1.ApplyBatch([]protocol.LogEntry{entry}); err != nil {
			t.Fatalf("ApplyBatch failed for key %s: %v", k, err)
		}
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("Failed to close store 1: %v", err)
	}

	// 2. Re-open Store
	s2, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("Failed to create recovered store: %v", err)
	}
	defer s2.Close()

	// 3. Verify Data
	// Note: KeyCount removed from stats, so we verify by reading values directly.
	for _, k := range keys {
		val, err := s2.Get(k)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", k, err)
		}
		expected := "val-" + k
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", k, expected, val)
		}
	}
}

func TestStore_Recover_CRC_Corruption(t *testing.T) {
	os.Setenv("TS_TEST_WAL_TRUNCATE", "true")
	defer os.Unsetenv("TS_TEST_WAL_TRUNCATE")
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create Store and write two entries
	s1, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatal(err)
	}

	// Write Entry 1 (Valid)
	if err := s1.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("key1"), Value: []byte("val1")}}); err != nil {
		t.Fatal(err)
	}
	// Write Entry 2 (To be corrupted)
	if err := s1.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("key2"), Value: []byte("val2")}}); err != nil {
		t.Fatal(err)
	}

	s1.Close()

	// 2. Corrupt the log manually
	logPath := filepath.Join(dir, "data.log")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("No data.log found in %s", dir)
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

	// 3. Re-open Store (should truncate corrupt tail)
	s2, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("Failed to recover store: %v", err)
	}
	defer s2.Close()

	// 4. Verify Entry 1 exists
	val, err := s2.Get("key1")
	if err != nil {
		t.Errorf("Expected key1 to survive corruption, got error: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("Expected val1, got %s", val)
	}

	// 5. Verify Entry 2 is gone
	_, err = s2.Get("key2")
	if err != protocol.ErrKeyNotFound {
		t.Errorf("Expected key2 to be dropped due to CRC failure, got: %v", err)
	}
}

func TestStore_Recover_PartialWrite(t *testing.T) {
	os.Setenv("TS_TEST_WAL_TRUNCATE", "true")
	defer os.Unsetenv("TS_TEST_WAL_TRUNCATE")
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create Store and write data
	s1, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatal(err)
	}
	s1.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("key1"), Value: []byte("val1")}})
	s1.Close()

	// 2. Append garbage at EOF
	logPath := filepath.Join(dir, "data.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// 3. Re-open
	s2, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("Recovery failed on partial write: %v", err)
	}
	defer s2.Close()

	// 4. Verify Valid Data remains
	if _, err := s2.Get("key1"); err != nil {
		t.Error("key1 lost during partial write recovery")
	}
}

func TestStore_Replication_Quorum(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create Store with MinReplicas = 1
	// This ensures that any write operation must wait for at least 1 replica to acknowledge.
	s, err := NewStore(context.Background(), dir, logger, 1, "time", 90)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Register a connected replica that hasn't acked anything yet.
	// ApplyBatch fails fast (before writing) if there are zero healthy
	// replicas, so a healthy-but-behind replica is needed to actually
	// exercise the quorum-wait path below.
	s.RegisterReplica("replica-1", 0, "server")

	// 3. Perform Write (Should Block)
	// We run this in a goroutine because ApplyBatch is synchronous and will block until quorum is met.
	done := make(chan error)
	go func() {
		done <- s.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("k"), Value: []byte("v")}})
	}()

	// 4. Verify it's blocked
	select {
	case err := <-done:
		t.Fatalf("Write returned before quorum was met. Err: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected behavior: timeout because write is blocked
	}

	// 5. Ack the write.
	// Under eager logging, a single-entry transaction writes multiple WAL
	// records (BEGIN, SET, COMMIT), so the quorum target is the COMMIT
	// record's opID, not a fixed LogSeq of 1. Query the actual opID the
	// write is waiting on before acking it.
	s.UpdateReplicaLogSeq("replica-1", s.DB.LastOpID())

	// 5. Verify Unblock
	// Now that quorum is met, the write should complete successfully.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Write timed out after quorum met")
	}
}

func TestStore_Replication_ApplyBatch(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Replica store (MinReplicas=0)
	s, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Simulate incoming batch from Primary
	entries := []protocol.LogEntry{
		{LogSeq: 100, OpCode: protocol.OpJournalSet, Key: []byte("k1"), Value: []byte("v1")},
		{LogSeq: 101, OpCode: protocol.OpJournalSet, Key: []byte("k2"), Value: []byte("v2")},
	}

	if err := s.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// Verify Data is readable
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

func TestStoreStats_ConflictsAndStorage(t *testing.T) {
	dir, _ := os.MkdirTemp("", "store_stats_test")
	defer os.RemoveAll(dir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	s, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	defer s.Close()

	// 1. Initial Stats
	stats := s.Stats()
	if stats.Conflicts != 0 {
		t.Errorf("expected 0 conflicts, got %d", stats.Conflicts)
	}

	// 2. Generate a Conflict
	// Tx 1
	tx1 := s.DB.NewTransaction(true)
	tx1.Put([]byte("key"), []byte("val1"))

	// Tx 2 (concurrent)
	tx2 := s.DB.NewTransaction(true)
	tx2.Put([]byte("key"), []byte("val2"))

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit failed: %v", err)
	}

	// Tx 2 should fail
	if err := tx2.Commit(); err == nil {
		t.Error("tx2 should have failed with conflict")
	}

	// 3. Verify Conflict Count
	stats = s.Stats()
	if stats.Conflicts != 1 {
		t.Errorf("expected 1 conflict, got %d", stats.Conflicts)
	}

	// 4. Write data to generate log usage
	entries := []protocol.LogEntry{
		{Key: []byte("k1"), Value: []byte("v1"), OpCode: protocol.OpJournalSet},
		{Key: []byte("k2"), Value: []byte("v2"), OpCode: protocol.OpJournalSet},
	}
	if err := s.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// 5. Verify Storage Metrics
	stats = s.Stats()
	if stats.WALFiles != 1 {
		t.Errorf("expected 1 log file, got %d", stats.WALFiles)
	}
	if stats.WALSize == 0 {
		t.Error("expected >0 bytes log logical size")
	}
}

func TestStore_ReplicaLag(t *testing.T) {
	dir, _ := os.MkdirTemp("", "store_lag_test")
	defer os.RemoveAll(dir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	s, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	defer s.Close()

	// Write some data to advance log seq
	entries := []protocol.LogEntry{
		{Key: []byte("k1"), Value: []byte("v1"), OpCode: protocol.OpJournalSet},
	}
	s.ApplyBatch(entries)
	head := s.DB.LastOpID()

	// Register Replica at head
	s.RegisterReplica("r1", head, "server")
	stats := s.Stats()
	if stats.ReplicaLag != 0 {
		t.Errorf("expected 0 lag, got %d", stats.ReplicaLag)
	}

	// Write more data
	s.ApplyBatch(entries)
	newHead := s.DB.LastOpID()

	// Check Lag (should be newHead - head)
	stats = s.Stats()
	expectedLag := newHead - head
	if stats.ReplicaLag != expectedLag {
		t.Errorf("expected lag %d, got %d", expectedLag, stats.ReplicaLag)
	}
}

// TestStore_BasicInit verifies that the store initializes and accepts writes.
func TestStore_BasicInit(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s, err := NewStore(context.Background(), dir, logger, 0, "time", 90)
	if err != nil {
		t.Fatalf("Failed to create store with block cache: %v", err)
	}
	defer s.Close()

	// 2. Perform operations to ensure stability
	entry := protocol.LogEntry{
		OpCode: protocol.OpJournalSet,
		Key:    []byte("cache_test"),
		Value:  []byte("value"),
	}
	if err := s.ApplyBatch([]protocol.LogEntry{entry}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	val, err := s.Get("cache_test")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Unexpected value: %s", val)
	}
}
