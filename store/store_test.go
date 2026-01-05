// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package store

import (
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

	// 1. Initialize Store and write data (isSystem=false)
	s1, err := NewStore(dir, logger, true, 0, false)
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

	// 2. Re-open Store (isSystem=false)
	s2, err := NewStore(dir, logger, true, 0, false)
	if err != nil {
		t.Fatalf("Failed to create recovered store: %v", err)
	}
	defer s2.Close()

	// 3. Verify Data
	stats := s2.Stats()
	// Expect 3 user keys.
	if stats.KeyCount < 3 {
		t.Errorf("Expected at least 3 keys, got %d", stats.KeyCount)
	}

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
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create Store and write two entries (isSystem=false)
	s1, err := NewStore(dir, logger, true, 0, false)
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

	// 2. Corrupt the WAL manually
	// Find the WAL file (stonedb stores them in 'wal' subdir)
	walDir := filepath.Join(dir, "wal")
	matches, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("No WAL files found in %s", walDir)
	}
	walPath := matches[len(matches)-1] // Use last file

	// Open file, flip the last byte.
	f, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	stat, _ := f.Stat()
	size := stat.Size()

	// Read last byte
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, size-1); err != nil {
		t.Fatal(err)
	}

	// Flip bit
	b[0] ^= 0xFF

	// Write back
	if _, err := f.WriteAt(b, size-1); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// FORCE FULL RECOVERY:
	// Wipe internal storage to force replay from WAL
	if err := os.RemoveAll(filepath.Join(dir, "vlog")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}

	// 3. Re-open Store (Should trigger truncate) (isSystem=false)
	s2, err := NewStore(dir, logger, true, 0, false)
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
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Create Store and write data (isSystem=false)
	s1, err := NewStore(dir, logger, true, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	s1.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("key1"), Value: []byte("val1")}})
	s1.Close()

	// 2. Append garbage (partial header)
	walDir := filepath.Join(dir, "wal")
	matches, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("No WAL files found")
	}
	walPath := matches[len(matches)-1]

	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Append 4 bytes (partial header)
	if _, err := f.Write(make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// 3. Re-open (isSystem=false)
	s2, err := NewStore(dir, logger, true, 0, false)
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

	// 1. Create Store with MinReplicas = 1 (isSystem=false)
	// This ensures that any write operation must wait for at least 1 replica to acknowledge.
	s, err := NewStore(dir, logger, true, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Perform Write (Should Block)
	// We run this in a goroutine because ApplyBatch is synchronous and will block until quorum is met.
	done := make(chan error)
	go func() {
		done <- s.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("k"), Value: []byte("v")}})
	}()

	// 3. Verify it's blocked
	select {
	case err := <-done:
		t.Fatalf("Write returned before quorum was met. Err: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected behavior: timeout because write is blocked
	}

	// 4. Register Replica and Ack
	// The write above generates a LogSeq.
	// Since we started fresh, nextLogSeq was 1. The write used LogSeq 1.
	// We acknowledge 1 to ensure the write is unblocked.
	s.RegisterReplica("replica-1", 0)
	s.UpdateReplicaLogSeq("replica-1", 1)

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

	// Replica store (MinReplicas=0) (isSystem=false)
	s, err := NewStore(dir, logger, true, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Simulate incoming batch from Primary
	entries := []protocol.LogEntry{
		{LogSeq: 100, OpCode: protocol.OpJournalSet, Key: []byte("k1"), Value: []byte("v1")},
		{LogSeq: 101, OpCode: protocol.OpJournalSet, Key: []byte("k2"), Value: []byte("v2")},
	}

	if err := s.ReplicateBatch(entries); err != nil {
		t.Fatalf("ReplicateBatch failed: %v", err)
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

func TestStore_ReadOnlySystemPartition(t *testing.T) {
	// Create a store named "0" (System Partition)
	dir := filepath.Join(t.TempDir(), "0")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// isSystem=true
	s, err := NewStore(dir, logger, true, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Attempt Write
	err = s.ApplyBatch([]protocol.LogEntry{{OpCode: protocol.OpJournalSet, Key: []byte("k"), Value: []byte("v")}})
	if err != protocol.ErrSystemPartitionReadOnly {
		t.Errorf("Expected system partition read-only error, got %v", err)
	}

	// Attempt Read (Also restricted by default in current store implementation)
	_, err = s.Get("any")
	if err != protocol.ErrSystemPartitionReadOnly {
		t.Errorf("Expected system partition read-only error on Get, got %v", err)
	}
}
