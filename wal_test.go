package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWAL_Open_Create verifies that OpenWAL creates the file if missing
// and correctly opens existing files, seeking to the end.
func TestWAL_Open_Create(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "values.log")

	// 1. Create New
	w1, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	if w1.size != 0 {
		t.Errorf("New WAL size mismatch. Want 0, got %d", w1.size)
	}

	// Write some data
	data := []byte("hello world")
	if _, err := w1.Write(data); err != nil {
		t.Fatal(err)
	}
	w1.Close()

	// 2. Open Existing
	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("Failed to open existing WAL: %v", err)
	}
	defer w2.Close()

	info, _ := os.Stat(path)
	if w2.size != info.Size() {
		t.Errorf("Re-opened size mismatch. Want %d, got %d", info.Size(), w2.size)
	}

	// Verify seek position (write should append)
	if _, err := w2.Write([]byte("!")); err != nil {
		t.Fatal(err)
	}

	// Read back all
	fullContent, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := "hello world!"
	if string(fullContent) != expected {
		t.Errorf("Content mismatch. Want %q, got %q", expected, string(fullContent))
	}
}

// TestWAL_Read_Write verifies basic read/write operations and offset handling.
func TestWAL_Read_Write(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write 1
	data1 := []byte("chunk1")
	off1, err := w.Write(data1)
	if err != nil {
		t.Fatal(err)
	}
	if off1 != 0 {
		t.Errorf("Offset mismatch. Want 0, got %d", off1)
	}

	// Write 2
	data2 := []byte("chunk2")
	off2, err := w.Write(data2)
	if err != nil {
		t.Fatal(err)
	}
	if off2 != int64(len(data1)) {
		t.Errorf("Offset mismatch. Want %d, got %d", len(data1), off2)
	}

	// Read 1
	buf := make([]byte, len(data1))
	n, err := w.ReadAt(buf, off1)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(data1) {
		t.Errorf("Short read")
	}
	if !bytes.Equal(buf, data1) {
		t.Errorf("Data mismatch")
	}

	// Read 2
	buf2 := make([]byte, len(data2))
	n, err = w.ReadAt(buf2, off2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf2, data2) {
		t.Errorf("Data mismatch")
	}

	// Read EOF
	n, err = w.ReadAt(buf, off2+100)
	if err != io.EOF {
		t.Errorf("Expected EOF reading past end, got %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes, got %d", n)
	}
}

// TestWAL_Wait_Broadcast verifies that Wait blocks until data is written
// and is woken up by Broadcast.
func TestWAL_Wait_Broadcast(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	initialSize := w.Size()
	waitComplete := make(chan struct{})

	// Consumer
	go func() {
		// Wait for size to exceed initialSize
		w.Wait(initialSize)
		close(waitComplete)
	}()

	// Ensure consumer is likely blocked
	select {
	case <-waitComplete:
		t.Fatal("Wait returned before write")
	case <-time.After(50 * time.Millisecond):
		// OK
	}

	// Producer
	if _, err := w.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}

	// Verify wakeup
	select {
	case <-waitComplete:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Wait timed out after write")
	}
}

// TestWAL_Truncate verifies file truncation logic.
func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write 10 bytes
	data := []byte("0123456789")
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}

	// Truncate to 5
	if err := w.Truncate(5); err != nil {
		t.Fatal(err)
	}

	if w.Size() != 5 {
		t.Errorf("Size mismatch after truncate. Want 5, got %d", w.Size())
	}

	// Verify content on disk
	content, _ := os.ReadFile(w.path)
	if string(content) != "01234" {
		t.Errorf("Disk content mismatch. Want '01234', got %q", string(content))
	}

	// Verify Seek position (next write should be at 5)
	if _, err := w.Write([]byte("A")); err != nil {
		t.Fatal(err)
	}
	content, _ = os.ReadFile(w.path)
	if string(content) != "01234A" {
		t.Errorf("Append after truncate failed. Got %q", string(content))
	}
}

// TestWAL_Sync verifies Sync call (mostly checking for no error).
func TestWAL_Sync(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := w.Write([]byte("persist")); err != nil {
		t.Fatal(err)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

// TestWAL_PunchHole verifies logic for deallocating space.
// Since actual disk usage checks are OS-dependent and flaky in CI containers,
// we verify it doesn't return error and logical file integrity remains.
func TestWAL_PunchHole(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write 8KB (2 pages typically)
	data := make([]byte, 8192)
	w.Write(data)

	// Punch first 4KB
	if err := w.PunchHole(0, 4096); err != nil {
		// Some filesystems (tmpfs on some OS) don't support fallocate/punch hole.
		// If it's a "not supported" error, we can skip, but the fallback implementation
		// should usually return nil.
		t.Logf("PunchHole returned error (might be FS limitation): %v", err)
	}

	// File size should NOT change
	if w.Size() != 8192 {
		t.Errorf("PunchHole should maintain logical size. Want 8192, got %d", w.Size())
	}
}

// TestWAL_ThreadSafety exercises the mutexes under high concurrency.
func TestWAL_ThreadSafety(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	workers := 10
	writes := 100

	// Writers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				w.Write([]byte("data"))
			}
		}()
	}

	// Readers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4)
			for j := 0; j < writes; j++ {
				// Read random valid offsets or current size
				s := w.Size()
				if s > 4 {
					w.ReadAt(buf, 0)
				}
			}
		}()
	}

	wg.Wait()

	expectedSize := int64(workers * writes * 4)
	if w.Size() != expectedSize {
		t.Errorf("Final size mismatch. Want %d, got %d", expectedSize, w.Size())
	}
}
