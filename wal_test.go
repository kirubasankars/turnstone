package main

import (
	"bytes"
	"testing"
	"time"
)

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
	if n != len(data1) || !bytes.Equal(buf, data1) {
		t.Errorf("Data mismatch")
	}
}

// TestWAL_Wait_Broadcast verifies that Wait blocks until data is written.
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
		w.Wait(initialSize)
		close(waitComplete)
	}()

	// Ensure consumer blocks
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
