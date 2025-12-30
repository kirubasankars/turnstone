package main

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL manages a single append-only log file.
type WAL struct {
	mu        sync.RWMutex
	path      string
	f         *os.File
	size      int64
	broadcast *sync.Cond
}

// OpenWAL initializes the write-ahead log.
func OpenWAL(dir string) (*WAL, error) {
	path := filepath.Join(dir, "values.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	// Seek to end to ensure appending happens correctly on restart
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	w := &WAL{path: path, f: f, size: info.Size()}
	w.broadcast = sync.NewCond(&w.mu)
	return w, nil
}

// Write appends data to the log and notifies waiters.
func (w *WAL) Write(p []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	offset := w.size
	n, err := w.f.Write(p)
	if err != nil {
		return 0, err
	}
	w.size += int64(n)
	w.broadcast.Broadcast()
	return offset, nil
}

// ReadAt reads data from a specific offset.
func (w *WAL) ReadAt(p []byte, off int64) (int, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if off >= w.size {
		return 0, io.EOF
	}
	return w.f.ReadAt(p, off)
}

// Sync flushes filesystem buffers.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Close flushes and closes the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.f.Sync()
	return w.f.Close()
}

func (w *WAL) Size() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.size
}

// Wait blocks until the WAL size reaches at least currentSize.
func (w *WAL) Wait(currentSize int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.size <= currentSize {
		w.broadcast.Wait()
	}
}

// Truncate resizes the file, usually for recovery or error handling.
func (w *WAL) Truncate(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(size); err != nil {
		return err
	}
	if _, err := w.f.Seek(size, io.SeekStart); err != nil {
		return err
	}
	w.size = size
	return nil
}

// PunchHole delegates to platform-specific implementations to reclaim disk space.
func (w *WAL) PunchHole(offset int64, size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.f.Sync()
	return sysPunchHole(w.f, offset, size)
}
