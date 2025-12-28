package main

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL manages a single append-only log file.
// It supports concurrent reads and synchronized writes.
type WAL struct {
	mu        sync.RWMutex
	path      string
	f         *os.File
	size      int64
	broadcast *sync.Cond // Used to notify replication watchers of new data.
}

// OpenWAL opens or creates the single wal.log file.
func OpenWAL(dir string) (*WAL, error) {
	path := filepath.Join(dir, "wal.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// Seek to end to be ready for appends
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return nil, err
	}

	w := &WAL{
		path: path,
		f:    f,
		size: info.Size(),
	}
	w.broadcast = sync.NewCond(&w.mu) // Use the same mutex for the condition variable

	return w, nil
}

// Write appends data to the WAL in a thread-safe manner.
func (w *WAL) Write(p []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset := w.size
	n, err := w.f.Write(p)
	if err != nil {
		return 0, err
	}

	w.size += int64(n)

	// Notify any watchers (Replication Streamers) that new data is available
	w.broadcast.Broadcast()

	return offset, nil
}

// ReadAt reads from the WAL at the given offset.
func (w *WAL) ReadAt(p []byte, off int64) (int, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if off >= w.size {
		return 0, io.EOF
	}

	return w.f.ReadAt(p, off)
}

// PunchHole deallocates the physical blocks for the given range, keeping the file size constant.
// This delegates to platform-specific implementations (sysPunchHole).
func (w *WAL) PunchHole(offset int64, size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure we flush any buffered writes before manipulating the filesystem
	if err := w.f.Sync(); err != nil {
		return err
	}

	return sysPunchHole(w.f, offset, size)
}

// Sync flushes the file content to disk (fsync).
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Close closes the file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// Size returns the current logical size of the WAL.
func (w *WAL) Size() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.size
}

// Wait blocks until the WAL size exceeds currentSize.
// This is used by the replication streamer to implement long-polling.
func (w *WAL) Wait(currentSize int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.size <= currentSize {
		w.broadcast.Wait()
	}
}

// Truncate resizes the file, usually to discard corrupted trailing bytes during recovery.
func (w *WAL) Truncate(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(size); err != nil {
		return err
	}
	if _, err := w.f.Seek(size, 0); err != nil {
		return err
	}
	w.size = size
	return nil
}
