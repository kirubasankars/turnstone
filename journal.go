package main

import (
	"os"
	"sync"
)

// JournalFile wraps os.File to provide thread-safe positional IO (pread/pwrite).
type JournalFile struct {
	path string
	f    *os.File
	mu   sync.RWMutex
}

func OpenJournal(path string) (*JournalFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &JournalFile{path: path, f: f}, nil
}

func (j *JournalFile) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}

// ReadAt wraps pread (safe for concurrent use)
func (j *JournalFile) ReadAt(p []byte, off int64) (int, error) {
	// Note: os.File.ReadAt is thread-safe on UNIX, but using RLock here
	// ensures no conflict with Truncate/Close operations.
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.f.ReadAt(p, off)
}

// WriteAt wraps pwrite (safe for concurrent use)
func (j *JournalFile) WriteAt(p []byte, off int64) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.WriteAt(p, off)
}

func (j *JournalFile) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Sync()
}

func (j *JournalFile) Truncate(size int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Truncate(size)
}

// Seek is only used during recovery
func (j *JournalFile) Seek(offset int64, whence int) (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Seek(offset, whence)
}
