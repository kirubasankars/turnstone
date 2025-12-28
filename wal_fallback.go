//go:build !linux

package main

import "os"

// sysPunchHole is a no-op on non-Linux platforms.
// Compaction will run logically (indexes update), but disk space
// might not be physically reclaimed by the OS immediately.
func sysPunchHole(f *os.File, offset, size int64) error {
	return nil
}
