//go:build linux

package main

import (
	"os"
	"syscall"
)

// sysPunchHole uses Linux fallocate to deallocate disk space.
func sysPunchHole(f *os.File, offset, size int64) error {
	// Constants for Linux fallocate
	const (
		FALLOC_FL_KEEP_SIZE  = 0x01
		FALLOC_FL_PUNCH_HOLE = 0x02
	)

	return syscall.Fallocate(int(f.Fd()), FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE, offset, size)
}
