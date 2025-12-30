//go:build !linux

package main

import "os"

// sysPunchHole is a no-op on non-Linux platforms.
func sysPunchHole(f *os.File, offset, size int64) error {
	return nil
}
