//go:build !windows

package stonedb

import (
	"syscall"
)

// getDiskUsage returns the usage percentage (0-100) of the partition containing path.
func getDiskUsage(path string) (float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}

	// Available blocks (for unprivileged users) vs Total blocks
	// Different OSs use slightly different types for Blocks (uint64 vs int64), cast to uint64 to be safe.
	total := uint64(stat.Blocks) * uint64(stat.Bsize)
	free := uint64(stat.Bavail) * uint64(stat.Bsize)

	if total == 0 {
		return 0, nil
	}

	used := total - free
	return (float64(used) / float64(total)) * 100.0, nil
}
