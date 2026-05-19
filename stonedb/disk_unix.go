//go:build !windows

package stonedb

import (
	"syscall"
)

const (
	fallocFlKeepSize  = 0x01
	fallocFlPunchHole = 0x02
	seekData          = 3 // SEEK_DATA on Linux
)

// getDiskUsage returns the usage percentage (0-100) of the partition containing path.
func getDiskUsage(path string) (float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	total := uint64(stat.Blocks) * uint64(stat.Bsize)
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	if total == 0 {
		return 0, nil
	}
	used := total - free
	return (float64(used) / float64(total)) * 100.0, nil
}

// punchHole reclaims disk space for the byte range [off, off+len) without
// changing the file's logical size.
func punchHole(fd int, off, length int64) error {
	return syscall.Fallocate(fd, fallocFlPunchHole|fallocFlKeepSize, off, length)
}

// fileBlockSize returns the filesystem block size for path.
func fileBlockSize(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 4096, err
	}
	if stat.Bsize <= 0 {
		return 4096, nil
	}
	return int64(stat.Bsize), nil
}
