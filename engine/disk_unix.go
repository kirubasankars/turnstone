//go:build !windows

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"syscall"
)

const (
	seekData = 3 // SEEK_DATA on Linux
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
