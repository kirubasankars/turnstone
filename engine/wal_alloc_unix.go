// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package engine

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocateFile reserves size bytes so later WAL writes do not grow the
// inode. fallocate matches PostgreSQL's fully-allocated WAL segments.
func preallocateFile(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err == nil || err == unix.EOPNOTSUPP || err == unix.ENOSYS {
		if err != nil {
			return f.Truncate(size)
		}
		return nil
	}
	return err
}
