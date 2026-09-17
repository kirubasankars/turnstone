// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package engine

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func isQuotaExceeded(err error) bool {
	return errors.Is(err, unix.EDQUOT)
}

// preallocateFileOS reserves size bytes so later WAL writes do not grow the
// inode. fallocate matches PostgreSQL's fully-allocated WAL segments.
func preallocateFileOS(f *os.File, size int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err == nil {
		return nil
	}
	if err == unix.EOPNOTSUPP || err == unix.ENOSYS {
		return f.Truncate(size)
	}
	return err
}
