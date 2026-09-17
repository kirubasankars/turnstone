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

// syncFile durability-flushes file data. fdatasync skips inode metadata that
// fsync would write — the same durable-write shortcut PostgreSQL uses on Unix.
func syncFile(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
