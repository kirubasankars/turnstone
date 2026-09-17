// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build linux

package engine

import "golang.org/x/sys/unix"

func adviseWALExtra(mapping []byte) {
	if len(mapping) == 0 {
		return
	}
	_ = unix.Madvise(mapping, unix.MADV_DONTFORK)
	_ = unix.Madvise(mapping, unix.MADV_DONTDUMP)
}
