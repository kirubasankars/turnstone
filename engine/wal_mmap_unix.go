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

func mmapWALFile(f *os.File, size int64) ([]byte, error) {
	if f == nil || size <= 0 {
		return nil, nil
	}
	mapped, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	adviseWALMapping(mapped, unix.MADV_RANDOM)
	adviseWALExtra(mapped)
	return mapped, nil
}

func unmapWAL(mapping []byte) {
	if len(mapping) == 0 {
		return
	}
	adviseWALMapping(mapping, unix.MADV_DONTNEED)
	_ = unix.Munmap(mapping)
}

func adviseWALMapping(mapping []byte, advice int) {
	if len(mapping) == 0 {
		return
	}
	_ = unix.Madvise(mapping, advice)
}

func adviseWALRange(mapping []byte, off, n int64, advice int) {
	if len(mapping) == 0 || n <= 0 {
		return
	}
	page := int64(unix.Getpagesize())
	if page <= 0 {
		page = 4096
	}
	start := off - off%page
	if start < 0 {
		start = 0
	}
	end := off + n
	end = (end + page - 1) &^ (page - 1)
	if end > int64(len(mapping)) {
		end = int64(len(mapping))
	}
	if start >= end {
		return
	}
	_ = unix.Madvise(mapping[start:end], advice)
}

func walAdviseSequential() int { return unix.MADV_SEQUENTIAL }
func walAdviseRandom() int     { return unix.MADV_RANDOM }
