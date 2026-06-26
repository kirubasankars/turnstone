// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree

import (
	"os"
	"syscall"
)

const (
	pageSize   = 4096
	metaPageID = 0
	magic      = 0x54534254524545 // "TSBTREE"
	version    = 1
)

// meta offsets within page 0
const (
	metaMagicOff     = 0
	metaVersionOff   = 8
	metaRootOff      = 12
	metaNumPagesOff  = 20
	metaFreeHeadOff  = 28
	metaLeftLeafOff  = 36
	leafHeaderSize   = 19
	internalHdrSize  = 19
	pageTypeLeaf     = 1
	pageTypeInternal = 0
)

type mmapFile struct {
	path string
	data []byte
}

func openMmap(path string, minPages uint64) (*mmapFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	size := info.Size()
	minSize := int64(pageSize) * int64(minPages)
	if minSize < int64(pageSize)*4 {
		minSize = int64(pageSize) * 4
	}
	if size < minSize {
		size = minSize
		if err := f.Truncate(size); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	_ = f.Close()
	if err != nil {
		return nil, err
	}

	return &mmapFile{path: path, data: data}, nil
}

func (mf *mmapFile) grow(minPages uint64) error {
	newSize := uint64(len(mf.data))
	needed := minPages * pageSize
	if needed <= newSize {
		return nil
	}
	for newSize < needed {
		newSize *= 2
	}
	if err := syscall.Munmap(mf.data); err != nil {
		return err
	}
	f, err := os.OpenFile(mf.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := f.Truncate(int64(newSize)); err != nil {
		_ = f.Close()
		return err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	_ = f.Close()
	if err != nil {
		return err
	}
	mf.data = data
	return nil
}

func (mf *mmapFile) sync() error {
	f, err := os.OpenFile(mf.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (mf *mmapFile) close() error {
	if mf.data != nil {
		_ = mf.sync()
		_ = syscall.Munmap(mf.data)
		mf.data = nil
	}
	return nil
}

func (mf *mmapFile) page(id uint64) []byte {
	off := int(id) * pageSize
	if off+pageSize > len(mf.data) {
		return nil
	}
	return mf.data[off : off+pageSize]
}

func readU64(buf []byte, off int) uint64 {
	if off+8 > len(buf) {
		return 0
	}
	return uint64(buf[off])<<56 | uint64(buf[off+1])<<48 | uint64(buf[off+2])<<40 | uint64(buf[off+3])<<32 |
		uint64(buf[off+4])<<24 | uint64(buf[off+5])<<16 | uint64(buf[off+6])<<8 | uint64(buf[off+7])
}

func writeU64(buf []byte, off int, v uint64) {
	buf[off] = byte(v >> 56)
	buf[off+1] = byte(v >> 48)
	buf[off+2] = byte(v >> 40)
	buf[off+3] = byte(v >> 32)
	buf[off+4] = byte(v >> 24)
	buf[off+5] = byte(v >> 16)
	buf[off+6] = byte(v >> 8)
	buf[off+7] = byte(v)
}

func readU32(buf []byte, off int) uint32 {
	return uint32(buf[off])<<24 | uint32(buf[off+1])<<16 | uint32(buf[off+2])<<8 | uint32(buf[off+3])
}

func writeU32(buf []byte, off int, v uint32) {
	buf[off] = byte(v >> 24)
	buf[off+1] = byte(v >> 16)
	buf[off+2] = byte(v >> 8)
	buf[off+3] = byte(v)
}

func readU16(buf []byte, off int) uint16 {
	return uint16(buf[off])<<8 | uint16(buf[off+1])
}

func writeU16(buf []byte, off int, v uint16) {
	buf[off] = byte(v >> 8)
	buf[off+1] = byte(v)
}

func compareKeys(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return int(a[i]) - int(b[i])
		}
	}
	return la - lb
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
