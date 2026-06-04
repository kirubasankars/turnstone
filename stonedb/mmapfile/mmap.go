// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package mmapfile

import (
	"os"
	"syscall"
)

const PageSize = 4096

// File is a growable memory-mapped file.
type File struct {
	path string
	data []byte
}

func Open(path string, minSize int64) (*File, error) {
	if minSize < PageSize*4 {
		minSize = PageSize * 4
	}
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
	return &File{path: path, data: data}, nil
}

func (f *File) Data() []byte { return f.data }

func (f *File) Len() int { return len(f.data) }

func (f *File) Grow(minSize int64) error {
	cur := int64(len(f.data))
	if minSize <= cur {
		return nil
	}
	newSize := cur
	for newSize < minSize {
		newSize *= 2
	}
	if err := syscall.Munmap(f.data); err != nil {
		return err
	}
	file, err := os.OpenFile(f.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := file.Truncate(newSize); err != nil {
		_ = file.Close()
		return err
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	_ = file.Close()
	if err != nil {
		return err
	}
	f.data = data
	return nil
}

func (f *File) Sync() error {
	file, err := os.OpenFile(f.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func (f *File) Close() error {
	if f.data != nil {
		_ = f.Sync()
		_ = syscall.Munmap(f.data)
		f.data = nil
	}
	return nil
}
