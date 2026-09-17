// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	walSegFooterMagic   uint32 = 0x54534631 // "TSF1"
	walSegFooterSize           = 16
	walRecycleDirName          = "recycle"
	maxRecycledSegments        = 8
)

func (l *DataLog) usableSegmentSize() int64 {
	if l.segmentSize <= walSegFooterSize+64 {
		return l.segmentSize
	}
	return l.segmentSize - walSegFooterSize
}

func (l *DataLog) recycleDir() string {
	return filepath.Join(l.walDir, walRecycleDirName)
}

func (l *DataLog) segmentUsedBytes(seg *walSegment) int64 {
	if seg.endLSN > 0 {
		used := seg.endLSN - seg.baseLSN
		if used < 0 {
			return 0
		}
		return used
	}
	used := l.writeOffset - seg.baseLSN
	if used < 0 {
		return 0
	}
	return used
}

func writeSegmentFooter(f *os.File, segmentSize, used int64) error {
	if f == nil || segmentSize < walSegFooterSize {
		return nil
	}
	buf := make([]byte, walSegFooterSize)
	binary.BigEndian.PutUint32(buf[0:], walSegFooterMagic)
	binary.BigEndian.PutUint64(buf[4:], uint64(used))
	binary.BigEndian.PutUint32(buf[12:], crc32.Checksum(buf[:12], Crc32Table))
	_, err := f.WriteAt(buf, segmentSize-walSegFooterSize)
	return err
}

// writeSegmentFooterIfAllocated writes the TSF1 footer only when the file is
// already segment-sized. Extending a grow-as-you-write file to 64 MiB just to
// store the footer would hit ENOSPC on a tight tmpfs the same way fallocate did.
func writeSegmentFooterIfAllocated(f *os.File, segmentSize, used int64) error {
	if f == nil {
		return nil
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < segmentSize {
		return nil
	}
	return writeSegmentFooter(f, segmentSize, used)
}

func readSegmentFooter(f *os.File, segmentSize int64) (used int64, ok bool) {
	if f == nil || segmentSize < walSegFooterSize {
		return 0, false
	}
	buf := make([]byte, walSegFooterSize)
	if _, err := f.ReadAt(buf, segmentSize-walSegFooterSize); err != nil {
		return 0, false
	}
	if binary.BigEndian.Uint32(buf[0:]) != walSegFooterMagic {
		return 0, false
	}
	if crc32.Checksum(buf[:12], Crc32Table) != binary.BigEndian.Uint32(buf[12:]) {
		return 0, false
	}
	used = int64(binary.BigEndian.Uint64(buf[4:]))
	if used < 0 || used > segmentSize-walSegFooterSize {
		return 0, false
	}
	return used, true
}

func createAllocatedWALFile(path string, segmentSize int64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, err
	}
	if err := preallocateFile(f, segmentSize); err != nil {
		if isNoSpace(err) {
			// Grow as we write. File size is the logical head; no end-of-file footer.
			return f, nil
		}
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := writeSegmentFooter(f, segmentSize, 0); err != nil {
		if isNoSpace(err) {
			_ = f.Truncate(0)
			return f, nil
		}
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := syncFile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return f, nil
}

func resetAllocatedWALFile(path string, segmentSize int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, fileMode)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := preallocateFile(f, segmentSize); err != nil {
		if isNoSpace(err) {
			return f.Truncate(0)
		}
		return err
	}
	if err := writeSegmentFooter(f, segmentSize, 0); err != nil {
		if isNoSpace(err) {
			return f.Truncate(0)
		}
		return err
	}
	return syncFile(f)
}

func (l *DataLog) loadRecyclePool() error {
	dir := l.recycleDir()
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var maxSeq uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(dir, name)
		if info, err := e.Info(); err == nil && info.Size() < l.segmentSize {
			if err := resetAllocatedWALFile(path, l.segmentSize); err != nil {
				_ = os.Remove(path)
				continue
			}
		}
		l.recycle = append(l.recycle, path)
		if seq, ok := parseRecycleSeq(name); ok && seq > maxSeq {
			maxSeq = seq
		}
	}
	l.recycleSeq = maxSeq
	return nil
}

func parseRecycleSeq(name string) (uint32, bool) {
	if !strings.HasPrefix(name, "r-") {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(name, "r-"), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

func (l *DataLog) takeRecycledSegment(nextPath string) (*os.File, bool, error) {
	for len(l.recycle) > 0 {
		src := l.recycle[0]
		l.recycle = l.recycle[1:]
		if err := os.Rename(src, nextPath); err != nil {
			_ = os.Remove(src)
			continue
		}
		f, err := os.OpenFile(nextPath, os.O_RDWR, fileMode)
		if err != nil {
			return nil, false, err
		}
		return f, true, nil
	}
	return nil, false, nil
}

func (l *DataLog) recycleOrRemove(path string) (recycled bool, err error) {
	if len(l.recycle) >= maxRecycledSegments {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	}
	if err := os.MkdirAll(l.recycleDir(), dirMode); err != nil {
		return false, err
	}
	l.recycleSeq++
	dst := filepath.Join(l.recycleDir(), fmt.Sprintf("r-%06d", l.recycleSeq))
	if err := os.Rename(path, dst); err != nil {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	}
	if err := resetAllocatedWALFile(dst, l.segmentSize); err != nil {
		_ = os.Remove(dst)
		return false, nil
	}
	l.recycle = append(l.recycle, dst)
	return true, nil
}

func (l *DataLog) logicalUsedBytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var total int64
	for i := range l.segments {
		total += l.segmentUsedBytes(&l.segments[i])
	}
	return total
}

func (l *DataLog) RecycledSegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.recycle)
}

func (l *DataLog) rotateIfNeededLocked(need int64) error {
	if need <= 0 {
		return nil
	}
	usable := l.usableSegmentSize()
	local := l.writeOffset - l.segments[l.activeIndex].baseLSN
	if local+need <= usable {
		return nil
	}
	if local == 0 {
		return nil
	}
	return l.rotateSegmentLocked()
}
