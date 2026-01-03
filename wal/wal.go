package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"turnstone/protocol"
)

// segmentInfo tracks archived logs to map virtual offsets to physical files
type segmentInfo struct {
	path        string
	startOffset int64
	size        int64
}

// Global buffer pool to reduce GC pressure on heavy write loads.
var walBufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 64*1024))
	},
}

// WAL manages a series of append-only log files presented as a single stream.
type WAL struct {
	mu   sync.RWMutex
	cond *sync.Cond
	dir  string
	path string
	f    *os.File
	size int64

	// Virtual Addressing
	segments    []segmentInfo
	activeStart int64 // The virtual offset where the current active file begins

	fsyncEnabled bool
}

func OpenWAL(dir string, fsync bool) (*WAL, error) {
	path := filepath.Join(dir, "values.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}

	w := &WAL{
		dir:          dir,
		path:         path,
		f:            f,
		size:         info.Size(),
		fsyncEnabled: fsync,
		segments:     make([]segmentInfo, 0),
	}
	w.cond = sync.NewCond(&w.mu)
	return w, nil
}

// Rotate archives the current log file and opens a fresh one.
func (w *WAL) Rotate() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	currentSize := w.size
	if err := w.f.Close(); err != nil {
		return err
	}

	timestamp := time.Now().Format("20060102150405")
	archiveName := fmt.Sprintf("values-%s.log", timestamp)
	archivePath := filepath.Join(w.dir, archiveName)

	if err := os.Rename(w.path, archivePath); err != nil {
		return err
	}

	w.segments = append(w.segments, segmentInfo{
		path:        archivePath,
		startOffset: w.activeStart,
		size:        currentSize,
	})
	w.activeStart += currentSize

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}

	w.f = f
	w.size = 0
	return nil
}

func (w *WAL) WriteBatch(entries []protocol.LogEntry) (int64, uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size >= protocol.MaxWALSize {
		if err := w.Rotate(); err != nil {
			return w.activeStart + w.size, 0, fmt.Errorf("wal rotation failed: %w", err)
		}
	}

	buf := walBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer walBufPool.Put(buf)

	var lastLogSeq uint64
	var scratch [8]byte

	for _, entry := range entries {
		kLen := len(entry.Key)
		vLen := len(entry.Value)
		if entry.OpCode == protocol.OpJournalDelete {
			vLen = 0
		}

		meta := packMeta(uint32(kLen), uint32(vLen), entry.OpCode == protocol.OpJournalDelete)
		binary.BigEndian.PutUint32(scratch[:4], meta)
		buf.Write(scratch[:4])

		binary.BigEndian.PutUint64(scratch[:8], entry.LogSeq)
		buf.Write(scratch[:8])
		lastLogSeq = entry.LogSeq

		buf.Write([]byte{0, 0, 0, 0}) // CRC Placeholder

		buf.Write(entry.Key)
		if entry.OpCode != protocol.OpJournalDelete {
			buf.Write(entry.Value)
		}

		totalEntryLen := 16 + kLen + vLen
		data := buf.Bytes()
		start := len(data) - totalEntryLen

		crc := crc32.Checksum(data[start:start+12], protocol.Crc32Table)
		crc = crc32.Update(crc, protocol.Crc32Table, data[start+16:])
		binary.BigEndian.PutUint32(data[start+12:start+16], crc)
	}

	n, err := w.f.Write(buf.Bytes())
	if err != nil {
		return w.activeStart + w.size, 0, err
	}

	if w.fsyncEnabled {
		if err := w.f.Sync(); err != nil {
			return w.activeStart + w.size, 0, err
		}
	}

	w.size += int64(n)
	w.cond.Broadcast()

	return w.activeStart + w.size, lastLogSeq, nil
}

func (w *WAL) Wait(offset int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for (w.activeStart + w.size) <= offset {
		w.cond.Wait()
	}
}

func (w *WAL) ReadAt(p []byte, virtualOff int64) (int, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if virtualOff >= w.activeStart {
		return w.f.ReadAt(p, virtualOff-w.activeStart)
	}

	for _, seg := range w.segments {
		if virtualOff >= seg.startOffset && virtualOff < (seg.startOffset+seg.size) {
			f, err := os.Open(seg.path)
			if err != nil {
				return 0, err
			}
			defer f.Close()
			return f.ReadAt(p, virtualOff-seg.startOffset)
		}
	}
	return 0, fmt.Errorf("offset %d not found in any segment", virtualOff)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.f.Sync()
	return w.f.Close()
}

func (w *WAL) Size() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeStart + w.size
}

func (w *WAL) Recover() ([]protocol.LogEntry, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []protocol.LogEntry
	w.activeStart = 0
	w.segments = make([]segmentInfo, 0)

	files, err := filepath.Glob(filepath.Join(w.dir, "values-*.log"))
	if err != nil {
		return nil, 0, err
	}
	sort.Strings(files)

	for _, fPath := range files {
		f, err := os.Open(fPath)
		if err != nil {
			return nil, 0, err
		}
		fi, _ := f.Stat()
		segSize := fi.Size()

		segmentEntries, _, err := w.recoverFile(f, w.activeStart)
		f.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("error recovering %s: %w", fPath, err)
		}
		entries = append(entries, segmentEntries...)
		w.segments = append(w.segments, segmentInfo{
			path:        fPath,
			startOffset: w.activeStart,
			size:        segSize,
		})
		w.activeStart += segSize
	}

	activeEntries, offset, err := w.recoverFile(w.f, w.activeStart)
	if err != nil {
		return entries, w.activeStart + offset, err
	}
	entries = append(entries, activeEntries...)
	w.size = offset

	return entries, w.activeStart + w.size, nil
}

func (w *WAL) recoverFile(f *os.File, virtualStart int64) ([]protocol.LogEntry, int64, error) {
	var entries []protocol.LogEntry
	var offset int64
	header := make([]byte, protocol.HeaderSize)

	stat, _ := f.Stat()
	size := stat.Size()

	for offset < size {
		if _, err := f.ReadAt(header, offset); err != nil {
			if err == io.EOF {
				break
			}
			return entries, offset, err
		}

		if isZero(header) {
			offset = (offset/4096 + 1) * 4096
			continue
		}

		packed := binary.BigEndian.Uint32(header[0:4])
		kLen, vLen, isDel := unpackMeta(packed)
		logSeq := binary.BigEndian.Uint64(header[4:12])
		storedCrc := binary.BigEndian.Uint32(header[12:16])

		payloadLen := int(kLen)
		if !isDel {
			payloadLen += int(vLen)
		}

		if payloadLen < 0 || offset+int64(protocol.HeaderSize)+int64(payloadLen) > size {
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := f.ReadAt(payload, offset+int64(protocol.HeaderSize)); err != nil {
			break
		}

		crc := crc32.Checksum(header[:12], protocol.Crc32Table)
		crc = crc32.Update(crc, protocol.Crc32Table, payload)
		if crc != storedCrc {
			break
		}

		op := protocol.OpJournalSet
		if isDel {
			op = protocol.OpJournalDelete
		}

		key := make([]byte, kLen)
		copy(key, payload[:kLen])
		var val []byte
		if !isDel {
			val = make([]byte, vLen)
			copy(val, payload[kLen:])
		}

		virtualOffset := virtualStart + offset + int64(protocol.HeaderSize) + int64(payloadLen)
		entries = append(entries, protocol.LogEntry{
			LogSeq: logSeq,
			OpCode: op,
			Key:    key,
			Value:  val,
			Offset: virtualOffset,
		})

		offset += int64(protocol.HeaderSize) + int64(payloadLen)
	}
	return entries, offset, nil
}

const (
	bitMaskDeleted  = 0x80000000
	bitMaskKeyLen   = 0x7FF80000
	bitMaskValLen   = 0x0007FFFF
	bitShiftDeleted = 31
	bitShiftKeyLen  = 19
	bitShiftValLen  = 0
)

func packMeta(keyLen, valLen uint32, deleted bool) uint32 {
	var packed uint32
	if deleted {
		packed |= (1 << bitShiftDeleted)
	}
	packed |= ((keyLen & 0xFFF) << bitShiftKeyLen)
	packed |= ((valLen & 0x7FFFF) << bitShiftValLen)
	return packed
}

func unpackMeta(packed uint32) (keyLen uint32, valLen uint32, deleted bool) {
	deleted = (packed & bitMaskDeleted) != 0
	keyLen = (packed & bitMaskKeyLen) >> bitShiftKeyLen
	valLen = (packed & bitMaskValLen) >> bitShiftValLen
	return
}

func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
