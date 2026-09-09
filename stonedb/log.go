package stonedb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// DataLog is a single append-only log file holding all records and values.
type DataLog struct {
	path        string
	file        *os.File
	writeOffset int64
	mu          sync.Mutex
	logger      *slog.Logger

	opOffsets map[uint64]int64 // opID -> frame start offset
}

func OpenDataLog(dir string, logger *slog.Logger) (*DataLog, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &DataLog{
		path:        path,
		file:        f,
		writeOffset: stat.Size(),
		logger:      logger,
		opOffsets:   make(map[uint64]int64),
	}, nil
}

func (l *DataLog) Path() string { return l.path }

func (l *DataLog) WriteOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writeOffset
}

func (l *DataLog) strictSync() {
	err := l.file.Sync()
	if err != nil {
		if errors.Is(err, syscall.EINTR) {
			l.strictSync()
			return
		}
		l.logger.Error("CRITICAL: fsync failed", "err", err)
		panic(fmt.Sprintf("CRITICAL STORAGE FAILURE: %v", err))
	}
}

// AppendRecordsWithOpIDs assigns opIDs and appends frames. Returns opIDs and
// the byte offset of each appended frame.
func (l *DataLog) AppendRecordsWithOpIDs(nextOpID func() uint64, builders []func(uint64) []byte, sync bool) ([]uint64, []int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(builders) == 0 {
		return nil, nil, nil
	}

	opIDs := make([]uint64, len(builders))
	offsets := make([]int64, len(builders))

	for i, build := range builders {
		opID := nextOpID()
		opIDs[i] = opID
		payload := build(opID)
		off := l.writeOffset
		if err := l.writeFrameLocked(payload); err != nil {
			return nil, nil, err
		}
		offsets[i] = off
		l.opOffsets[opID] = off
	}

	if sync {
		l.strictSync()
	}
	return opIDs, offsets, nil
}

func (l *DataLog) AppendReplicatedRecord(payload []byte, sync bool) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off := l.writeOffset
	if err := l.writeFrameLocked(payload); err != nil {
		return 0, err
	}
	if opID, ok := peekOpID(payload); ok {
		l.opOffsets[opID] = off
	}
	if sync {
		l.strictSync()
	}
	return off, nil
}

func (l *DataLog) writeFrameLocked(payload []byte) error {
	length := uint32(len(payload))
	checksum := crc32.Checksum(payload, Crc32Table)
	totalLen := LogFrameHeaderSize + int(length)

	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:], length)
	binary.BigEndian.PutUint32(buf[4:], checksum)
	copy(buf[8:], payload)

	n, err := l.file.WriteAt(buf, l.writeOffset)
	if err != nil {
		return err
	}
	l.writeOffset += int64(n)
	return nil
}

func (l *DataLog) ReadValueAt(offset int64, valLen uint32) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	header := make([]byte, LogFrameHeaderSize)
	if _, err := l.file.ReadAt(header, offset); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header[0:])
	payload := make([]byte, payloadLen)
	if _, err := l.file.ReadAt(payload, offset+LogFrameHeaderSize); err != nil {
		return nil, err
	}

	rec, err := decodeWALRecord(payload)
	if err != nil {
		return nil, err
	}
	if len(rec.Value) != int(valLen) {
		return nil, fmt.Errorf("value length mismatch at offset %d", offset)
	}
	return append([]byte(nil), rec.Value...), nil
}

func (l *DataLog) OpOffset(opID uint64) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, ok := l.opOffsets[opID]
	return off, ok
}

func (l *DataLog) SetOpOffset(opID uint64, offset int64) {
	l.mu.Lock()
	l.opOffsets[opID] = offset
	l.mu.Unlock()
}

// Replay scans the entire log, invoking onRecord for each valid frame.
// Holes are skipped via SEEK_DATA. Returns max tx/op IDs seen.
func (l *DataLog) Replay(truncateCorrupt bool, history []TimelineHistoryItem, onRecord func(rec WALRecord, span recordSpan)) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}

	cutoffOp := timelineCutoff(history)

	pos, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	for pos < fileSize {
		next, err := f.Seek(pos, seekData)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if next >= fileSize {
			break
		}
		pos = next

		validEnd, rec, span, rerr := l.readFrameAt(f, pos, fileSize)
		if rerr != nil {
			if (rerr == io.ErrUnexpectedEOF || rerr == ErrChecksum || rerr == ErrCorruptData) && truncateCorrupt {
				l.logger.Warn("Truncating corrupt log tail", "offset", pos, "err", rerr)
				if err := os.Truncate(l.path, pos); err != nil {
					return err
				}
				l.writeOffset = pos
				return ErrTruncated
			}
			if rerr == io.EOF {
				break
			}
			return fmt.Errorf("log corruption at offset %d: %w", pos, rerr)
		}

		if rec.OpID > cutoffOp {
			pos = validEnd
			continue
		}

		l.opOffsets[rec.OpID] = span.offset
		if onRecord != nil {
			onRecord(rec, span)
		}
		pos = validEnd
	}
	return nil
}

func timelineCutoff(history []TimelineHistoryItem) uint64 {
	if len(history) == 0 {
		return math.MaxUint64
	}
	return history[len(history)-1].EndOp
}

func (l *DataLog) readFrameAt(f *os.File, offset, fileSize int64) (int64, WALRecord, recordSpan, error) {
	if offset+LogFrameHeaderSize > fileSize {
		return offset, WALRecord{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	header := make([]byte, LogFrameHeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		return offset, WALRecord{}, recordSpan{}, err
	}
	length := binary.BigEndian.Uint32(header[0:])
	checksum := binary.BigEndian.Uint32(header[4:])
	if length > 1<<30 {
		return offset, WALRecord{}, recordSpan{}, ErrCorruptData
	}
	total := frameSize(int(length))
	if offset+total > fileSize {
		return offset, WALRecord{}, recordSpan{}, io.ErrUnexpectedEOF
	}
	payload := make([]byte, length)
	if _, err := f.ReadAt(payload, offset+LogFrameHeaderSize); err != nil {
		return offset, WALRecord{}, recordSpan{}, err
	}
	if crc32.Checksum(payload, Crc32Table) != checksum {
		return offset, WALRecord{}, recordSpan{}, ErrChecksum
	}
	rec, err := decodeWALRecord(payload)
	if err != nil {
		return offset, WALRecord{}, recordSpan{}, err
	}
	span := recordSpan{
		offset:  offset,
		length:  total,
		opID:    rec.OpID,
		xid:     rec.XID,
		recType: rec.Type,
	}
	return offset + total, rec, span, nil
}

// Scan streams records from startOpID onward.
func (l *DataLog) Scan(startOpID uint64, fn func([]WALRecord) error) error {
	l.mu.Lock()
	startOff, ok := l.findScanStartLocked(startOpID)
	l.mu.Unlock()
	if !ok {
		return ErrLogUnavailable
	}

	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	pos := startOff
	for pos < fileSize {
		next, err := f.Seek(pos, seekData)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if next >= fileSize {
			break
		}
		pos = next

		validEnd, rec, _, rerr := l.readFrameAt(f, pos, fileSize)
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return rerr
		}
		if rec.OpID >= startOpID {
			if err := fn([]WALRecord{rec}); err != nil {
				return err
			}
		}
		pos = validEnd
	}
	return nil
}

func (l *DataLog) findScanStartLocked(targetOpID uint64) (int64, bool) {
	if off, ok := l.opOffsets[targetOpID]; ok {
		return off, true
	}
	var bestOp uint64
	var bestOff int64
	found := false
	for op, off := range l.opOffsets {
		if op <= targetOpID {
			if !found || op > bestOp {
				bestOp = op
				bestOff = off
				found = true
			}
		}
	}
	return bestOff, found
}

func (l *DataLog) PunchHole(off, length int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return punchHole(int(l.file.Fd()), off, length)
}

func (l *DataLog) LogicalSize() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	stat, err := l.file.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
}

func (l *DataLog) AllocatedSize() int64 {
	var st syscall.Stat_t
	if err := syscall.Stat(l.path, &st); err != nil {
		return 0
	}
	return st.Blocks * 512
}

func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// streamFrames reads frames sequentially from r (used in tests).
func streamFrames(r io.Reader, onFrame func(offset int64, payload []byte) error) (int64, error) {
	validOffset := int64(0)
	reader := bufio.NewReader(r)
	for {
		header := make([]byte, LogFrameHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				return validOffset, io.EOF
			}
			return validOffset, io.ErrUnexpectedEOF
		}
		length := binary.BigEndian.Uint32(header[0:])
		checksum := binary.BigEndian.Uint32(header[4:])
		if length > 1<<30 {
			return validOffset, ErrCorruptData
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return validOffset, io.ErrUnexpectedEOF
		}
		if crc32.Checksum(payload, Crc32Table) != checksum {
			return validOffset, ErrChecksum
		}
		if err := onFrame(validOffset, payload); err != nil {
			return validOffset, err
		}
		validOffset += frameSize(int(length))
	}
}
