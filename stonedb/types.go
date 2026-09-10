// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"errors"
	"hash/crc32"
	"log/slog"
	"time"
)

const (
	dirMode  = 0o755
	fileMode = 0o644

	// LogRecordHeader: Type(1) + XID(8) + OpID(8)
	LogRecordHeaderSize = 17

	// Frame header: Length(4) + Checksum(4)
	LogFrameHeaderSize = 8

	logFileName = "data.log"
)

// WALRecordType identifies the kind of a single log record.
type WALRecordType uint8

const (
	WALRecordBegin  WALRecordType = 1
	WALRecordSet    WALRecordType = 2
	WALRecordDelete WALRecordType = 3
	WALRecordCommit WALRecordType = 4
	WALRecordAbort  WALRecordType = 5
)

// WALRecord is a single decoded log entry.
type WALRecord struct {
	Type  WALRecordType
	XID   uint64
	OpID  uint64
	Key   []byte
	Value []byte
}

// TxStatus is the commit-log state of a transaction ID.
type TxStatus uint8

const (
	TxInProgress TxStatus = 0
	TxCommitted  TxStatus = 1
	TxAborted    TxStatus = 2
)

// Snapshot captures the visibility boundary for a transaction.
type Snapshot struct {
	Xmax uint64
	Xip  map[uint64]bool
}

func (s Snapshot) contains(xid uint64) bool {
	return s.Xip[xid]
}

var Crc32Table = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrTxnFinished    = errors.New("transaction is already finished")
	ErrWriteConflict  = errors.New("write conflict detected")
	ErrKeyNotFound    = errors.New("key not found")
	ErrChecksum       = errors.New("checksum mismatch")
	ErrCorruptData    = errors.New("data corruption detected")
	ErrTruncated      = errors.New("wal truncated due to corruption")
	ErrLogUnavailable = errors.New("wal log unavailable for requested operation id")
	ErrDiskFull       = errors.New("disk usage exceeds threshold")
	ErrDatabaseClosed = errors.New("database is closed")
)

// Options configures the store on Open.
type Options struct {
	TruncateCorruptWAL bool

	CompactionMinGarbage   int64
	SegmentTargetSize      int64 // logical data.log segment size; sealed on commit boundary
	CompactionInterval     time.Duration
	ChecksumInterval       time.Duration
	AutoCheckpointInterval time.Duration
	MaxDiskUsagePercent    int
	Logger                 *slog.Logger
	TxTimeout              time.Duration
	CommitDelay            time.Duration
	CommitSiblings         int
	UnsafeDisableFsync     bool
}

// indexVersion points at one MVCC version in the append-only log.
type indexVersion struct {
	offset    int64
	valueLen  uint32
	xmin      uint64
	opID      uint64
	tombstone bool
}

// recordSpan is the on-disk byte range of one log frame (header + payload).
type recordSpan struct {
	offset  int64
	length  int64 // total bytes including frame header
	opID    uint64
	xid     uint64
	recType WALRecordType
}
