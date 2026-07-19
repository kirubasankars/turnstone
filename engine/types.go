// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"errors"
	"hash/crc32"
	"log/slog"
	"time"
)

const (
	dirMode  = 0o755
	fileMode = 0o644

	// LogRecordHeader: Type(1) + XID(8)
	LogRecordHeaderSize = 9

	// Frame header: Length(4) + Checksum(4)
	LogFrameHeaderSize = 8

	logFileName = "data.log"
)

// RecordType identifies the kind of a single log record.
type RecordType uint8

const (
	RecordBegin  RecordType = 1
	RecordSet    RecordType = 2
	RecordDelete RecordType = 3
	RecordCommit RecordType = 4
	RecordAbort  RecordType = 5
)

// Record is a single decoded log entry.
type Record struct {
	Type  RecordType
	XID   uint64
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
	ErrTruncated      = errors.New("log truncated due to corruption")
	ErrLogUnavailable = errors.New("log unavailable for requested byte offset")
	ErrDiskFull       = errors.New("disk usage exceeds threshold")
	ErrDatabaseClosed = errors.New("database is closed")
)

// Options configures the engine on Open.
type Options struct {
	TruncateCorruptTail bool

	ChecksumInterval    time.Duration
	RetentionInterval   time.Duration
	MaxDiskUsagePercent int
	Logger              *slog.Logger
	TxTimeout           time.Duration
	CommitDelay         time.Duration
	CommitSiblings      int
	UnsafeDisableFsync  bool

	// WalSegmentSize rotates the active WAL segment at this many bytes (0 = default 64MB).
	WalSegmentSize int64
	// IndexCompactFragmentation triggers arena rewrite when arenaUsed/liveBytes
	// exceeds this ratio during retention (0 = default 3.0).
	IndexCompactFragmentation float64
	// IndexCompactOnRetention runs MaybeCompactIndex on the retention ticker.
	// Nil omits the default (enabled on open). Set explicitly to false to disable.
	IndexCompactOnRetention *bool
	// WalCopyForwardFragmentation triggers copy-forward when allocated/live bytes
	// exceeds this ratio during retention (0 = default 3.0).
	WalCopyForwardFragmentation float64
	// WalCopyForwardOnRetention runs MaybeCopyForwardWal on the retention ticker.
	// Nil omits the default (enabled on open). Set explicitly to false to disable.
	WalCopyForwardOnRetention *bool
}

// indexVersion points at one MVCC version in the append-only log.
type indexVersion struct {
	offset    int64
	valueLen  uint32
	xmin      uint64
	tombstone bool
}

// recordSpan is the on-disk byte range of one log frame (header + payload).
type recordSpan struct {
	offset int64
	length int64 // total bytes including frame header
}
