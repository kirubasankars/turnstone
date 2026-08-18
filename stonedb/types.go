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

	// ValueLog Entry Header: CRC(4) + KeyLen(4) + ValLen(4) + TxID(8) + OpID(8) + Type(1)
	ValueLogHeaderSize = 29

	// Meta Size: FileID(4) + Offset(8) + Len(4) + TxID(8) + OpID(8) + Type(1)
	// Updated to 33 bytes to support 64-bit offsets (was 29).
	MetaSize = 33

	// WAL Header: Length(4) + Checksum(4)
	WALHeaderSize = 8

	// WAL Record Header: Type(1) + XID(8) + OpID(8)
	WALRecordHeaderSize = 17

	// Default limit for open ValueLog files
	DefaultValueLogMaxOpenFiles = 500
)

// WALRecordType identifies the kind of a single WAL record. Every client
// operation (BEGIN/SET/DEL/COMMIT/ABORT) is logged as its own typed record,
// Postgres-style, rather than being buffered and flushed as one batch at commit.
type WALRecordType uint8

const (
	WALRecordBegin  WALRecordType = 1
	WALRecordSet    WALRecordType = 2
	WALRecordDelete WALRecordType = 3
	WALRecordCommit WALRecordType = 4
	WALRecordAbort  WALRecordType = 5
)

// WALRecord is a single decoded WAL entry.
type WALRecord struct {
	Type  WALRecordType
	XID   uint64 // Transaction ID (assigned at BEGIN)
	OpID  uint64 // Monotonic log sequence number, assigned per record
	Key   []byte
	Value []byte
}

// TxStatus is the commit-log (clog) state of a transaction ID.
type TxStatus uint8

const (
	TxInProgress TxStatus = 0
	TxCommitted  TxStatus = 1
	TxAborted    TxStatus = 2
)

// Snapshot captures the visibility boundary for a transaction, Postgres-style:
// a version with a given xmin is visible iff xmin < Xmax and xmin is not in Xip,
// and the transaction that produced it is marked committed in the clog.
type Snapshot struct {
	Xmax uint64          // Versions with xmin >= Xmax are never visible.
	Xip  map[uint64]bool // Transactions that were in-progress when the snapshot was taken.
}

func (s Snapshot) contains(xid uint64) bool {
	return s.Xip[xid]
}

var (
	// Crc32Table uses the Castagnoli polynomial which is often hardware-accelerated (CRC32C)
	// and provides better error detection properties than IEEE.
	Crc32Table = crc32.MakeTable(crc32.Castagnoli)

	sysStaleBytesPrefix = []byte("!sys!garbage!")
	sysTransactionIDKey = []byte("!sys!txseq!")
	sysOperationIDKey   = []byte("!sys!opseq!")
	sysKeyCountKey      = []byte("!sys!keycount!") // Persisted Key Count
	sysWALIndexPrefix   = []byte("!sys!wal!idx!")
	sysClogPrefix       = []byte("!sys!clog!")
)

var (
	ErrTxnFinished    = errors.New("transaction is already finished")
	ErrWriteConflict  = errors.New("write conflict detected")
	ErrTxAborted      = errors.New("current transaction is aborted, commands ignored until end of transaction block")
	ErrKeyNotFound    = errors.New("key not found")
	ErrChecksum       = errors.New("checksum mismatch")
	ErrCorruptData    = errors.New("data corruption detected")
	ErrTruncated      = errors.New("wal truncated due to corruption")
	ErrLogUnavailable = errors.New("wal log unavailable for requested operation id")
	ErrDiskFull       = errors.New("disk usage exceeds threshold")
	ErrDatabaseClosed = errors.New("database is closed")
)

// Options allows configuring the store behavior on Open
type Options struct {
	// If true, WAL corruption at the end of the file (partial writes)
	// will be truncated and the system will start.
	// If false, Open will return an error on corruption.
	TruncateCorruptWAL bool

	// MaxVLogSize is the threshold in bytes at which the ValueLog file is rotated.
	// If 0, defaults to 200MB.
	// Updated to int64 for 64-bit support.
	MaxVLogSize int64

	// CompactionMinGarbage is the minimum amount of stale data (in bytes)
	// required in a file before it becomes a candidate for compaction.
	// If 0, a default of 1MB is used.
	CompactionMinGarbage int64

	// CompactionInterval is the interval at which the background compaction task runs.
	// If 0, defaults to 2 minutes.
	CompactionInterval time.Duration

	// ChecksumInterval is the interval at which the background verifies ValueLog integrity.
	// If 0, background checksumming is disabled.
	ChecksumInterval time.Duration

	// AutoCheckpointInterval defines the frequency of the background checkpoint task.
	// If 0, defaults to 60 seconds.
	AutoCheckpointInterval time.Duration

	// MaxDiskUsagePercent (0-100). If disk usage exceeds this, write transactions fail.
	// If 0, disabled.
	MaxDiskUsagePercent int

	// BlockCacheSize is the capacity of the LevelDB block cache in bytes.
	// If 0, defaults to 64MB.
	BlockCacheSize int

	// Logger is the structured logger to use. If nil, logging is discarded.
	Logger *slog.Logger

	// TxTimeout is the maximum age a RW transaction may reach before the
	// background liveness reaper force-aborts it, independent of client
	// activity. If 0, defaults to protocol.MaxTxDuration.
	TxTimeout time.Duration

	// CommitDelay is a short bounded wait the group-commit worker takes
	// after the first pending commit arrives, giving other
	// concurrently-committing transactions a chance to join the same WAL
	// fsync before it fires (mirrors PostgreSQL's commit_delay). It is only
	// applied when CommitSiblings indicates real concurrency, so a single,
	// unbatched client never pays it as pure added latency.
	// If 0, defaults to 2ms. Set to a negative value to disable grouping.
	CommitDelay time.Duration

	// CommitSiblings is the minimum number of concurrently active
	// transactions (counting the one about to commit) required before
	// CommitDelay is applied (mirrors PostgreSQL's commit_siblings).
	// If 0, defaults to 2.
	CommitSiblings int

	// UnsafeDisableFsync skips the WAL fsync on COMMIT entirely (mirrors
	// PostgreSQL's fsync=off). A crash or power loss can then silently lose
	// or corrupt recently "committed" data that the OS never actually
	// flushed to disk. Benchmarking/debugging only -- never enable in
	// production.
	UnsafeDisableFsync bool
}

// WALLocation points to a specific batch in the WAL files
type WALLocation struct {
	FileStartOffset uint64 `json:"f"` // The virtual offset used for the filename (e.g. 00000.wal)
	RelativeOffset  uint32 `json:"o"` // The byte offset within that file
}

// pendingFile represents a file waiting to be deleted
type pendingFile struct {
	fileID     uint32
	obsoleteAt uint64 // The TxID when this file was removed from the index
}

// PendingOp represents a modification in memory (formerly pendingWrite)
type PendingOp struct {
	Value    []byte
	IsDelete bool
}

// ValueLogEntry represents an entry on disk (formerly VLogEntry)
type ValueLogEntry struct {
	Key           []byte
	Value         []byte
	TransactionID uint64 // The transaction this entry belongs to
	OperationID   uint64 // The specific operation ID
	IsDelete      bool
}

// EntryMeta is the pointer stored in LevelDB (formerly valueMeta)
type EntryMeta struct {
	FileID        uint32
	ValueOffset   int64 // Updated to int64 (8 bytes) to remove 4GB limit
	ValueLen      uint32
	TransactionID uint64
	OperationID   uint64
	IsTombstone   bool
}
