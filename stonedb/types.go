package stonedb

import (
	"errors"
	"hash/crc32"
	"time"
)

const (
	dirMode  = 0o755
	fileMode = 0o644

	// ValueLog Entry Header: CRC(4) + KeyLen(4) + ValLen(4) + TxID(8) + OpID(8) + Type(1)
	ValueLogHeaderSize = 29

	// Meta Size: FileID(4) + Offset(4) + Len(4) + TxID(8) + OpID(8) + Type(1)
	MetaSize = 29

	// WAL Header: Length(4) + Checksum(4)
	WALHeaderSize = 8

	// WAL Batch Header: TxID(8) + StartOpID(8) + Count(4)
	WALBatchHeaderSize = 20

	// Default limit for open ValueLog files
	DefaultValueLogMaxOpenFiles = 500
)

var (
	// Crc32Table uses the Castagnoli polynomial which is often hardware-accelerated (CRC32C)
	// and provides better error detection properties than IEEE.
	Crc32Table = crc32.MakeTable(crc32.Castagnoli)

	sysStaleBytesPrefix = []byte("!sys!garbage!")
	sysTransactionIDKey = []byte("!sys!txseq!")
	sysOperationIDKey   = []byte("!sys!opseq!")
	sysWALIndexPrefix   = []byte("!sys!wal!idx!") // Prefix for WAL Index in LevelDB
)

var (
	ErrTxnFinished    = errors.New("transaction is already finished")
	ErrWriteConflict  = errors.New("write conflict detected")
	ErrKeyNotFound    = errors.New("key not found")
	ErrChecksum       = errors.New("checksum mismatch")
	ErrCorruptData    = errors.New("data corruption detected")
	ErrTruncated      = errors.New("wal truncated due to corruption")
	ErrLogUnavailable = errors.New("wal log unavailable for requested operation id")
)

// Options allows configuring the store behavior on Open
type Options struct {
	// If true, WAL corruption at the end of the file (partial writes)
	// will be truncated and the system will start.
	// If false, Open will return an error on corruption.
	TruncateCorruptWAL bool

	// MaxWALSize is the threshold in bytes at which the WAL file is rotated.
	// If 0, a default of 10MB is used.
	MaxWALSize uint32

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

	// WALRetentionTime determines how long WAL files are kept.
	// Files older than this duration AND fully checkpointed will be purged.
	// If 0, defaults to 2 hours.
	WALRetentionTime time.Duration
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
	ValueOffset   uint32
	ValueLen      uint32
	TransactionID uint64
	OperationID   uint64
	IsTombstone   bool
}
