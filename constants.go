package main

import (
	"errors"
	"hash/crc32"
	"time"
)

const (
	// Network & Protocol defaults
	DefaultPort         = ":6379"              // server port
	DefaultReadTimeout  = 5 * time.Second      // network read timeout
	DefaultWriteTimeout = 5 * time.Second      // network write timeout
	IdleTimeout         = 3 * 60 * time.Second // idle timeout
	ShutdownTimeout     = 10 * time.Second     // graceful shutdown wait

	// Transaction Limits
	MaxTxDuration = 60 * time.Second // max transaction life time
	MaxTxOps      = 10000            // max number of operations in a transaction

	// Storage Paths & Names
	DefaultDataDir = "data"
	BoltBucketData = "index"
	BoltBucketMeta = "meta"
	BoltBucketPair = "pair" // New bucket: Offset -> Length (renamed from Record)
	KeyLastOffset  = "last_offset"

	// Limits & Safety
	MaxKeySize           = 1 * 1024
	MaxValueSize         = 4 * 1024
	MaxCommandSize       = 64 * 1024
	MaxPendingWriteBytes = 128 * 1024 * 1024
	MaxMemoryLimit       = 1024 * 1024 * 1024
	MaxSyncBytes         = 16 * 1024 * 1024 // 16MB limit for CDC batches
	MaxResponseSize      = 32 * 1024 * 1024 // 32MB Limit for responses

	// Storage Format (Journal)
	HeaderSize = 20 // Entry Header (KeyLen + ValLen + MinReadVersion + CRC)
	Tombstone  = ^uint32(0)

	// Internal Op Types (Journal)
	OpJournalSet    = 1
	OpJournalDelete = 2
	OpJournalGet    = 3

	// Tuning Parameters
	BatchDelay                 = 10 * time.Millisecond
	MaxBatchSize               = 2000
	MaxBatchBytes              = 64 * 1024 // Adaptive batching: flush if batch exceeds 64KB
	FlushInterval              = 1 * time.Second
	DefaultCompactionThreshold = 10 * 1024 * 1024
	CompactionGracePeriod      = 5 * time.Minute // Time to keep old files after compaction for CDC
	MaxCompactionIterations    = 10              // Max catch-up loops before forcing STW
	IndexFlushThreshold        = 100_000         // From compaction logic
)

// Standard Errors
var (
	ErrKeyNotFound          = errors.New("key does not exist")
	ErrCrcMismatch          = errors.New("crc checksum mismatch")
	ErrClosed               = errors.New("store closed")
	ErrCommandTooLarge      = errors.New("command line too large")
	ErrConflict             = errors.New("transaction conflict")
	ErrBusy                 = errors.New("server busy")
	ErrTransactionTimeout   = errors.New("transaction timeout")
	ErrCompactionInProgress = errors.New("compaction already in progress")
	ErrGenerationMismatch   = errors.New("generation mismatch")
)

// Request OpCodes
const (
	OpCodePing    = 0x01
	OpCodeGet     = 0x02
	OpCodeSet     = 0x03
	OpCodeDel     = 0x04
	OpCodeBegin   = 0x10
	OpCodeCommit  = 0x11
	OpCodeAbort   = 0x12
	OpCodeStat    = 0x20
	OpCodeCompact = 0x21
	OpCodeAuth    = 0x23
	OpCodeSync    = 0x30
	OpCodeQuit    = 0xFF
)

// Response Status Codes
const (
	ResStatusOK             = 0x00
	ResStatusErr            = 0x01
	ResStatusNotFound       = 0x02
	ResStatusTxRequired     = 0x03
	ResStatusTxTimeout      = 0x04
	ResStatusTxConflict     = 0x05
	ResStatusServerBusy     = 0x06
	ResStatusEntityTooLarge = 0x07
	ResStatusAuthRequired   = 0x08
	ResStatusGenMismatch    = 0x09
)

// Fixed Header Size: 1 byte OpCode + 4 bytes Length
const ProtoHeaderSize = 5

// Optimization: Use Castagnoli Table for hardware acceleration (SSE4.2)
var CrcTable = crc32.MakeTable(crc32.Castagnoli)
