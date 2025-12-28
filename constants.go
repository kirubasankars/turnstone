package main

import (
	"errors"
	"hash/crc32"
	"time"
)

// Network & Protocol Defaults
const (
	DefaultPort         = ":6379"              // DefaultPort is the standard TCP port for the server.
	DefaultReadTimeout  = 5 * time.Second      // DefaultReadTimeout is the timeout for reading headers/payloads.
	DefaultWriteTimeout = 5 * time.Second      // DefaultWriteTimeout is the timeout for writing responses.
	IdleTimeout         = 3 * 60 * time.Second // IdleTimeout is the duration before an inactive connection is closed.
	ShutdownTimeout     = 10 * time.Second     // ShutdownTimeout is the max time to wait for connections to drain.
)

// Transaction Limits
const (
	MaxTxDuration = 5 * time.Second // MaxTxDuration is the maximum lifespan of a transaction before auto-abort.
	MaxTxOps      = 10000           // MaxTxOps is the maximum number of operations allowed in a single transaction.
)

// Storage Constants
const (
	DefaultDataDir = "data" // DefaultDataDir is the default relative path for storage.
)

// Limits & Safety
const (
	MaxKeySize     = 1 * 1024          // MaxKeySize is 1KB.
	MaxValueSize   = 4 * 1024          // MaxValueSize is 4KB (optimized for typical page sizes).
	MaxCommandSize = 64 * 1024         // MaxCommandSize limit for network payloads.
	MaxSyncBytes   = 16 * 1024 * 1024  // MaxSyncBytes (16MB) limit for CDC/Replication batches.
	MaxIndexBytes  = 512 * 1024 * 1024 // MaxIndexBytes (512MB) soft limit for in-memory index size.
)

// Storage Format
const (
	HeaderSize = 16 // HeaderSize is the fixed size of the entry header in the WAL (CRC, LSN, Meta).
)

// Internal Op Types (Journal)
const (
	OpJournalSet    = 1 // OpJournalSet represents a standard Set operation in the WAL.
	OpJournalDelete = 2 // OpJournalDelete represents a tombstone/delete in the WAL.
	OpJournalGet    = 3 // OpJournalGet is an internal marker for read operations (not written to WAL).
)

// Tuning Parameters
const (
	BatchDelay    = 10 * time.Millisecond // BatchDelay is the micro-batch window for the Group Commit.
	MaxBatchSize  = 4000                  // MaxBatchSize is the maximum count of requests in a Group Commit.
	MaxBatchBytes = 512 * 1024            // MaxBatchBytes (512KB) triggers an immediate flush.
)

// Bit Packing Constants
// The WAL header packs metadata into a single uint32:
// [ Deleted (1 bit) | KeyLength (12 bits) | ValueLength (19 bits) ]
const (
	BitMaskDeleted = 0x80000000 // Mask for the 31st bit (Delete flag).
	BitMaskKeyLen  = 0x7FF80000 // Mask for bits 19-30 (Key Length).
	BitMaskValLen  = 0x0007FFFF // Mask for bits 0-18 (Value Length).

	BitShiftDeleted = 31 // Shift for Delete flag.
	BitShiftKeyLen  = 19 // Shift for Key Length.
	BitShiftValLen  = 0  // Shift for Value Length.
)

// PackMeta combines the key length, value length, and deleted flag into a single uint32
// to save space in the WAL header.
func PackMeta(keyLen, valLen uint32, deleted bool) uint32 {
	var packed uint32
	if deleted {
		packed |= (1 << BitShiftDeleted)
	}
	packed |= ((keyLen & 0xFFF) << BitShiftKeyLen)   // 0xFFF = 12 bits mask
	packed |= ((valLen & 0x7FFFF) << BitShiftValLen) // 0x7FFFF = 19 bits mask
	return packed
}

// UnpackMeta extracts key length, value length, and deleted flag from the packed uint32.
func UnpackMeta(packed uint32) (keyLen uint32, valLen uint32, deleted bool) {
	deleted = (packed & BitMaskDeleted) != 0
	keyLen = (packed & BitMaskKeyLen) >> BitShiftKeyLen
	valLen = (packed & BitMaskValLen) >> BitShiftValLen
	return
}

// Standard Errors
var (
	ErrKeyNotFound          = errors.New("key does not exist")
	ErrCrcMismatch          = errors.New("crc checksum mismatch")
	ErrClosed               = errors.New("store closed")
	ErrCommandTooLarge      = errors.New("command line too large")
	ErrConflict             = errors.New("transaction conflict")
	ErrBusy                 = errors.New("server busy")
	ErrTransactionTimeout   = errors.New("transaction timeout")
	ErrReadOnly             = errors.New("server is read-only")
	ErrMemoryLimitExceeded  = errors.New("memory limit exceeded")
	ErrCompactionInProgress = errors.New("compaction already in progress")
)

// Request OpCodes (Wire Protocol)
const (
	OpCodePing      = 0x01 // Ping (Health check)
	OpCodeGet       = 0x02 // Get Key
	OpCodeSet       = 0x03 // Set Key Value
	OpCodeDel       = 0x04 // Delete Key
	OpCodeBegin     = 0x10 // Begin Transaction
	OpCodeCommit    = 0x11 // Commit Transaction
	OpCodeAbort     = 0x12 // Abort Transaction
	OpCodeStat      = 0x20 // Server Statistics
	OpCodeCompact   = 0x31 // Trigger Compaction
	OpCodeReplHello = 0x50 // Replication Handshake
	OpCodeReplBatch = 0x51 // Replication Batch Data
	OpCodeReplAck   = 0x52 // Replication ACK
	OpCodeQuit      = 0xFF // Close Connection
)

// Response Status Codes (Wire Protocol)
const (
	ResStatusOK             = 0x00 // Success
	ResStatusErr            = 0x01 // Generic Error
	ResStatusNotFound       = 0x02 // Key Not Found
	ResStatusTxRequired     = 0x03 // Operation requires an active transaction
	ResStatusTxTimeout      = 0x04 // Transaction exceeded MaxTxDuration
	ResStatusTxConflict     = 0x05 // Transaction collision detected
	ResStatusServerBusy     = 0x06 // Server overloaded
	ResStatusEntityTooLarge = 0x07 // Payload exceeds limits
	ResStatusMemoryLimit    = 0x08 // Memory limit reached
)

// ProtoHeaderSize is the size of the request header (1 byte OpCode + 4 bytes Length).
const ProtoHeaderSize = 5

// --- CRC32 Optimization ---
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// CalculateCRC32 computes the CRC32 checksum using the Castagnoli polynomial.
func CalculateCRC32(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}
