package main

import (
	"errors"
	"hash/crc32"
	"time"
)

// Network & Protocol Defaults
const (
	DefaultPort         = ":6379"
	DefaultReadTimeout  = 5 * time.Second
	DefaultWriteTimeout = 5 * time.Second
	IdleTimeout         = 3 * 60 * time.Second
	ShutdownTimeout     = 10 * time.Second
	MaxTxDuration       = 5 * time.Second
	MaxTxOps            = 10000
)

// Storage Limits
const (
	DefaultDataDir = "data"
	MaxKeySize     = 1 * 1024
	MaxValueSize   = 4 * 1024
	MaxCommandSize = 64 * 1024
	MaxSyncBytes   = 16 * 1024 * 1024
	MaxIndexBytes  = 512 * 1024 * 1024
)

// Storage Format & Journals
const (
	HeaderSize      = 24
	OpJournalSet    = 1
	OpJournalDelete = 2
	OpJournalGet    = 3
)

// Tuning
const (
	BatchDelay    = 10 * time.Millisecond
	MaxBatchSize  = 4000
	MaxBatchBytes = 64 * 1024
)

// Bit Packing Masks
const (
	BitMaskDeleted  = 0x80000000
	BitMaskKeyLen   = 0x7FF80000
	BitMaskValLen   = 0x0007FFFF
	BitShiftDeleted = 31
	BitShiftKeyLen  = 19
	BitShiftValLen  = 0
)

// Wire Protocol OpCodes
const (
	OpCodePing      = 0x01
	OpCodeGet       = 0x02
	OpCodeSet       = 0x03
	OpCodeDel       = 0x04
	OpCodeSelect    = 0x05
	OpCodeBegin     = 0x10
	OpCodeCommit    = 0x11
	OpCodeAbort     = 0x12
	OpCodeStat      = 0x20
	OpCodeCompact   = 0x31
	OpCodeReplicaOf = 0x32 // NEW: Set replication source
	OpCodeReplHello = 0x50
	OpCodeReplBatch = 0x51
	OpCodeReplAck   = 0x52
	OpCodeQuit      = 0xFF
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
	ResStatusMemoryLimit    = 0x08
)

const ProtoHeaderSize = 5

// Errors
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
	ErrDbNotFound           = errors.New("database not found")
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// PackMeta combines metadata (key length, value length, deleted flag) into a single uint32.
func PackMeta(keyLen, valLen uint32, deleted bool) uint32 {
	var packed uint32
	if deleted {
		packed |= (1 << BitShiftDeleted)
	}
	packed |= ((keyLen & 0xFFF) << BitShiftKeyLen)
	packed |= ((valLen & 0x7FFFF) << BitShiftValLen)
	return packed
}

// UnpackMeta extracts metadata from a packed uint32.
func UnpackMeta(packed uint32) (keyLen uint32, valLen uint32, deleted bool) {
	deleted = (packed & BitMaskDeleted) != 0
	keyLen = (packed & BitMaskKeyLen) >> BitShiftKeyLen
	valLen = (packed & BitMaskValLen) >> BitShiftValLen
	return
}
