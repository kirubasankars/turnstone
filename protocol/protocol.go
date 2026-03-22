package protocol

import (
	"errors"
	"hash/crc32"
	"time"
)

// --- Constants ---

const (
	DefaultPort         = ":6379"
	DefaultReadTimeout  = 5 * time.Second
	DefaultWriteTimeout = 5 * time.Second
	IdleTimeout         = 3 * 60 * time.Second
	ShutdownTimeout     = 10 * time.Second
	MaxTxDuration       = 5 * time.Second
	MaxTxOps            = 10000
	DefaultDataDir      = "data"
	MaxKeySize          = 1 * 1024
	MaxValueSize        = 4 * 1024
	MaxCommandSize      = 64 * 1024
	MaxSyncBytes        = 16 * 1024 * 1024
	MaxIndexBytes       = 512 * 1024 * 1024
	HeaderSize          = 16 // Meta(4) + LogSeq(8) + CRC(4)

	BatchDelay = 500 * time.Microsecond

	MaxBatchSize         = 4000
	MaxBatchBytes        = 64 * 1024
	ProtoHeaderSize      = 5
	CheckpointInterval   = 512 * 1024 * 1024
	SlowOpThreshold      = 500 * time.Millisecond
	MaxWALSize           = 200 * 1024 * 1024 // 200MB Limit
)

// Variables (Mutable for testing)
var (
	ReplicationTimeout = 30 * time.Second
)

// OpCodes define the available commands in the TurnstoneDB wire protocol.
const (
	OpCodePing      uint8 = 0x01
	OpCodeGet       uint8 = 0x02
	OpCodeSet       uint8 = 0x03
	OpCodeDel       uint8 = 0x04
	OpCodeSelect    uint8 = 0x05
	OpCodeMGet      uint8 = 0x06
	OpCodeMSet      uint8 = 0x07
	OpCodeMDel      uint8 = 0x08
	OpCodeBegin     uint8 = 0x10
	OpCodeCommit    uint8 = 0x11
	OpCodeAbort     uint8 = 0x12
	OpCodeStat      uint8 = 0x20
	OpCodeReplicaOf uint8 = 0x32
	OpCodeSlotDel   uint8 = 0x33
	OpCodeReplHello uint8 = 0x50
	OpCodeReplBatch uint8 = 0x51
	OpCodeReplAck   uint8 = 0x52
	
	// --- SNAPSHOT OPCODES ---
	OpCodeReplSnapshot     uint8 = 0x53 // Bulk data payload (Full Sync)
	OpCodeReplSnapshotDone uint8 = 0x54 // Transition signal to WAL streaming
	
	// --- SAFE POINT PROPAGATION ---
	// Payload: [LogSeq(8)]
	// Sent by Leader to Followers indicating the oldest log sequence 
	// required by the cluster (min of all replica slots).
	OpCodeReplSafePoint    uint8 = 0x55 

	OpCodeQuit      uint8 = 0xFF

	// Journal Specific Ops
	OpJournalSet    uint8 = 1
	OpJournalDelete uint8 = 2
	OpJournalCommit uint8 = 3 
)

// Response Status Codes
const (
	ResStatusOK             = 0x00
	ResStatusErr            = 0x01
	ResStatusNotFound       = 0x02
	ResStatusTxRequired     = 0x03
	ResStatusTxTimeout      = 0x04
	ResStatusTxConflict     = 0x05
	ResTxInProgress         = 0x06
	ResStatusServerBusy     = 0x07
	ResStatusEntityTooLarge = 0x08
	ResStatusMemoryLimit    = 0x09
)

// Errors
var (
	ErrKeyNotFound         = errors.New("key does not exist")
	ErrCrcMismatch         = errors.New("crc checksum mismatch")
	ErrClosed              = errors.New("store closed")
	ErrCommandTooLarge     = errors.New("command line too large")
	ErrConflict            = errors.New("transaction conflict")
	ErrBusy                = errors.New("server busy")
	ErrTransactionTimeout  = errors.New("transaction timeout")
	ErrReadOnly            = errors.New("server is read-only")
	ErrMemoryLimitExceeded = errors.New("memory limit exceeded")
	ErrDatabaseNotFound    = errors.New("database not found")
)

var Crc32Table = crc32.MakeTable(crc32.Castagnoli)

// LogEntry represents a single operation in the WAL and Memory.
type LogEntry struct {
	LogSeq uint64
	OpCode uint8
	Key    []byte
	Value  []byte
	Offset int64 // Virtual Offset in the WAL (internal use for checkpoints)
}

type Entry struct {
	Key      string
	Value    []byte
	IsDelete bool
	TxID     uint64
	LogSeq   uint64
}
