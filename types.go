package main

import (
	"sync/atomic"
	"time"
)

// Checkpoint maps a Log Sequence Number (LogSeq) to a physical WAL offset.
type Checkpoint struct {
	LogSeq uint64 // The physical sequence number in the WAL.
	Offset int64  // The byte offset in the file.
}

// ReplicaState tracks the progress of a connected replica.
type ReplicaState struct {
	LogSeq   uint64 // The last LogSeq acknowledged by the replica.
	LastSeen time.Time
}

// StoreStats holds a snapshot of internal metrics.
type StoreStats struct {
	KeyCount               int
	IndexSizeBytes         int64
	Uptime                 string
	ActiveSnapshots        int
	Offset                 int64
	NextTxID               uint64
	NextLogSeq             uint64
	ConflictCount          int64
	RecoveryDuration       time.Duration
	PendingOps             int
	QueueCapacity          int
	BytesWritten           int64
	BytesRead              int64
	SlowOps                int64
	LastCompactionDuration time.Duration
}

// bufferedOp represents a single operation within a pending transaction.
type bufferedOp struct {
	opType int
	key    string
	val    []byte
	header [HeaderSize]byte // Pre-allocated header for serializing.
}

// request wraps a client's transaction for processing by the single-writer loop.
type request struct {
	ops           []bufferedOp        // Contains ALL ops.
	resp          chan error          // Channel to return result.
	opLengths     []int               // Calculated lengths for serialization.
	readTxID      uint64              // Snapshot TxID.
	cancelled     atomic.Bool         // Cancellation flag.
	isReplication bool                // Replication flag.
	accessMap     map[string]struct{} // Read/Write set for conflict detection.
}

// LogEntry is the logical op for replication transport.
type LogEntry struct {
	LogSeq uint64
	TxID   uint64
	OpType uint8
	Key    []byte
	Value  []byte
}

// replPacket is an internal struct for multiplexing replication streams.
type replPacket struct {
	dbName string
	data   []byte // Raw encoded batch
	count  uint32
}
