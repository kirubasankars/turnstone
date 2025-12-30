package main

import (
	"sync/atomic"
	"time"
)

// Checkpoint maps a Log Sequence Number (LogID) to a physical WAL offset.
type Checkpoint struct {
	LogID  uint64 // The physical sequence number in the WAL.
	Offset int64  // The byte offset in the file.
}

// ReplicaState tracks the progress of a connected follower.
type ReplicaState struct {
	LogID    uint64 // The last LogID acknowledged by the replica.
	LastSeen time.Time
}

// StoreStats holds a snapshot of internal metrics.
type StoreStats struct {
	KeyCount               int
	IndexSizeBytes         int64
	Uptime                 string
	ActiveSnapshots        int
	Offset                 int64
	NextLSN                uint64
	NextLogID              uint64
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
	readLSN       uint64              // Snapshot LSN.
	cancelled     atomic.Bool         // Cancellation flag.
	isCompaction  bool                // Maintenance flag.
	isReplication bool                // Replication flag.
	accessMap     map[string]struct{} // Read/Write set for conflict detection.
}

// LogEntry is the logical op for replication transport.
type LogEntry struct {
	LogID  uint64
	LSN    uint64
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
