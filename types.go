package main

import (
	"sync/atomic"
	"time"
)

// StoreStats holds a snapshot of internal metrics exposed via OpCodeStat.
type StoreStats struct {
	KeyCount         int
	IndexSizeBytes   int64 // Estimated memory usage of the index.
	Uptime           string
	ActiveSnapshots  int // Number of active readers.
	Offset           int64
	NextLSN          uint64
	ConflictCount    int64
	RecoveryDuration time.Duration

	// Backpressure Metrics
	PendingOps    int // Current depth of the write queue.
	QueueCapacity int // Max capacity of the write queue.

	// Day 2 / Operational Metrics
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

// request wraps a client's transaction for processing by the single-writer loop in Store.
type request struct {
	ops           []bufferedOp // Contains ALL ops (Gets + Sets + Deletes).
	resp          chan error   // Channel to return the result to the goroutine.
	opLens        []int        // Calculated lengths of entries in the journal.
	readLSN       uint64       // The snapshot LSN this transaction read from (for conflict checks).
	cancelled     atomic.Bool  // Atomic flag to signal timeout/cancellation.
	isCompaction  bool         // Flag to indicate this is a maintenance rewrite (preserve LSN).
	isReplication bool         // Flag to indicate this is a replication batch (preserve LSN).

	// accessMap is derived from ops.
	// It is the set of ALL keys touched by the transaction (Read + Write) used for conflict detection.
	accessMap map[string]struct{}
}

// txState tracks the state of an active client transaction context.
type txState struct {
	active   bool
	readOnly bool // If true, SET/DEL operations are forbidden.
	deadline time.Time
	readLSN  uint64
	ops      []bufferedOp // The complete history of the transaction.
}
