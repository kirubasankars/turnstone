package main

import (
	"sync/atomic"
	"time"
)

// bufferedOp represents a single operation within a pending transaction.
// It holds the pre-calculated header and the raw key/value data.
type bufferedOp struct {
	opType int
	key    string
	val    []byte
	header [HeaderSize]byte
}

// Mutation tracks a committed write for Serializable Snapshot Isolation (SSI) conflict detection.
type Mutation struct {
	Key    string
	Offset int64
}

// request wraps a client's transaction for processing by the single-writer loop in Store.
type request struct {
	ops         []bufferedOp // Contains ALL ops (Gets + Sets + Deletes)
	resp        chan error   // Channel to return the result to the goroutine
	opLens      []int        // Calculated lengths of entries in the journal
	readVersion int64        // The snapshot version this transaction read from
	generation  uint64       // The generation this transaction belongs to
	cancelled   atomic.Bool  // Atomic flag to signal timeout/cancellation

	// accessMap is derived from ops.
	// It is the set of ALL keys touched by the transaction (Read + Write) used for conflict detection.
	accessMap map[string]struct{}

	batchSize int64 // Total byte size of the batch (for memory limiting)
}

// txState tracks the state of an active client transaction context.
type txState struct {
	active       bool
	readOnly     bool // If true, SET/DEL operations are forbidden
	isSyncClient bool // True if this connection has performed a CDC sync (promoted connection)
	deadline     time.Time
	readVersion  int64
	generation   uint64
	ops          []bufferedOp // The complete history of the transaction
	memUsage     int64        // Approximate memory usage of pending writes

	// Multi-DB Support
	dbIndex int // Current selected database (0-15)
}
