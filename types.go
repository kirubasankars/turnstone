package main

import (
	"sync/atomic"
	"time"
)

// bufferedOp represents a single operation within a pending transaction.
type bufferedOp struct {
	opType int
	key    string
	val    []byte
}

// Mutation tracks a committed write for SSI conflict detection.
type Mutation struct {
	Key    string
	Offset int64
}

// request wraps a client's transaction for processing by the single-writer loop.
type request struct {
	ops         []bufferedOp    // Write operations (Set/Delete)
	reads       []string        // Read set for Anti-Dependency Checking (SSI)
	resp        chan error      // Channel to signal completion/error to client
	opLens      []int           // Byte lengths of operations for offset calculation
	readVersion int64           // The snapshot (WAL offset) this tx sees
	generation  uint64          // FIX: Validates that the file backing readVersion hasn't changed
	cancelled   atomic.Bool     // Flag to abort processing on timeout/disconnect
	accessMap   map[string]bool // Fast lookup for conflict checks against pending batches
	batchSize   int64           // Total bytes in this request (for backpressure)
}

// txState tracks the state of an active client transaction context.
type txState struct {
	active      bool
	deadline    time.Time
	readVersion int64
	generation  uint64 // FIX: The store generation this transaction belongs to
	ops         []bufferedOp
	reads       map[string]struct{}
	memUsage    int64
}

