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
	header [HeaderSize]byte
}

// Mutation tracks a committed write for SSI conflict detection.
type Mutation struct {
	Key    string
	Offset int64
}

// request wraps a client's transaction for processing by the single-writer loop.
type request struct {
	ops         []bufferedOp // Contains ALL ops (Gets + Sets + Deletes)
	resp        chan error
	opLens      []int
	readVersion int64
	generation  uint64
	cancelled   atomic.Bool

	// accessMap is derived from ops.
	// It is the set of ALL keys touched by the transaction (Read + Write).
	accessMap map[string]struct{}

	batchSize int64
}

// txState tracks the state of an active client transaction context.
type txState struct {
	active       bool
	readOnly     bool // Default true
	isSyncClient bool // True if this connection has performed a CDC sync
	deadline     time.Time
	readVersion  int64
	generation   uint64
	ops          []bufferedOp // The complete history of the transaction
	memUsage     int64

	// Multi-DB Support
	dbIndex int // Current selected database (0-15)
}
