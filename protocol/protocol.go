// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package protocol

import (
	"errors"
	"hash/crc32"
	"time"
)

// --- Constants ---

const (
	DefaultWriteTimeout = 5 * time.Second
	IdleTimeout         = 3 * 60 * time.Second
	MaxTxDuration       = 30 * time.Second  // Strict 30s limit
	MaxTxSize           = 200 * 1024 * 1024 // 200MB Limit
	MaxValueSize        = 4 * 1024 * 1024   // 4MB Limit
	MaxCommandSize      = 512 * 1024 * 1024 // 512MB Limit (Must fit in uint32)

	ProtoHeaderSize = 5
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
	OpCodePromote   uint8 = 0x34
	OpCodeStepDown  uint8 = 0x35
	OpCodeFlushDB   uint8 = 0x37 // Wipe Database

	OpCodeReplHello uint8 = 0x50
	OpCodeReplAck   uint8 = 0x52

	// --- SNAPSHOT OPCODES ---
	OpCodeReplSnapshot     uint8 = 0x53 // Bulk data payload (Full Sync)
	OpCodeReplSnapshotDone uint8 = 0x54 // Transition signal to WAL streaming

	// --- SAFE POINT PROPAGATION ---
	// Payload: [LogSeq(8)]
	// Sent by Leader to Followers indicating the oldest log sequence
	// required by the cluster (min of all replica slots).
	OpCodeReplSafePoint uint8 = 0x55

	// --- TIMELINE PROPAGATION ---
	// Payload: [TimelineID(8)]
	// Sent by Leader to Followers to indicate the current timeline.
	// Sent initially after handshake and upon any timeline fork (Promotion).
	OpCodeReplTimeline uint8 = 0x56

	// OpCodeReplLogSegment carries a raw byte range of complete WAL frames.
	// Inner payload (after DB name/count): [StartOffset(8)][EndOffset(8)][RawBytes...]
	// EndOffset-StartOffset must equal len(RawBytes). Frames are never split.
	OpCodeReplLogSegment uint8 = 0x57

	OpCodeQuit uint8 = 0xFF

	// Begin payload flags (OpCodeBegin body).
	// Empty payload opens a writable transaction on PRIMARY (legacy default).
	// A one-byte payload of BeginReadOnly opens a snapshot read transaction.
	BeginReadOnly uint8 = 0

	// Journal ops used by store.ApplyBatch (client write path).
	OpJournalBegin  uint8 = 4
	OpJournalSet    uint8 = 1
	OpJournalDelete uint8 = 2
	OpJournalCommit uint8 = 3
	OpJournalAbort  uint8 = 5
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
)

// Errors
var (
	ErrKeyNotFound = errors.New("key does not exist")
)

var Crc32Table = crc32.MakeTable(crc32.Castagnoli)

// LogEntry represents a single operation in the WAL and Memory.
type LogEntry struct {
	LogSeq uint64
	OpCode uint8
	Key    []byte
	Value  []byte
}

// IsASCII validates if a string contains only ASCII characters.
func IsASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 127 {
			return false
		}
	}
	return true
}
