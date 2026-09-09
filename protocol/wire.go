// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package protocol

import "encoding/binary"

// AppendHeader appends a 5-byte request/response header to buf.
func AppendHeader(buf []byte, op uint8, payloadLen int) []byte {
	var header [ProtoHeaderSize]byte
	header[0] = op
	binary.BigEndian.PutUint32(header[1:], uint32(payloadLen))
	return append(buf, header[:]...)
}

// EncodeFrame returns a complete wire frame: header plus payload.
func EncodeFrame(op uint8, payload []byte) []byte {
	buf := AppendHeader(nil, op, len(payload))
	if len(payload) > 0 {
		buf = append(buf, payload...)
	}
	return buf
}

// StatusName returns a human-readable name for a response status byte.
func StatusName(status byte) string {
	switch status {
	case ResStatusOK:
		return "OK"
	case ResStatusErr:
		return "ERR"
	case ResStatusNotFound:
		return "NOT_FOUND"
	case ResStatusTxRequired:
		return "TX_REQUIRED"
	case ResStatusTxTimeout:
		return "TX_TIMEOUT"
	case ResStatusTxConflict:
		return "TX_CONFLICT"
	case ResStatusTxInProgress:
		return "TX_IN_PROGRESS"
	case ResStatusServerBusy:
		return "SERVER_BUSY"
	case ResStatusEntityTooLarge:
		return "ENTITY_TOO_LARGE"
	case ResStatusMemoryLimit:
		return "MEMORY_LIMIT"
	default:
		return "UNKNOWN"
	}
}
