// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import "encoding/binary"

// encodeWALRecord serializes a WALRecord to its on-disk payload.
func encodeWALRecord(rec WALRecord) []byte {
	var bodyLen int
	switch rec.Type {
	case WALRecordSet:
		bodyLen = 4 + len(rec.Key) + 4 + len(rec.Value)
	case WALRecordDelete:
		bodyLen = 4 + len(rec.Key)
	}
	buf := make([]byte, LogRecordHeaderSize+bodyLen)
	buf[0] = byte(rec.Type)
	binary.BigEndian.PutUint64(buf[1:], rec.XID)
	binary.BigEndian.PutUint64(buf[9:], rec.OpID)

	switch rec.Type {
	case WALRecordSet:
		off := LogRecordHeaderSize
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
		off += 4
		copy(buf[off:], rec.Key)
		off += len(rec.Key)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Value)))
		off += 4
		copy(buf[off:], rec.Value)
	case WALRecordDelete:
		off := LogRecordHeaderSize
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
		off += 4
		copy(buf[off:], rec.Key)
	}
	return buf
}

func decodeWALRecord(payload []byte) (WALRecord, error) {
	if len(payload) < LogRecordHeaderSize {
		return WALRecord{}, ErrCorruptData
	}
	rec := WALRecord{
		Type: WALRecordType(payload[0]),
		XID:  binary.BigEndian.Uint64(payload[1:]),
		OpID: binary.BigEndian.Uint64(payload[9:]),
	}
	body := payload[LogRecordHeaderSize:]
	switch rec.Type {
	case WALRecordSet:
		if len(body) < 4 {
			return WALRecord{}, ErrCorruptData
		}
		klen := int(binary.BigEndian.Uint32(body[0:]))
		off := 4
		if off+klen+4 > len(body) {
			return WALRecord{}, ErrCorruptData
		}
		rec.Key = append([]byte(nil), body[off:off+klen]...)
		off += klen
		vlen := int(binary.BigEndian.Uint32(body[off:]))
		off += 4
		if off+vlen > len(body) {
			return WALRecord{}, ErrCorruptData
		}
		rec.Value = append([]byte(nil), body[off:off+vlen]...)
	case WALRecordDelete:
		if len(body) < 4 {
			return WALRecord{}, ErrCorruptData
		}
		klen := int(binary.BigEndian.Uint32(body[0:]))
		off := 4
		if off+klen > len(body) {
			return WALRecord{}, ErrCorruptData
		}
		rec.Key = append([]byte(nil), body[off:off+klen]...)
	case WALRecordBegin, WALRecordCommit, WALRecordAbort:
	default:
		return WALRecord{}, ErrCorruptData
	}
	return rec, nil
}

func peekOpID(payload []byte) (uint64, bool) {
	if len(payload) < LogRecordHeaderSize {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[9:17]), true
}

func frameSize(payloadLen int) int64 {
	return int64(LogFrameHeaderSize + payloadLen)
}
