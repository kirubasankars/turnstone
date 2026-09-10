// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import "encoding/binary"

// encodeRecord serializes a Record to its on-disk payload.
func encodeRecord(rec Record) []byte {
	var bodyLen int
	switch rec.Type {
	case RecordSet:
		bodyLen = 4 + len(rec.Key) + 4 + len(rec.Value)
	case RecordDelete:
		bodyLen = 4 + len(rec.Key)
	}
	buf := make([]byte, LogRecordHeaderSize+bodyLen)
	buf[0] = byte(rec.Type)
	binary.BigEndian.PutUint64(buf[1:], rec.XID)

	switch rec.Type {
	case RecordSet:
		off := LogRecordHeaderSize
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
		off += 4
		copy(buf[off:], rec.Key)
		off += len(rec.Key)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Value)))
		off += 4
		copy(buf[off:], rec.Value)
	case RecordDelete:
		off := LogRecordHeaderSize
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
		off += 4
		copy(buf[off:], rec.Key)
	}
	return buf
}

func decodeRecord(payload []byte) (Record, error) {
	if len(payload) < LogRecordHeaderSize {
		return Record{}, ErrCorruptData
	}
	rec := Record{
		Type: RecordType(payload[0]),
		XID:  binary.BigEndian.Uint64(payload[1:]),
	}
	body := payload[LogRecordHeaderSize:]
	switch rec.Type {
	case RecordSet:
		if len(body) < 4 {
			return Record{}, ErrCorruptData
		}
		klen := int(binary.BigEndian.Uint32(body[0:]))
		off := 4
		if off+klen+4 > len(body) {
			return Record{}, ErrCorruptData
		}
		rec.Key = append([]byte(nil), body[off:off+klen]...)
		off += klen
		vlen := int(binary.BigEndian.Uint32(body[off:]))
		off += 4
		if off+vlen > len(body) {
			return Record{}, ErrCorruptData
		}
		rec.Value = append([]byte(nil), body[off:off+vlen]...)
	case RecordDelete:
		if len(body) < 4 {
			return Record{}, ErrCorruptData
		}
		klen := int(binary.BigEndian.Uint32(body[0:]))
		off := 4
		if off+klen > len(body) {
			return Record{}, ErrCorruptData
		}
		rec.Key = append([]byte(nil), body[off:off+klen]...)
	case RecordBegin, RecordCommit, RecordAbort:
	default:
		return Record{}, ErrCorruptData
	}
	return rec, nil
}

func frameSize(payloadLen int) int64 {
	return int64(LogFrameHeaderSize + payloadLen)
}
