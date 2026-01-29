package stonedb

import (
	"encoding/binary"
	"errors"
	"math"
)

func encodeWALIndexKey(opID uint64) []byte {
	k := make([]byte, len(sysWALIndexPrefix)+8)
	copy(k, sysWALIndexPrefix)
	binary.BigEndian.PutUint64(k[len(sysWALIndexPrefix):], opID)
	return k
}

func decodeWALIndexKey(key []byte) uint64 {
	if len(key) < len(sysWALIndexPrefix)+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(sysWALIndexPrefix):])
}

func encodeIndexKey(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+9)
	copy(out, key)
	out[len(key)] = 0x00
	binary.BigEndian.PutUint64(out[len(key)+1:], math.MaxUint64-ts)
	return out
}

func decodeIndexKey(data []byte) ([]byte, uint64, error) {
	if len(data) < 9 {
		return nil, 0, errors.New("invalid key length")
	}
	key := data[:len(data)-9]
	invTs := binary.BigEndian.Uint64(data[len(data)-8:])
	return key, math.MaxUint64 - invTs, nil
}

func (m *EntryMeta) Encode() []byte {
	buf := make([]byte, MetaSize)
	binary.BigEndian.PutUint32(buf[0:], m.FileID)
	binary.BigEndian.PutUint64(buf[4:], uint64(m.ValueOffset)) // 8 bytes for offset
	binary.BigEndian.PutUint32(buf[12:], m.ValueLen)
	binary.BigEndian.PutUint64(buf[16:], m.TransactionID)
	binary.BigEndian.PutUint64(buf[24:], m.OperationID)
	if m.IsTombstone {
		buf[32] = 1
	}
	return buf
}

func decodeEntryMeta(data []byte) (*EntryMeta, error) {
	if len(data) < MetaSize {
		return nil, errors.New("invalid meta length")
	}
	m := &EntryMeta{}
	m.FileID = binary.BigEndian.Uint32(data[0:])
	m.ValueOffset = int64(binary.BigEndian.Uint64(data[4:])) // 8 bytes for offset
	m.ValueLen = binary.BigEndian.Uint32(data[12:])
	m.TransactionID = binary.BigEndian.Uint64(data[16:])
	m.OperationID = binary.BigEndian.Uint64(data[24:])
	m.IsTombstone = data[32] == 1
	return m, nil
}
