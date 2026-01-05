// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
	binary.BigEndian.PutUint32(buf[4:], m.ValueOffset)
	binary.BigEndian.PutUint32(buf[8:], m.ValueLen)
	binary.BigEndian.PutUint64(buf[12:], m.TransactionID)
	binary.BigEndian.PutUint64(buf[20:], m.OperationID)
	if m.IsTombstone {
		buf[28] = 1
	}
	return buf
}

func decodeEntryMeta(data []byte) (*EntryMeta, error) {
	if len(data) < MetaSize {
		return nil, errors.New("invalid meta length")
	}
	m := &EntryMeta{}
	m.FileID = binary.BigEndian.Uint32(data[0:])
	m.ValueOffset = binary.BigEndian.Uint32(data[4:])
	m.ValueLen = binary.BigEndian.Uint32(data[8:])
	m.TransactionID = binary.BigEndian.Uint64(data[12:])
	m.OperationID = binary.BigEndian.Uint64(data[20:])
	m.IsTombstone = data[28] == 1
	return m, nil
}
