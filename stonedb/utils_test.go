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
	"bytes"
	"testing"
)

func TestEncoding_IndexKey(t *testing.T) {
	key := []byte("mykey")
	ts := uint64(12345)

	encoded := encodeIndexKey(key, ts)

	decodedKey, decodedTs, err := decodeIndexKey(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decodedKey, key) {
		t.Errorf("Key mismatch: %s != %s", decodedKey, key)
	}
	if decodedTs != ts {
		t.Errorf("TS mismatch: %d != %d", decodedTs, ts)
	}

	// Test Sort Order (Descending TS for MVCC)
	tsOld := uint64(1000)
	tsNew := uint64(2000)

	encodedOld := encodeIndexKey(key, tsOld)
	encodedNew := encodeIndexKey(key, tsNew)

	// "New" should be smaller/before "Old" because (Max - 2000) < (Max - 1000)
	if bytes.Compare(encodedNew, encodedOld) >= 0 {
		t.Error("Expected newer key (higher TS) to be smaller lexicographically")
	}
}

func TestUtils_EncodingErrors(t *testing.T) {
	// Test decodeWALIndexKey short buffer
	if id := decodeWALIndexKey([]byte("short")); id != 0 {
		t.Errorf("Expected 0 for short key, got %d", id)
	}

	// Test decodeIndexKey short buffer
	if _, _, err := decodeIndexKey([]byte{1, 2}); err == nil {
		t.Error("Expected error for short index key")
	}

	// Test decodeEntryMeta short buffer
	if _, err := decodeEntryMeta(make([]byte, 10)); err == nil {
		t.Error("Expected error for short meta buffer")
	}
}

func TestEntryMeta_Encode(t *testing.T) {
	m := EntryMeta{IsTombstone: true}
	buf := m.Encode()
	if buf[28] != 1 {
		t.Error("Failed to encode tombstone")
	}
	m2, _ := decodeEntryMeta(buf)
	if !m2.IsTombstone {
		t.Error("Failed to decode tombstone")
	}
}
