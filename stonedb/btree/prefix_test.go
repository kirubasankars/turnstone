// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree_test

import (
	"encoding/binary"
	"testing"

	"turnstone/stonedb/btree"
)

func TestBytesPrefixWithSuffix(t *testing.T) {
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	prefix := []byte("!sys!garbage!")
	k := make([]byte, len(prefix)+4)
	copy(k, prefix)
	binary.BigEndian.PutUint32(k[len(prefix):], 99)
	if err := tree.Put(k, []byte{0, 0, 0, 0, 0, 0, 0, 4}); err != nil {
		t.Fatal(err)
	}

	limit := make([]byte, len(prefix))
	copy(limit, prefix)
	limit[len(limit)-1]++

	it := tree.NewIterator(&btree.Range{Start: prefix, Limit: limit})
	defer it.Release()
	if !it.First() {
		t.Fatal("expected prefix key")
	}
	if it.Key() == nil || len(it.Key()) != len(prefix)+4 {
		t.Fatalf("unexpected key: %v", it.Key())
	}
}
