// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package btree_test

import (
	"testing"

	"turnstone/stonedb/btree"
)

func openTree(t *testing.T) *btree.Tree {
	t.Helper()
	dir := t.TempDir()
	tree, err := btree.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	return tree
}
