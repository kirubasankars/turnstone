// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"errors"

	"turnstone/engine/hashindex"
)

func mapIndexError(err error) error {
	if errors.Is(err, hashindex.ErrArenaLimit) {
		return ErrIndexArenaLimit
	}
	return err
}

func (idx *Index) SetMaxArenaBytes(n int64) {
	if idx.hash != nil {
		idx.hash.SetMaxArenaBytes(n)
	}
}

func (idx *Index) SetEnforceLimit(enforce bool) {
	if idx.hash != nil {
		idx.hash.SetEnforceLimit(enforce)
	}
}

func (idx *Index) RecalcUsedBytes() {
	if idx.hash != nil {
		idx.hash.RecalcUsedBytes()
	}
}

func (idx *Index) IndexArenaUsedBytes() int64 {
	if idx.hash == nil {
		return 0
	}
	return idx.hash.UsedBytes()
}
