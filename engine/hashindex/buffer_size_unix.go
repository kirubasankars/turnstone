// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package hashindex

func bufferAllocSize(n int64) int64 {
	return alignShardBufferSize(n)
}
