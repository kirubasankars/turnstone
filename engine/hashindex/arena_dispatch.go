// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

var (
	shardBufferNew     func(int64, bool) (*shardBuffer, error)
	shardBufferGrow    func(*shardBuffer, int64) error
	shardBufferRelease func(*shardBuffer)
)

func newShardBuffer(size int64) (*shardBuffer, error) {
	return shardBufferNew(size, false)
}

func newShardBufferLocked(size int64, lock bool) (*shardBuffer, error) {
	return shardBufferNew(size, lock)
}

func growShardBuffer(b *shardBuffer, minSize int64) error {
	return shardBufferGrow(b, minSize)
}

func releaseShardBuffer(b *shardBuffer) {
	shardBufferRelease(b)
}

func useHeapShardBuffers() func() {
	prevNew := shardBufferNew
	prevGrow := shardBufferGrow
	prevRelease := shardBufferRelease
	shardBufferNew = heapNewShardBuffer
	shardBufferGrow = heapGrowShardBuffer
	shardBufferRelease = heapReleaseShardBuffer
	return func() {
		shardBufferNew = prevNew
		shardBufferGrow = prevGrow
		shardBufferRelease = prevRelease
	}
}
