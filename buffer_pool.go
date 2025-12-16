package main

import "sync"

// bufferPool reduces Garbage Collector pressure by reusing byte slices
var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

func getBuffer(size int) *[]byte {
	ptr := bufferPool.Get().(*[]byte)
	if cap(*ptr) < size {
		b := make([]byte, size)
		ptr = &b
	}
	*ptr = (*ptr)[:size]
	return ptr
}

func putBuffer(bufPtr *[]byte) {
	if bufPtr == nil {
		return
	}
	// Don't pool massive buffers to avoid holding memory unnecessarily
	if cap(*bufPtr) > MaxPooledBuffer {
		return
	}
	// Reset length to 0 before pooling to prevent data leakage/confusion
	*bufPtr = (*bufPtr)[:0]
	bufferPool.Put(bufPtr)
}

