// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package hashindex

import (
	"syscall"
	"testing"
)

func TestMappedBytesAlignToPageSize(t *testing.T) {
	page := int64(syscall.Getpagesize())
	buf, err := newShardBuffer(page + 1)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.close()
	if int64(len(buf.data)) != page*2 {
		t.Fatalf("expected %d-byte mapping, got %d", page*2, len(buf.data))
	}
}
