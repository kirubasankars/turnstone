// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package mlock

import (
	"os"
	"testing"
)

func TestLockUnlock(t *testing.T) {
	if !Supported() {
		if err := Lock(make([]byte, 8)); err != ErrUnsupported {
			t.Fatalf("want ErrUnsupported, got %v", err)
		}
		return
	}
	b := make([]byte, os.Getpagesize())
	if err := Lock(b); err != nil {
		if IsDenied(err) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	b[0] = 1
	Unlock(b)
}
