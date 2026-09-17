// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"errors"
	"os"
	"syscall"
)

// testingPreallocErr, when set, is returned from preallocateFile (tests only).
var testingPreallocErr error

func preallocateFile(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	if err := testingPreallocErr; err != nil {
		return err
	}
	return preallocateFileOS(f, size)
}

func isNoSpace(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.ENOSPC) || isQuotaExceeded(err)
}
