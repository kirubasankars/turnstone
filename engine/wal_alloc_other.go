// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build !unix

package engine

import "os"

func isQuotaExceeded(error) bool { return false }

func preallocateFileOS(f *os.File, size int64) error {
	return f.Truncate(size)
}
