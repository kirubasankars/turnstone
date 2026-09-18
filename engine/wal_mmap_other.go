// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build !unix

package engine

import "os"

func mmapWALFile(f *os.File, size int64) ([]byte, error) { return nil, nil }

func unmapWAL(mapping []byte) {}

func adviseWALMapping(mapping []byte, advice int) {}

func adviseWALRange(mapping []byte, off, n int64, advice int) {}

func walAdviseSequential() int { return 0 }
func walAdviseRandom() int     { return 0 }
