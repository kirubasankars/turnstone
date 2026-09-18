// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build !unix

package mlock

import "errors"

func Lock([]byte) error { return ErrUnsupported }

func Unlock([]byte) {}

func Supported() bool { return false }

func IsDenied(err error) bool { return errors.Is(err, ErrUnsupported) }
