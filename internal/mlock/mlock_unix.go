// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package mlock

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Lock pins b in RAM so the kernel will not swap those pages.
func Lock(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	if err := unix.Mlock(b); err != nil {
		return err
	}
	return nil
}

// Unlock releases a previous Lock. Munmap also unlocks.
func Unlock(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = unix.Munlock(b)
}

func Supported() bool { return true }

func IsDenied(err error) bool {
	return errors.Is(err, ErrUnsupported) ||
		errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.ENOMEM) || errors.Is(err, unix.EAGAIN)
}
