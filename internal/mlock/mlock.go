// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package mlock

import "errors"

// ErrUnsupported is returned when Lock is requested on a platform without mlock.
var ErrUnsupported = errors.New("mlock is not supported on this platform")
