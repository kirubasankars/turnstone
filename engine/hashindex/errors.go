// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "errors"

// ErrArenaLimit is returned when a shard buffer grow would exceed MaxArenaBytes.
var ErrArenaLimit = errors.New("index arena size exceeds limit")
