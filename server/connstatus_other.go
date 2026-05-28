//go:build !unix

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package server

import "net"

// connAppearsClosed has no reliable non-destructive implementation on this
// platform; always assume the connection is still open, matching the prior
// behavior of relying solely on WaitForQuorum's own timeout.
func connAppearsClosed(conn net.Conn) bool {
	return false
}
