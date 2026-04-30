//go:build !unix

package server

import "net"

// connAppearsClosed has no reliable non-destructive implementation on this
// platform; always assume the connection is still open, matching the prior
// behavior of relying solely on WaitForQuorum's own timeout.
func connAppearsClosed(conn net.Conn) bool {
	return false
}
