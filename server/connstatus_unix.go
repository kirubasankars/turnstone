//go:build unix

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package server

import (
	"net"
	"syscall"
)

// connAppearsClosed does a best-effort, non-destructive check for whether
// the peer has already closed its side of a TCP/TLS connection, using
// MSG_PEEK so it never consumes application bytes from the socket. It
// returns false ("assume still connected") whenever the check can't be
// performed reliably (e.g. not a raw TCP socket) -- this is purely an
// optimization layered on top of an already-bounded wait such as
// WaitForQuorum's timeout, so a false negative here only means we fall back
// to waiting out the existing timeout, never a source of new incorrect
// behavior.
func connAppearsClosed(conn net.Conn) bool {
	type netConnUnwrapper interface{ NetConn() net.Conn }
	raw := conn
	if nc, ok := conn.(netConnUnwrapper); ok {
		raw = nc.NetConn()
	}
	sc, ok := raw.(syscall.Conn)
	if !ok {
		return false
	}
	rawConn, err := sc.SyscallConn()
	if err != nil {
		return false
	}

	closed := false
	buf := make([]byte, 1)
	_ = rawConn.Read(func(fd uintptr) bool {
		n, _, rerr := syscall.Recvfrom(int(fd), buf, syscall.MSG_PEEK)
		switch {
		case rerr == syscall.EAGAIN || rerr == syscall.EWOULDBLOCK:
			// Nothing to peek right now; socket is readable-but-empty, so
			// the connection is still open.
		case rerr != nil:
			// Any other error (e.g. ECONNRESET) means the peer is gone.
			closed = true
		case n == 0:
			// Recvfrom returning 0 bytes with no error is an orderly EOF:
			// the peer closed its write side.
			closed = true
		}
		return true
	})
	return closed
}
