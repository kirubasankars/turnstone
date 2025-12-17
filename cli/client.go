package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// OpCodes (Must match server)
const (
	OpCodePing    = 0x01
	OpCodeGet     = 0x02
	OpCodeSet     = 0x03
	OpCodeDel     = 0x04
	OpCodeBegin   = 0x10
	OpCodeCommit  = 0x11
	OpCodeAbort   = 0x12
	OpCodeStat    = 0x20
	OpCodeCompact = 0x21
	OpCodeQuit    = 0xFF
)

// Response Codes
const (
	ResStatusOK         = 0x00
	ResStatusErr        = 0x01
	ResStatusNotFound   = 0x02
	ResStatusTxRequired = 0x03
	ResStatusTxTimeout  = 0x04
	ResStatusTxConflict = 0x05
	ResStatusServerBusy = 0x06
)

func main() {
	host := flag.String("host", "localhost:6379", "Server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *host)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to", *host)
	fmt.Println("Commands: get <k>, set <k> <v>, del <k>, begin, commit, abort, stat, compact, clear, quit")

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "clear", "cls":
			// ANSI Escape Code: \033[H (Move cursor to top-left) \033[2J (Clear entire screen)
			fmt.Print("\033[H\033[2J")
			fmt.Print("> ")
			continue // Skip the network send/read for this loop
		case "ping":
			send(conn, OpCodePing, nil)
		case "stat":
			send(conn, OpCodeStat, nil)
		case "compact":
			send(conn, OpCodeCompact, nil)
		case "begin":
			send(conn, OpCodeBegin, nil)
		case "commit":
			send(conn, OpCodeCommit, nil)
		case "abort":
			send(conn, OpCodeAbort, nil)
		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				fmt.Print("> ")
				continue
			}
			send(conn, OpCodeGet, []byte(parts[1]))
		case "del":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
				fmt.Print("> ")
				continue
			}
			send(conn, OpCodeDel, []byte(parts[1]))
		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				fmt.Print("> ")
				continue
			}
			key := parts[1]
			val := parts[2]
			payload := make([]byte, 4+len(key)+len(val))
			binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
			copy(payload[4:], key)
			copy(payload[4+len(key):], val)
			send(conn, OpCodeSet, payload)
		case "quit", "exit":
			send(conn, OpCodeQuit, nil)
			return
		default:
			fmt.Println("Unknown command")
			fmt.Print("> ")
			continue
		}

		// Wait for response. If returns false, connection is dead.
		if !readResponse(conn) {
			fmt.Println("\nConnection terminated.")
			return
		}
		fmt.Print("> ")
	}
}

func send(w io.Writer, op byte, payload []byte) {
	header := make([]byte, 5)
	header[0] = op
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	if _, err := w.Write(header); err != nil {
		fmt.Printf("Write error: %v\n", err)
		return
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			fmt.Printf("Payload write error: %v\n", err)
			return
		}
	}
}

// readResponse reads the protocol response.
// Returns true if connection is healthy, false if connection closed/error.
func readResponse(r io.Reader) bool {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF {
			fmt.Println("Server closed the connection.")
		} else {
			fmt.Printf("Read error: %v\n", err)
		}
		return false // Signal main loop to exit
	}

	status := header[0]
	length := binary.BigEndian.Uint32(header[1:])

	var body []byte
	if length > 0 {
		body = make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			fmt.Printf("Body read error: %v\n", err)
			return false
		}
	}

	switch status {
	case ResStatusOK:
		if len(body) > 0 {
			fmt.Printf("OK: %s\n", string(body))
		} else {
			fmt.Println("OK")
		}
	case ResStatusErr:
		fmt.Printf("ERR: %s\n", string(body))
	case ResStatusNotFound:
		fmt.Println("(nil)")
	case ResStatusTxRequired:
		fmt.Println("ERR: Transaction Required")
	case ResStatusTxTimeout:
		fmt.Println("ERR: Transaction Timeout")
	case ResStatusTxConflict:
		fmt.Println("ERR: Conflict Detected (Retry)")
	case ResStatusServerBusy:
		fmt.Println("ERR: Server Busy")
	default:
		fmt.Printf("Unknown Status: %x Body: %s\n", status, string(body))
	}

	return true // Connection still alive
}
