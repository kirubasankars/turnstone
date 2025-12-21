package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
	OpCodeSelect  = 0x13
	OpCodeStat    = 0x20
	OpCodeCompact = 0x21
	OpCodeQuit    = 0xFF
)

// Response Codes
const (
	ResStatusOK             = 0x00
	ResStatusErr            = 0x01
	ResStatusNotFound       = 0x02
	ResStatusTxRequired     = 0x03
	ResStatusTxTimeout      = 0x04
	ResStatusTxConflict     = 0x05
	ResStatusServerBusy     = 0x06
	ResStatusEntityTooLarge = 0x07
	ResStatusGenMismatch    = 0x09
)

// serverResponse wraps the parsed protocol response or a network error
type serverResponse struct {
	status byte
	body   []byte
	err    error
}

func main() {
	host := flag.String("host", "localhost:6379", "Server address")
	caFile := flag.String("ca", "", "CA Certificate file")
	certFile := flag.String("cert", "", "Client Certificate file")
	keyFile := flag.String("key", "", "Client Key file")
	flag.Parse()

	var conn net.Conn
	var err error

	// mTLS Connection Strategy
	if *caFile != "" && *certFile != "" && *keyFile != "" {
		// Load CA
		caCert, readErr := os.ReadFile(*caFile)
		if readErr != nil {
			fmt.Printf("Failed to read CA file: %v\n", readErr)
			os.Exit(1)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			fmt.Println("Failed to parse CA certificate PEM.")
			os.Exit(1)
		}

		// Load Client Cert/Key
		cert, readErr := tls.LoadX509KeyPair(*certFile, *keyFile)
		if readErr != nil {
			fmt.Printf("Failed to load client keypair: %v\n", readErr)
			os.Exit(1)
		}

		tlsConfig := &tls.Config{
			RootCAs:      caCertPool,
			Certificates: []tls.Certificate{cert},
		}

		fmt.Printf("Connecting to %s via mTLS...\n", *host)
		conn, err = tls.Dial("tcp", *host, tlsConfig)
	} else {
		fmt.Printf("Connecting to %s via insecure TCP (server may reject if mTLS is enforced)...\n", *host)
		conn, err = net.Dial("tcp", *host)
	}

	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected.")
	fmt.Println("Commands: select <db>, get <k>, set <k> <v>, del <k>, begin, commit, abort, stat, compact, clear, quit")

	// 1. Async Input Reader
	inputChan := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			inputChan <- scanner.Text()
		}
		close(inputChan)
	}()

	// 2. Async Network Reader
	respChan := make(chan serverResponse)
	go func() {
		defer close(respChan)
		for {
			header := make([]byte, 5)
			if _, err := io.ReadFull(conn, header); err != nil {
				respChan <- serverResponse{err: err}
				return
			}

			status := header[0]
			length := binary.BigEndian.Uint32(header[1:])

			var body []byte
			if length > 0 {
				body = make([]byte, length)
				if _, err := io.ReadFull(conn, body); err != nil {
					respChan <- serverResponse{err: err}
					return
				}
			}
			respChan <- serverResponse{status: status, body: body}
		}
	}()

	fmt.Print("> ")

	// 3. Event Loop
	for {
		select {
		// Case A: Server sent data (or closed connection) while we were idle
		case resp, ok := <-respChan:
			if !ok || resp.err != nil {
				if resp.err == io.EOF {
					fmt.Println("\nServer closed the connection.")
				} else {
					fmt.Printf("\nConnection error: %v\n", resp.err)
				}
				return
			}
			printResponse(resp.status, resp.body)
			fmt.Print("> ")

		// Case B: User entered a command
		case line, ok := <-inputChan:
			if !ok {
				return // Stdin closed
			}

			line = strings.TrimSpace(line)
			if line == "" {
				fmt.Print("> ")
				continue
			}

			parts := strings.SplitN(line, " ", 3)
			cmd := strings.ToLower(parts[0])

			// Handle local commands that don't need network
			if cmd == "clear" || cmd == "cls" {
				fmt.Print("\033[H\033[2J")
				fmt.Print("> ")
				continue
			}

			// Process and Send Command
			shouldExpectResponse := true
			switch cmd {
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
			case "select":
				if len(parts) < 2 {
					fmt.Println("Usage: select <db_index>")
					shouldExpectResponse = false
				} else {
					dbIdx, err := strconv.Atoi(parts[1])
					if err != nil || dbIdx < 0 || dbIdx > 255 {
						fmt.Println("Invalid DB index (0-255)")
						shouldExpectResponse = false
					} else {
						send(conn, OpCodeSelect, []byte{byte(dbIdx)})
					}
				}
			case "get":
				if len(parts) < 2 {
					fmt.Println("Usage: get <key>")
					shouldExpectResponse = false
				} else {
					send(conn, OpCodeGet, []byte(parts[1]))
				}
			case "del":
				if len(parts) < 2 {
					fmt.Println("Usage: del <key>")
					shouldExpectResponse = false
				} else {
					send(conn, OpCodeDel, []byte(parts[1]))
				}
			case "set":
				if len(parts) < 3 {
					fmt.Println("Usage: set <key> <value>")
					shouldExpectResponse = false
				} else {
					key := parts[1]
					val := parts[2]
					payload := make([]byte, 4+len(key)+len(val))
					binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
					copy(payload[4:], key)
					copy(payload[4+len(key):], val)
					send(conn, OpCodeSet, payload)
				}
			case "quit", "exit":
				send(conn, OpCodeQuit, nil)
				return
			default:
				fmt.Println("Unknown command")
				shouldExpectResponse = false
			}

			if !shouldExpectResponse {
				fmt.Print("> ")
				continue
			}

			// Block until we get the specific response for this command
			// (or until the server disconnects)
			resp, ok := <-respChan
			if !ok || resp.err != nil {
				fmt.Println("\nConnection terminated.")
				return
			}
			printResponse(resp.status, resp.body)
			fmt.Print("> ")
		}
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

func printResponse(status byte, body []byte) {
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
}
