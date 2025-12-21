package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"turnstone/client" // Imports the local client package
)

func main() {
	host := flag.String("host", "localhost:6379", "Server address")
	caFile := flag.String("ca", "", "CA Certificate file")
	certFile := flag.String("cert", "", "Client Certificate file")
	keyFile := flag.String("key", "", "Client Key file")
	flag.Parse()

	var cl *client.Client
	var err error

	// 1. Connection Setup
	if *caFile != "" && *certFile != "" && *keyFile != "" {
		fmt.Printf("Connecting to %s via mTLS...\n", *host)
		cl, err = client.NewMTLSClientHelper(*host, *caFile, *certFile, *keyFile)
	} else {
		fmt.Printf("Connecting to %s via insecure TCP...\n", *host)
		cl, err = client.NewClient(client.Config{
			Address:        *host,
			ConnectTimeout: 5 * time.Second,
		})
	}

	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	fmt.Println("Connected.")
	fmt.Println("Commands: select <db>, get <k>, set <k> <v>, del <k>, begin, commit, abort, stat, compact, clear, quit")
	fmt.Print("> ")

	// 2. Interactive Loop
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		// Handle local commands
		if cmd == "clear" || cmd == "cls" {
			fmt.Print("\033[H\033[2J")
			fmt.Print("> ")
			continue
		}
		if cmd == "quit" || cmd == "exit" {
			return
		}

		handleCommand(cl, cmd, parts)
		fmt.Print("> ")
	}
}

func handleCommand(cl *client.Client, cmd string, parts []string) {
	var err error
	var result []byte
	var resultStr string

	switch cmd {
	case "ping":
		err = cl.Ping()
		if err == nil {
			fmt.Println("PONG")
		}

	case "stat":
		resultStr, err = cl.Stat()
		if err == nil {
			fmt.Println(resultStr)
		}

	case "compact":
		err = cl.Compact()
		if err == nil {
			fmt.Println("OK")
		}

	case "begin":
		err = cl.Begin()
		if err == nil {
			fmt.Println("OK")
		}

	case "commit":
		err = cl.Commit()
		if err == nil {
			fmt.Println("OK")
		}

	case "abort":
		err = cl.Abort()
		if err == nil {
			fmt.Println("OK")
		}

	case "select":
		if len(parts) < 2 {
			fmt.Println("Usage: select <db_index>")
			return
		}
		dbIdx, atoiErr := strconv.Atoi(parts[1])
		if atoiErr != nil {
			fmt.Println("Invalid integer for DB index")
			return
		}
		err = cl.Select(dbIdx)
		if err == nil {
			fmt.Println("OK")
		}

	case "get":
		if len(parts) < 2 {
			fmt.Println("Usage: get <key>")
			return
		}
		result, err = cl.Get(parts[1])
		if err == nil {
			fmt.Printf("OK: %s\n", string(result))
		}

	case "del":
		if len(parts) < 2 {
			fmt.Println("Usage: del <key>")
			return
		}
		err = cl.Del(parts[1])
		if err == nil {
			fmt.Println("OK")
		}

	case "set":
		if len(parts) < 3 {
			fmt.Println("Usage: set <key> <value>")
			return
		}
		err = cl.Set(parts[1], []byte(parts[2]))
		if err == nil {
			fmt.Println("OK")
		}

	default:
		fmt.Println("Unknown command")
		return
	}

	if err != nil {
		printError(err)
	}
}

func printError(err error) {
	switch {
	case errors.Is(err, client.ErrNotFound):
		fmt.Println("(nil)")
	case errors.Is(err, client.ErrTxRequired):
		fmt.Println("ERR: Transaction Required")
	case errors.Is(err, client.ErrTxTimeout):
		fmt.Println("ERR: Transaction Timeout")
	case errors.Is(err, client.ErrTxConflict):
		fmt.Println("ERR: Conflict Detected (Retry)")
	case errors.Is(err, client.ErrServerBusy):
		fmt.Println("ERR: Server Busy")
	case errors.Is(err, client.ErrEntityTooLarge):
		fmt.Println("ERR: Entity Too Large")
	case errors.Is(err, client.ErrGenMismatch):
		fmt.Println("ERR: Generation Mismatch")
	default:
		fmt.Printf("ERR: %v\n", err)
	}
}
