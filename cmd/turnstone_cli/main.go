package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"turnstone/client"
)

func main() {
	host := flag.String("host", "localhost:6379", "Server address")
	home := flag.String("home", ".", "Path to home directory containing certs/")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Setup Logger
	var logger *slog.Logger
	if *debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	var cl *client.Client
	var err error

	// 1. Connection Setup
	// Resolve certificate paths relative to home directory
	caPath := filepath.Join(*home, "certs", "ca.crt")
	certPath := filepath.Join(*home, "certs", "client.crt")
	keyPath := filepath.Join(*home, "certs", "client.key")

	// Check if certificates exist to determine connection mode
	if _, err := os.Stat(caPath); err == nil {
		fmt.Printf("Connecting to %s via mTLS (Home: %s)...\n", *host, *home)
		cl, err = client.NewMTLSClientHelper(*host, caPath, certPath, keyPath, logger)
	} else {
		// Fallback to insecure if certs are missing in the expected location
		fmt.Printf("Certificates not found at %s/certs. Connecting to %s via insecure TCP...\n", *home, *host)
		cl, err = client.NewClient(client.Config{
			Address:        *host,
			ConnectTimeout: 5 * time.Second,
			Logger:         logger,
		})
	}

	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	fmt.Println("Connected.")
	fmt.Println("Commands: select <db>, replicaof <host:port> <remote_db>, get <k>, set <k> <v>, del <k>, begin, commit, abort, stat, clear, quit")
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

	case "select":
		if len(parts) < 2 {
			fmt.Println("Usage: select <db>")
			return
		}
		err = cl.Select(parts[1])
		if err == nil {
			fmt.Println("OK")
		}

	case "replicaof":
		// Handle "replicaof no one" as stop replication
		if len(parts) >= 3 && parts[1] == "no" && parts[2] == "one" {
			err = cl.ReplicaOf("", "")
			if err == nil {
				fmt.Println("Replication stopped (NO ONE)")
			}
		} else if len(parts) < 3 {
			// Also allow empty args to stop
			err = cl.ReplicaOf("", "")
			if err == nil {
				fmt.Println("Replication stopped (Empty args)")
			}
		} else {
			// Normal case: replicaof <host> <db>
			err = cl.ReplicaOf(parts[1], parts[2])
			if err == nil {
				fmt.Printf("Replication started from %s/%s\n", parts[1], parts[2])
			}
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
	case errors.Is(err, client.ErrMemoryLimit):
		fmt.Println("ERR: Server Memory Limit Exceeded")
	default:
		fmt.Printf("ERR: %v\n", err)
	}
}
