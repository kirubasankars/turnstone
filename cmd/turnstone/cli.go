// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"turnstone/client"
)

func newCLICmd() *cobra.Command {
	var host string
	var debug bool
	var asAdmin bool

	cmd := &cobra.Command{
		Use:   "cli",
		Short: "Open an interactive client session",
		Run: func(cmd *cobra.Command, args []string) {
			runCLI(host, debug, asAdmin)
		},
	}

	cmd.Flags().StringVar(&host, "host", "localhost:6379", "Server address")
	cmd.Flags().BoolVar(&debug, "debug", false, "Enable debug logging")
	cmd.Flags().BoolVar(&asAdmin, "admin", false, "Connect using the admin certificate")

	return cmd
}

func runCLI(host string, debug bool, asAdmin bool) {
	var logger *slog.Logger
	if debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	certRole := "client"
	if asAdmin {
		certRole = "admin"
	}

	caPath := filepath.Join(homeDir, "certs", "ca.crt")
	certPath := filepath.Join(homeDir, "certs", certRole+".crt")
	keyPath := filepath.Join(homeDir, "certs", certRole+".key")

	var cl *client.Client
	var err error

	if _, statErr := os.Stat(caPath); statErr == nil {
		fmt.Printf("Connecting to %s via mTLS as %s (Home: %s)...\n", host, strings.ToUpper(certRole), homeDir)
		cl, err = client.NewMTLSClientHelper(host, caPath, certPath, keyPath, logger)
	} else {
		fmt.Printf("Certificates not found at %s/certs. Connecting to %s via insecure TCP...\n", homeDir, host)
		cl, err = client.NewClient(client.Config{
			Address:        host,
			ConnectTimeout: 5 * time.Second,
			Logger:         logger,
		})
	}

	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	if err := cl.Ping(); err != nil {
		fmt.Printf("Failed to ping server: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Connected.")
	fmt.Println("Commands: select <db>, replicaof <host:port> <remote_db>, promote [min_replicas], stepdown, flushdb, get <k>, set <k> <v>, del <k>, mget <k>..., mset <k> <v>..., mdel <k>..., begin [read], commit, abort, stat, clear, quit")

	currentDB := "0"
	fmt.Printf("%s> ", currentDB)

	scanner := bufio.NewScanner(os.Stdin)
	hasError := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Printf("%s> ", currentDB)
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		if cmd == "clear" || cmd == "cls" {
			fmt.Print("\033[H\033[2J")
			fmt.Printf("%s> ", currentDB)
			continue
		}
		if cmd == "quit" || cmd == "exit" {
			if hasError {
				os.Exit(1)
			}
			return
		}

		if err := handleCommand(cl, cmd, parts); err != nil {
			if !errors.Is(err, client.ErrNotFound) {
				hasError = true
			}
		} else if cmd == "select" && len(parts) >= 2 {
			currentDB = parts[1]
		}
		fmt.Printf("%s> ", currentDB)
	}

	if hasError {
		os.Exit(1)
	}
}

func handleCommand(cl *client.Client, cmd string, parts []string) error {
	var err error
	var result []byte

	switch cmd {
	case "ping":
		err = cl.Ping()
		if err == nil {
			fmt.Println("PONG")
		}

	case "select":
		if len(parts) < 2 {
			fmt.Println("Usage: select <db>")
			return errors.New("usage error")
		}
		err = cl.Select(parts[1])
		if err == nil {
			fmt.Println("OK")
		}

	case "replicaof":
		if len(parts) < 3 {
			fmt.Println("Usage: replicaof <host:port> <remote_db>  (use stepdown to stop following)")
			return errors.New("usage error")
		}
		err = cl.ReplicaOf(parts[1], parts[2])
		if err == nil {
			fmt.Printf("Replication started from %s/%s\n", parts[1], parts[2])
		}

	case "promote":
		minReplicas := 0
		if len(parts) > 1 {
			var val int
			if _, errScan := fmt.Sscanf(parts[1], "%d", &val); errScan == nil {
				minReplicas = val
			}
		}
		err = cl.Promote(minReplicas)
		if err == nil {
			fmt.Println("OK")
		}

	case "stepdown":
		err = cl.StepDown()
		if err == nil {
			fmt.Println("OK")
		}

	case "flushdb":
		err = cl.FlushDB()
		if err == nil {
			fmt.Println("OK")
		}

	case "begin":
		if len(parts) > 1 && strings.EqualFold(parts[1], "read") {
			err = cl.BeginReadOnly()
		} else {
			err = cl.Begin()
		}
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

	case "stat":
		result, err = cl.Stat()
		if err == nil {
			var obj interface{}
			if json.Unmarshal(result, &obj) == nil {
				pretty, _ := json.MarshalIndent(obj, "", "  ")
				fmt.Println(string(pretty))
			} else {
				fmt.Println(string(result))
			}
		}

	case "get":
		if len(parts) < 2 {
			fmt.Println("Usage: get <key>")
			return errors.New("usage error")
		}
		result, err = cl.Get(parts[1])
		if err == nil {
			fmt.Printf("OK: %s\n", string(result))
		}

	case "del":
		if len(parts) < 2 {
			fmt.Println("Usage: del <key>")
			return errors.New("usage error")
		}
		err = cl.Del(parts[1])
		if err == nil {
			fmt.Println("OK")
		}

	case "set":
		if len(parts) < 3 {
			fmt.Println("Usage: set <key> <value>")
			return errors.New("usage error")
		}
		err = cl.Set(parts[1], []byte(parts[2]))
		if err == nil {
			fmt.Println("OK")
		}

	case "mget":
		var args []string
		if len(parts) > 1 {
			args = append(args, parts[1])
		}
		if len(parts) > 2 {
			args = append(args, strings.Fields(parts[2])...)
		}
		if len(args) == 0 {
			fmt.Println("Usage: mget <key1> [<key2> ...]")
			return errors.New("usage error")
		}

		vals, err2 := cl.MGet(args...)
		err = err2
		if err == nil {
			for i, val := range vals {
				if val == nil {
					fmt.Printf("%d) (nil)\n", i+1)
				} else {
					fmt.Printf("%d) %s\n", i+1, string(val))
				}
			}
		}

	case "mset":
		var allArgs []string
		if len(parts) > 1 {
			allArgs = append(allArgs, parts[1])
		}
		if len(parts) > 2 {
			allArgs = append(allArgs, strings.Fields(parts[2])...)
		}

		if len(allArgs) < 2 || len(allArgs)%2 != 0 {
			fmt.Println("Usage: mset <key1> <val1> [<key2> <val2> ...]")
			return errors.New("usage error")
		}

		data := make(map[string][]byte)
		for i := 0; i < len(allArgs); i += 2 {
			data[allArgs[i]] = []byte(allArgs[i+1])
		}

		err = cl.MSet(data)
		if err == nil {
			fmt.Println("OK")
		}

	case "mdel":
		var args []string
		if len(parts) > 1 {
			args = append(args, parts[1])
		}
		if len(parts) > 2 {
			args = append(args, strings.Fields(parts[2])...)
		}
		if len(args) == 0 {
			fmt.Println("Usage: mdel <key1> [<key2> ...]")
			return errors.New("usage error")
		}

		n, err2 := cl.MDel(args...)
		err = err2
		if err == nil {
			fmt.Printf("(integer) %d\n", n)
		}

	default:
		fmt.Println("Unknown command")
		return errors.New("unknown command")
	}

	if err != nil {
		printCLIError(err)
	}
	return err
}

func printCLIError(err error) {
	switch {
	case errors.Is(err, client.ErrConnection):
		fmt.Println("ERR: Connection closed by server")
		os.Exit(1)
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
