// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newCLICmd() *cobra.Command {
	var host string
	var debug bool
	var asAdmin bool

	cmd := &cobra.Command{
		Use:   "cli",
		Short: "Interactive client or one-shot command execution",
		Long: `Connect to a TurnstoneDB server with an interactive REPL, or run a single command.

Examples:
  turnstone cli
  turnstone cli exec get mykey
  turnstone cli --admin exec promote`,
		Run: func(cmd *cobra.Command, args []string) {
			runCLIInteractive(host, debug, asAdmin)
		},
	}

	cmd.PersistentFlags().StringVar(&host, "host", "localhost:6379", "Server address")
	cmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging")
	cmd.PersistentFlags().BoolVar(&asAdmin, "admin", false, "Connect using the admin certificate")

	cmd.AddCommand(&cobra.Command{
		Use:   "exec <command>",
		Short: "Run a single client command and exit",
		Long: `Execute one REPL command without entering interactive mode.

Examples:
  turnstone cli exec get mykey
  turnstone cli exec set mykey hello
  turnstone cli --admin exec promote
  turnstone cli exec "mget key1 key2 key3"`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			runCLIExec(host, debug, asAdmin, strings.Join(args, " "))
		},
	})

	return cmd
}

func runCLIInteractive(host string, debug bool, asAdmin bool) {
	cl, err := connectCLIClient(host, debug, asAdmin)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer cl.Close()

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

		cmdName, parts := parseCommandLine(line)

		if cmdName == "clear" || cmdName == "cls" {
			fmt.Print("\033[H\033[2J")
			fmt.Printf("%s> ", currentDB)
			continue
		}
		if cmdName == "quit" || cmdName == "exit" {
			if hasError {
				os.Exit(1)
			}
			return
		}

		if err := handleCommand(cl, cmdName, parts); err != nil {
			if isCLICommandFailure(err) {
				hasError = true
			}
		} else if cmdName == "select" && len(parts) >= 2 {
			currentDB = parts[1]
		}
		fmt.Printf("%s> ", currentDB)
	}

	if hasError {
		os.Exit(1)
	}
}

func runCLIExec(host string, debug bool, asAdmin bool, line string) {
	cl, err := connectCLIClient(host, debug, asAdmin)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer cl.Close()

	if err := executeCLICommand(cl, line); isCLICommandFailure(err) {
		os.Exit(1)
	}
}
