// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Interactive CLI, bench, backup/restore helpers for TurnstoneDB.

mod backup_cmd;
mod bench;
mod bench_io;
mod commands;
mod connect;
mod repl;

pub use backup_cmd::{run_backup_cmd, run_restore_cmd, BackupCliOptions, RestoreCliOptions};
pub use bench::{run_bench, BenchOptions};
pub use commands::{
    execute_line, handle_command, parse_command_line, print_cli_error, CommandOutcome,
};
pub use connect::{connect, home_exists, ConnectOptions};
pub use repl::{run_exec, run_interactive};
