// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{self, BufRead, Write};

use crate::commands::{
    execute_line, handle_command, is_command_failure, welcome_message, CommandOutcome,
};
use crate::connect::{connect, ConnectOptions};

pub fn run_interactive(opts: ConnectOptions) -> Result<(), String> {
    let client = connect(&opts).map_err(|e| e.to_string())?;
    welcome_message();
    let mut current_db = "0".to_string();
    print_prompt(&current_db);
    let stdin = io::stdin();
    let mut has_error = false;
    for line in stdin.lock().lines() {
        let line = line.map_err(|e| e.to_string())?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            print_prompt(&current_db);
            continue;
        }
        let (cmd, parts) = crate::commands::parse_command_line(trimmed);
        if cmd == "clear" || cmd == "cls" {
            print!("\x1b[H\x1b[2J");
            io::stdout().flush().ok();
            print_prompt(&current_db);
            continue;
        }
        if cmd == "quit" || cmd == "exit" {
            if has_error {
                std::process::exit(1);
            }
            return Ok(());
        }
        let outcome = if cmd == "select" && !parts.is_empty() {
            let o = handle_command(&client, &cmd, &parts);
            if o == CommandOutcome::Ok {
                current_db = parts[0].clone();
            }
            o
        } else {
            execute_line(&client, trimmed)
        };
        if is_command_failure(outcome) {
            has_error = true;
        }
        print_prompt(&current_db);
    }
    if has_error {
        std::process::exit(1);
    }
    Ok(())
}

pub fn run_exec(opts: ConnectOptions, line: &str) -> Result<(), String> {
    let client = connect(&opts).map_err(|e| e.to_string())?;
    let outcome = execute_line(&client, line);
    if is_command_failure(outcome) {
        Err("command failed".into())
    } else {
        Ok(())
    }
}

fn print_prompt(db: &str) {
    print!("{db}> ");
    io::stdout().flush().ok();
}
