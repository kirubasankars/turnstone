// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Standalone wire client (`turnstone-rs`), mirroring `turnstone cli` when --home is set.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use turnstone_cli::{run_exec, run_interactive, ConnectOptions};

#[derive(Parser)]
#[command(
    name = "turnstone-rs",
    version,
    about = "TurnstoneDB Rust wire client"
)]
struct Cli {
    #[arg(long, global = true, default_value = "127.0.0.1:6379")]
    host: String,

    #[arg(long, global = true, default_value = "tsdata")]
    home: PathBuf,

    #[arg(long, global = true)]
    admin: bool,

    #[arg(long, global = true)]
    debug: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Send OP_PING
    Ping,
    /// begin / set / commit / get smoke test
    Smoke { key: String, value: String },
    /// Run one REPL command
    Exec { command: String },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let opts = ConnectOptions {
        host: cli.host,
        home: cli.home,
        admin: cli.admin,
        debug: cli.debug,
    };

    match cli.command {
        None => {
            if let Err(e) = run_interactive(opts) {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        }
        Some(Commands::Ping) => {
            let client = match turnstone_cli::connect(&opts) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::from(1);
                }
            };
            if let Err(e) = client.ping() {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
            println!("PONG");
        }
        Some(Commands::Smoke { key, value }) => {
            if let Err(e) = smoke(&opts, &key, &value) {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        }
        Some(Commands::Exec { command }) => {
            if let Err(e) = run_exec(opts, &command) {
                eprintln!("{e}");
                return ExitCode::from(1);
            }
        }
    }
    ExitCode::SUCCESS
}

fn smoke(opts: &ConnectOptions, key: &str, value: &str) -> Result<(), String> {
    let client = turnstone_cli::connect(opts).map_err(|e| e.to_string())?;
    client.begin().map_err(|e| e.to_string())?;
    client
        .set(key, value.as_bytes())
        .map_err(|e| e.to_string())?;
    client.commit().map_err(|e| e.to_string())?;
    client.begin_read_only().map_err(|e| e.to_string())?;
    let got = client.get(key).map_err(|e| e.to_string())?;
    client.commit().map_err(|e| e.to_string())?;
    if got != value.as_bytes() {
        return Err(format!(
            "expected {value:?}, got {:?}",
            String::from_utf8_lossy(&got)
        ));
    }
    println!("OK: {}", String::from_utf8_lossy(&got));
    Ok(())
}
