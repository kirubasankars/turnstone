// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use turnstone_client::{Client, ClientConfig};
use turnstone_tls::{cert_paths, load_mtls, Role};

#[derive(Parser)]
#[command(
    name = "turnstone-rs",
    about = "TurnstoneDB Rust client (wire-compatible with the Go server)"
)]
struct Cli {
    #[arg(long, global = true, default_value = "127.0.0.1:6379")]
    host: String,

    #[arg(long, global = true)]
    home: Option<PathBuf>,

    #[arg(long, global = true)]
    admin: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send OP_PING to the server
    Ping,
    /// Run begin / set / commit / get / commit against KEY and VALUE
    Smoke { key: String, value: String },
}

fn connect(cli: &Cli) -> Result<Client, String> {
    let mut cfg = ClientConfig {
        address: cli.host.clone(),
        ..ClientConfig::default()
    };
    if let Some(home) = &cli.home {
        let role = if cli.admin { Role::Admin } else { Role::Client };
        let (ca, cert, key) = cert_paths(home, role);
        if ca.exists() {
            cfg.tls = Some(load_mtls(ca, cert, key).map_err(|e| e.to_string())?);
        }
    }
    Client::connect(cfg).map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let client = match connect(&cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect failed: {e}");
            return ExitCode::from(1);
        }
    };

    let result: Result<(), String> = match cli.command {
        Commands::Ping => client
            .ping()
            .map(|_| println!("PONG"))
            .map_err(|e| e.to_string()),
        Commands::Smoke { key, value } => smoke(&client, &key, &value),
    };

    if let Err(e) = result {
        eprintln!("{e}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn smoke(client: &Client, key: &str, value: &str) -> Result<(), String> {
    client.begin().map_err(|e| e.to_string())?;
    client
        .set(key, value.as_bytes())
        .map_err(|e| e.to_string())?;
    client.commit().map_err(|e| e.to_string())?;
    client.begin_read_only().map_err(|e| e.to_string())?;
    let got = client.get(key).map_err(|e| e.to_string())?;
    client.commit().map_err(|e| e.to_string())?;
    let s = String::from_utf8_lossy(&got);
    if s != value {
        return Err(format!("expected {value:?}, got {s:?}"));
    }
    println!("OK: {s}");
    Ok(())
}
