// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use turnstone_client::{Client, ClientConfig};
use turnstone_config::{generate_config_artifacts, Config};
use turnstone_engine::{Db, Options};
use turnstone_tls::{cert_paths, load_mtls, Role};

#[derive(Parser)]
#[command(name = "turnstone", about = "TurnstoneDB — Rust CLI")]
struct Cli {
    #[arg(long, global = true, default_value = "tsdata")]
    home: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize home directory (certs + turnstone.json)
    Init {
        #[arg(long)]
        ip: Option<String>,
    },
    /// Run embedded engine smoke test (wire server not yet ported)
    Server,
    /// Wire client: OP_PING
    Cli {
        #[command(subcommand)]
        cmd: CliCmd,
    },
}

#[derive(Subcommand)]
enum CliCmd {
    Ping {
        #[arg(long, default_value = "127.0.0.1:6379")]
        host: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::from(1)
        }
    }
}

fn run(cli: &Cli) -> Result<(), String> {
    match &cli.command {
        Commands::Init { ip } => {
            let extra: Vec<&str> = ip
                .as_deref()
                .map(|s| s.split(',').collect())
                .unwrap_or_default();
            let cfg = Config {
                port: ":6379".into(),
                number_of_databases: 4,
                tls_cert_file: "certs/server.crt".into(),
                tls_key_file: "certs/server.key".into(),
                tls_ca_file: "certs/ca.crt".into(),
                ..Default::default()
            };
            let path = cli.home.join("turnstone.json");
            generate_config_artifacts(&cli.home, cfg, &path, &extra).map_err(|e| e.to_string())?;
            Ok(())
        }
        Commands::Server => {
            let db_dir = cli.home.join("data").join("0");
            std::fs::create_dir_all(&db_dir).map_err(|e| e.to_string())?;
            let db = Db::open(&db_dir, Options::default()).map_err(|e| e.to_string())?;
            let mut tx = db.new_transaction(true);
            tx.put(b"turnstone:ready", b"1").map_err(|e| e.to_string())?;
            tx.commit().map_err(|e| e.to_string())?;
            db.close().map_err(|e| e.to_string())?;
            eprintln!(
                "engine OK at {}; full wire server is still Go — use `turnstone server` from the Go binary for production",
                db_dir.display()
            );
            Ok(())
        }
        Commands::Cli { cmd } => match cmd {
            CliCmd::Ping { host } => {
                let mut cfg = ClientConfig {
                    address: host.clone(),
                    ..ClientConfig::default()
                };
                let (ca, cert, key) = cert_paths(&cli.home, Role::Client);
                if ca.exists() {
                    cfg.tls = Some(load_mtls(ca, cert, key).map_err(|e| e.to_string())?);
                }
                Client::connect(cfg)
                    .map_err(|e| e.to_string())?
                    .ping()
                    .map_err(|e| e.to_string())?;
                println!("PONG");
                Ok(())
            }
        },
    }
}
