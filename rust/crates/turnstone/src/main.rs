// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use turnstone_client::{Client, ClientConfig};
use turnstone_config::{
    database_count, generate_config_artifacts, resolve_path, share_bytes, validate_config, Config,
};
use turnstone_database::{open as open_database, OpenOptions, STATE_PRIMARY};
use turnstone_engine::Options as EngineOptions;
use turnstone_repl::Manager;
use turnstone_server::new_server;
use turnstone_tls::{cert_paths, load_mtls, Role};

#[derive(Parser)]
#[command(name = "turnstone", about = "TurnstoneDB — Rust implementation")]
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
    /// Run the mTLS wire server
    Server {
        #[arg(long)]
        dev: bool,
    },
    /// Wire client commands
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
        Commands::Server { dev } => run_server(&cli.home, *dev),
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

fn run_server(home: &PathBuf, dev: bool) -> Result<(), String> {
    if !home.is_dir() {
        return Err(format!(
            "home directory does not exist: {} (run `turnstone init` first)",
            home.display()
        ));
    }

    let config_path = home.join("turnstone.json");
    let bytes = std::fs::read(&config_path).map_err(|e| e.to_string())?;
    let cfg: Config = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    validate_config(&cfg).map_err(|e| e.to_string())?;

    let retention = if cfg.log_retention.is_empty() {
        "replication".to_string()
    } else {
        cfg.log_retention.clone()
    };

    let n_db = database_count(cfg.number_of_databases);
    let _per_cache = share_bytes(cfg.value_cache_bytes, n_db);

    let cert_file = resolve_path(home, &cfg.tls_cert_file);
    let key_file = resolve_path(home, &cfg.tls_key_file);
    let ca_file = resolve_path(home, &cfg.tls_ca_file);
    let client_cert = resolve_path(home, &cfg.tls_client_cert_file);
    let client_key = resolve_path(home, &cfg.tls_client_key_file);

    let mut stores: HashMap<String, Arc<turnstone_database::Database>> = HashMap::new();
    for i in 0..n_db {
        let name = i.to_string();
        let path = home.join("data").join(&name);
        eprintln!("Opening database {name}...");
        let db = open_database(
            &path,
            OpenOptions {
                retention_strategy: retention.clone(),
                max_disk_usage_percent: cfg.max_disk_usage_percent,
                max_index_arena_bytes: cfg.max_index_arena_bytes,
                engine: EngineOptions {
                    max_disk_usage_percent: cfg.max_disk_usage_percent,
                    ..EngineOptions::default()
                },
                ..OpenOptions::default()
            },
        )
        .map_err(|e| e.to_string())?;

        if dev {
            db.set_min_replicas(0);
            db.promote().map_err(|e| e.to_string())?;
            eprintln!("Dev mode: auto-promoted DB {name} to {STATE_PRIMARY}");
        }

        stores.insert(name, db);
    }

    let repl_tls = load_mtls(&ca_file, &client_cert, &client_key).map_err(|e| e.to_string())?;
    let repl_manager = Arc::new(Manager::new(cfg.id.clone(), stores.clone(), repl_tls));

    let max_conns = if cfg.max_conns > 0 {
        cfg.max_conns as usize
    } else {
        1000
    };

    let listen = cfg.port.clone();
    let server = new_server(
        cfg.id,
        listen.clone(),
        stores,
        max_conns,
        cert_file.display().to_string(),
        key_file.display().to_string(),
        ca_file.display().to_string(),
        Some(repl_manager),
        dev,
    )
    .map_err(|e| e.to_string())?;

    eprintln!(
        "TurnstoneDB (Rust) listening on {}",
        server
            .addr()
            .map(|a| a.to_string())
            .unwrap_or(listen)
    );
    server.run().map_err(|e| e.to_string())
}
