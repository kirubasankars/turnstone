// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::io::{self, Write};

use serde_json::Value;
use turnstone_client::{Client, ClientError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutcome {
    Ok,
    UsageError,
    NotFound,
    Failed,
    Unknown,
}

pub fn parse_command_line(line: &str) -> (String, Vec<String>) {
    let line = line.trim();
    if line.is_empty() {
        return (String::new(), Vec::new());
    }
    let mut parts = line.splitn(3, ' ');
    let cmd = parts.next().unwrap_or("").to_ascii_lowercase();
    let rest: Vec<String> = parts.map(str::to_string).collect();
    (cmd, rest)
}

pub fn execute_line(client: &Client, line: &str) -> CommandOutcome {
    let (cmd, parts) = parse_command_line(line);
    if cmd.is_empty() {
        return CommandOutcome::Ok;
    }
    handle_command(client, &cmd, &parts)
}

pub fn handle_command(client: &Client, cmd: &str, parts: &[String]) -> CommandOutcome {
    let result = match cmd {
        "ping" => client.ping().map(|_| {
            println!("PONG");
        }),
        "select" => {
            if parts.is_empty() {
                println!("Usage: select <db>");
                return CommandOutcome::UsageError;
            }
            client.select_db(&parts[0]).map(|_| println!("OK"))
        }
        "replicaof" => {
            if parts.len() < 2 {
                println!("Usage: replicaof <host:port> <remote_db>  (use stepdown to stop following)");
                return CommandOutcome::UsageError;
            }
            client
                .replica_of(&parts[0], &parts[1])
                .map(|_| println!("Replication started from {}/{}", parts[0], parts[1]))
        }
        "promote" => {
            let min = parts
                .first()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);
            client.promote(min).map(|_| println!("OK"))
        }
        "stepdown" => client.step_down().map(|_| println!("OK")),
        "flushdb" => client.flush_db().map(|_| println!("OK")),
        "begin" => {
            let r = if parts.len() > 1 && parts[1].eq_ignore_ascii_case("read") {
                client.begin_read_only()
            } else {
                client.begin()
            };
            r.map(|_| println!("OK"))
        }
        "commit" => client.commit().map(|_| println!("OK")),
        "abort" => client.abort().map(|_| println!("OK")),
        "stat" => client.stat().map(|raw| print_stat_json(&raw)),
        "get" => {
            if parts.is_empty() {
                println!("Usage: get <key>");
                return CommandOutcome::UsageError;
            }
            match client.get(&parts[0]) {
                Ok(v) => {
                    println!("OK: {}", String::from_utf8_lossy(&v));
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        "del" => {
            if parts.is_empty() {
                println!("Usage: del <key>");
                return CommandOutcome::UsageError;
            }
            client.del(&parts[0]).map(|_| println!("OK"))
        }
        "set" => {
            if parts.len() < 2 {
                println!("Usage: set <key> <value>");
                return CommandOutcome::UsageError;
            }
            let value = if parts.len() == 2 {
                parts[1].clone()
            } else {
                parts[1..].join(" ")
            };
            client
                .set(&parts[0], value.as_bytes())
                .map(|_| println!("OK"))
        }
        "mget" => {
            let keys = collect_trailing_args(parts);
            if keys.is_empty() {
                println!("Usage: mget <key1> [<key2> ...]");
                return CommandOutcome::UsageError;
            }
            let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            match client.mget(&refs) {
                Ok(vals) => {
                    for (i, val) in vals.into_iter().enumerate() {
                        match val {
                            None => println!("{}) (nil)", i + 1),
                            Some(v) => println!("{}) {}", i + 1, String::from_utf8_lossy(&v)),
                        }
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        "mset" => {
            let all = collect_trailing_args(parts);
            if all.len() < 2 || all.len() % 2 != 0 {
                println!("Usage: mset <key1> <val1> [<key2> <val2> ...]");
                return CommandOutcome::UsageError;
            }
            let mut pairs: Vec<(&str, &[u8])> = Vec::new();
            let mut owned: Vec<(String, Vec<u8>)> = Vec::new();
            for chunk in all.chunks(2) {
                owned.push((chunk[0].clone(), chunk[1].as_bytes().to_vec()));
            }
            for (k, v) in &owned {
                pairs.push((k.as_str(), v.as_slice()));
            }
            client.mset(&pairs).map(|_| println!("OK"))
        }
        "mdel" => {
            let keys = collect_trailing_args(parts);
            if keys.is_empty() {
                println!("Usage: mdel <key1> [<key2> ...]");
                return CommandOutcome::UsageError;
            }
            let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            match client.mdel(&refs) {
                Ok(n) => {
                    println!("(integer) {n}");
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        _ => {
            println!("Unknown command");
            return CommandOutcome::Unknown;
        }
    };

    match result {
        Ok(()) => CommandOutcome::Ok,
        Err(e) => {
            print_cli_error(&e);
            if matches!(e, ClientError::NotFound) {
                CommandOutcome::NotFound
            } else if matches!(e, ClientError::Connection(_)) {
                CommandOutcome::Failed
            } else {
                CommandOutcome::Failed
            }
        }
    }
}

fn collect_trailing_args(parts: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    if !parts.is_empty() {
        out.push(parts[0].clone());
    }
    if parts.len() > 1 {
        out.extend(parts[1].split_whitespace().map(str::to_string));
    }
    out
}

fn print_stat_json(raw: &[u8]) {
    if let Ok(v) = serde_json::from_slice::<Value>(raw) {
        if let Ok(pretty) = serde_json::to_string_pretty(&v) {
            println!("{pretty}");
            return;
        }
    }
    println!("{}", String::from_utf8_lossy(raw));
}

pub fn print_cli_error(err: &ClientError) {
    match err {
        ClientError::Connection(_) => {
            println!("ERR: Connection closed by server");
            io::stdout().flush().ok();
            std::process::exit(1);
        }
        ClientError::NotFound => println!("(nil)"),
        ClientError::TxRequired => println!("ERR: Transaction Required"),
        ClientError::TxTimeout => println!("ERR: Transaction Timeout"),
        ClientError::TxConflict => println!("ERR: Conflict Detected (Retry)"),
        ClientError::ServerBusy => println!("ERR: Server Busy"),
        ClientError::EntityTooLarge => println!("ERR: Entity Too Large"),
        ClientError::MemoryLimit => println!("ERR: Server Memory Limit Exceeded"),
        other => println!("ERR: {other}"),
    }
}

pub fn is_command_failure(outcome: CommandOutcome) -> bool {
    !matches!(outcome, CommandOutcome::Ok | CommandOutcome::NotFound)
}

pub fn welcome_message() {
    println!("Connected.");
    println!(
        "Commands: select <db>, replicaof <host:port> <remote_db>, promote [min_replicas], stepdown, flushdb, get <k>, set <k> <v>, del <k>, mget <k>..., mset <k> <v>..., mdel <k>..., begin [read], commit, abort, stat, ping, clear, quit"
    );
}

#[allow(dead_code)]
pub fn parse_mset_map(parts: &[String]) -> Option<HashMap<String, Vec<u8>>> {
    let all = collect_trailing_args(parts);
    if all.len() < 2 || all.len() % 2 != 0 {
        return None;
    }
    let mut m = HashMap::new();
    for chunk in all.chunks(2) {
        m.insert(chunk[0].clone(), chunk[1].as_bytes().to_vec());
    }
    Some(m)
}
