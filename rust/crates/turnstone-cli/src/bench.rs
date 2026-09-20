// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;
use serde_json::Value;
use turnstone_client::{Client, ClientError};

use crate::connect::{connect, ConnectOptions};

#[derive(Debug, Clone)]
pub struct BenchOptions {
    pub home: std::path::PathBuf,
    pub addr: String,
    pub db: i32,
    pub concurrency: usize,
    pub ops: u64,
    pub duration: Duration,
    pub report: Duration,
    pub value_size: usize,
    pub key_size: usize,
    pub read_ratio: Option<f64>,
    pub batch: usize,
    pub prefix: String,
}

pub fn run_bench(opts: BenchOptions) -> Result<(), String> {
    if opts.concurrency == 0 || opts.batch == 0 {
        return Err("concurrency and batch must be > 0".into());
    }
    if opts.duration.is_zero() && opts.ops == 0 {
        return Err("set --ops > 0 or --duration".into());
    }
    if let Some(r) = opts.read_ratio {
        if !(0.0..=1.0).contains(&r) {
            return Err("read-ratio must be between 0.0 and 1.0".into());
        }
    }

    bench_preflight(&opts)?;

    let payload: Vec<u8> = (0..opts.value_size).map(|i| (i % 251) as u8).collect();
    let soak = !opts.duration.is_zero();

    println!("--- TurnstoneDB Benchmark (Rust client) ---");
    println!("Server:       {}", opts.addr);
    println!("Home:         {}", opts.home.display());
    println!("Database:     {}", opts.db);
    println!("Concurrency:  {} clients", opts.concurrency);
    if soak {
        println!("Duration:     {:?}", opts.duration);
    } else {
        println!("Total Ops:    {}", opts.ops);
    }
    println!("Batch Size:   {} ops/tx", opts.batch);
    println!("Payload:      {} bytes", opts.value_size);
    println!("Key Prefix:   {}", opts.prefix);
    if let Some(r) = opts.read_ratio {
        println!(
            "Mode:         Mixed ({:.0}% read / {:.0}% write)",
            r * 100.0,
            (1.0 - r) * 100.0
        );
    } else {
        println!("Mode:         Sequential (write phase then read phase)");
    }
    println!("--------------------------------------------------");

    if soak {
        run_duration(&opts, &payload)?;
    } else if let Some(ratio) = opts.read_ratio {
        run_phase(&opts, &payload, ratio, "MIXED")?;
    } else {
        run_phase(&opts, &payload, 0.0, "WRITE")?;
        run_phase(&opts, &payload, 1.0, "READ ")?;
    }
    Ok(())
}

fn bench_preflight(opts: &BenchOptions) -> Result<(), String> {
    let client = connect(&connect_opts(opts)).map_err(|e| e.to_string())?;
    client
        .select_db(&opts.db.to_string())
        .map_err(|e| e.to_string())?;
    let raw = client.stat().map_err(|e| e.to_string())?;
    let v: Value = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
    let state = v
        .get("state")
        .and_then(|s| s.as_str())
        .unwrap_or("UNKNOWN");
    if state != "PRIMARY" {
        return Err(format!(
            "database {} is {state}; writes require PRIMARY (use `turnstone cli --admin exec promote` or server --dev)",
            opts.db
        ));
    }
    Ok(())
}

fn connect_opts(opts: &BenchOptions) -> ConnectOptions {
    ConnectOptions {
        host: opts.addr.clone(),
        home: opts.home.clone(),
        admin: false,
        debug: false,
    }
}

fn run_phase(
    opts: &BenchOptions,
    payload: &[u8],
    read_pct: f64,
    label: &str,
) -> Result<(), String> {
    println!("Starting {label} phase...");
    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let ops_total = opts.ops;
    let concurrency = opts.concurrency;

    let mut handles = Vec::new();
    for worker in 0..concurrency {
        let ops = ops_total / concurrency as u64
            + u64::from((worker as u64) < (ops_total % concurrency as u64));
        let completed_c = Arc::clone(&completed);
        let failed_c = Arc::clone(&failed);
        let conn_opts = connect_opts(opts);
        let prefix = opts.prefix.clone();
        let batch = opts.batch;
        let key_size = opts.key_size;
        let db = opts.db;
        let payload = payload.to_vec();
        handles.push(thread::spawn(move || {
            if ops == 0 {
                return;
            }
            let client = match connect(&conn_opts) {
                Ok(c) => c,
                Err(_) => {
                    failed_c.fetch_add(ops, Ordering::Relaxed);
                    return;
                }
            };
            if client.select_db(&db.to_string()).is_err() {
                failed_c.fetch_add(ops, Ordering::Relaxed);
                return;
            }
            let mut rng = rand::thread_rng();
            let mut done = 0u64;
            while done < ops {
                let n = batch.min((ops - done) as usize);
                let read_tx = rng.gen::<f64>() < read_pct;
                let res = if read_tx {
                    run_read_batch(&client, &prefix, key_size, worker, done as usize, n, &mut rng)
                } else {
                    run_write_batch(
                        &client,
                        &prefix,
                        key_size,
                        worker,
                        done as usize,
                        n,
                        &payload,
                    )
                };
                match res {
                    Ok(k) => {
                        completed_c.fetch_add(k as u64, Ordering::Relaxed);
                        done += k as u64;
                    }
                    Err(ClientError::NotFound) => {
                        completed_c.fetch_add(n as u64, Ordering::Relaxed);
                        done += n as u64;
                    }
                    Err(_) => {
                        failed_c.fetch_add(n as u64, Ordering::Relaxed);
                        done += n as u64;
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().map_err(|_| "worker panicked".to_string())?;
    }
    let elapsed = start.elapsed();
    let ok = completed.load(Ordering::Relaxed);
    let bad = failed.load(Ordering::Relaxed);
    print_summary(label, ok, bad, elapsed);
    Ok(())
}

fn run_duration(opts: &BenchOptions, payload: &[u8]) -> Result<(), String> {
    let read_pct = opts.read_ratio.unwrap_or(0.5);
    let report = if opts.report.is_zero() {
        Duration::from_secs(5)
    } else {
        opts.report
    };
    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let start = Instant::now();
    let deadline = start + opts.duration;

    let mut handles = Vec::new();
    for worker in 0..opts.concurrency {
        let completed_c = Arc::clone(&completed);
        let failed_c = Arc::clone(&failed);
        let stop_c = Arc::clone(&stop);
        let conn_opts = connect_opts(opts);
        let prefix = opts.prefix.clone();
        let batch = opts.batch;
        let key_size = opts.key_size;
        let db = opts.db;
        let payload = payload.to_vec();
        handles.push(thread::spawn(move || {
            let client = match connect(&conn_opts) {
                Ok(c) => c,
                Err(_) => return,
            };
            if client.select_db(&db.to_string()).is_err() {
                return;
            }
            let mut rng = rand::thread_rng();
            let mut seq = 0usize;
            while !stop_c.load(Ordering::Relaxed) {
                let read_tx = rng.gen::<f64>() < read_pct;
                let res = if read_tx {
                    run_read_batch(&client, &prefix, key_size, worker, seq, batch, &mut rng)
                } else {
                    run_write_batch(
                        &client,
                        &prefix,
                        key_size,
                        worker,
                        seq,
                        batch,
                        &payload,
                    )
                };
                seq += batch;
                match res {
                    Ok(k) => {
                        completed_c.fetch_add(k as u64, Ordering::Relaxed);
                    }
                    Err(ClientError::NotFound) => {
                        completed_c.fetch_add(batch as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed_c.fetch_add(batch as u64, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    while Instant::now() < deadline {
        thread::sleep(report);
        let ok = completed.load(Ordering::Relaxed);
        let bad = failed.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 { ok as f64 / elapsed } else { 0.0 };
        eprintln!(
            "[report] ops={ok} failed={bad} elapsed={elapsed:.1}s rate={rate:.0} ops/s"
        );
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().map_err(|_| "worker panicked".to_string())?;
    }
    print_summary("SOAK", completed.load(Ordering::Relaxed), failed.load(Ordering::Relaxed), start.elapsed());
    Ok(())
}

fn run_write_batch(
    client: &Client,
    prefix: &str,
    key_size: usize,
    worker: usize,
    base: usize,
    n: usize,
    payload: &[u8],
) -> Result<usize, ClientError> {
    client.begin()?;
    for i in 0..n {
        let key = make_key(prefix, key_size, worker, base + i);
        client.set(&key, payload)?;
    }
    client.commit()?;
    Ok(n)
}

fn run_read_batch(
    client: &Client,
    prefix: &str,
    key_size: usize,
    worker: usize,
    base: usize,
    n: usize,
    rng: &mut impl Rng,
) -> Result<usize, ClientError> {
    client.begin_read_only()?;
    for i in 0..n {
        let idx = if n > 1 {
            base + rng.gen_range(0..n)
        } else {
            base + i
        };
        let key = make_key(prefix, key_size, worker, idx);
        let _ = client.get(&key)?;
    }
    client.commit()?;
    Ok(n)
}

fn make_key(prefix: &str, key_size: usize, worker: usize, index: usize) -> String {
    let base = format!("{prefix}-{worker}-{index}");
    if base.len() >= key_size {
        base
    } else {
        format!("{}{}", base, "x".repeat(key_size - base.len()))
    }
}

fn print_summary(label: &str, ok: u64, failed: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "{label} done: {} ok, {} failed, {:.2}s, {:.0} ops/s",
        ok,
        failed,
        secs,
        ok as f64 / secs
    );
}
