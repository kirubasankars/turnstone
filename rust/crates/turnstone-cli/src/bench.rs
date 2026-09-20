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
use turnstone_client::Client;
use turnstone_tls::{cert_paths, Role};

use crate::bench_io::BenchConn;
use turnstone_protocol::{
    append_header, BEGIN_READ_ONLY, OP_BEGIN, OP_COMMIT, OP_GET, OP_SET,
};

use crate::connect::{connect, ConnectOptions};

const SOAK_KEY_SPACE: usize = 10_000;

#[derive(Debug, Clone)]
pub struct BenchOptions {
    pub home: std::path::PathBuf,
    pub addr: String,
    pub db: i32,
    pub concurrency: usize,
    pub pipeline_depth: usize,
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
    if opts.concurrency == 0 || opts.batch == 0 || opts.pipeline_depth == 0 {
        return Err("concurrency, depth, and batch must be > 0".into());
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

    let (ca, _, _) = cert_paths(&opts.home, Role::Client);
    let transport = if ca.exists() { "mTLS" } else { "plain TCP" };

    println!("--- TurnstoneDB Benchmark (Rust client) ---");
    println!("Server:       {}", opts.addr);
    println!("Home:         {}", opts.home.display());
    println!("Transport:    {transport}");
    println!("Database:     {}", opts.db);
    println!("Concurrency:  {} clients", opts.concurrency);
    if soak {
        println!("Duration:     {:?}", opts.duration);
    } else {
        println!("Total Ops:    {}", opts.ops);
    }
    println!("Pipeline:     {} tx/batch (inflight)", opts.pipeline_depth);
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
        run_phase(&opts, &payload, PhaseKind::Mixed(ratio), "MIXED")?;
    } else {
        run_phase(&opts, &payload, PhaseKind::Write, "WRITE")?;
        run_phase(&opts, &payload, PhaseKind::Read, "READ ")?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum PhaseKind {
    Write,
    Read,
    Mixed(f64),
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
    phase: PhaseKind,
    label: &str,
) -> Result<(), String> {
    println!("Starting {label} phase...");
    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let ops_total = opts.ops;
    let concurrency = opts.concurrency;
    let depth = opts.pipeline_depth;
    let batch = opts.batch;

    let mut handles = Vec::new();
    for worker in 0..concurrency {
        let ops = ops_total / concurrency as u64
            + u64::from((worker as u64) < (ops_total % concurrency as u64));
        let completed_c = Arc::clone(&completed);
        let failed_c = Arc::clone(&failed);
        let conn_opts = connect_opts(opts);
        let prefix = opts.prefix.clone();
        let key_size = opts.key_size;
        let db = opts.db;
        let payload = payload.to_vec();
        handles.push(thread::spawn(move || {
            if ops == 0 {
                return;
            }
            let mut conn = match BenchConn::connect(&conn_opts) {
                Ok(c) => c,
                Err(_) => {
                    failed_c.fetch_add(ops, Ordering::Relaxed);
                    return;
                }
            };
            if conn.select_db(db).is_err() {
                failed_c.fetch_add(ops, Ordering::Relaxed);
                return;
            }

            let txs_per_client = (ops / batch as u64).max(if ops > 0 { 1 } else { 0 });
            let batches_of_pipeline =
                (txs_per_client / depth as u64).max(if txs_per_client > 0 { 1 } else { 0 });

            let est_op_size = 20 + payload.len() + key_size;
            let mut write_buf = Vec::with_capacity(depth * (40 + batch * est_op_size));
            let mut rng = rand::thread_rng();
            let mut tx_count = 0u64;

            for _ in 0..batches_of_pipeline {
                write_buf.clear();
                for _ in 0..depth {
                    append_workload_transaction(
                        &mut write_buf,
                        phase,
                        worker,
                        ops,
                        tx_count,
                        batch,
                        key_size,
                        &prefix,
                        &payload,
                        &mut rng,
                    );
                    tx_count += 1;
                }
                let ops_in_batch = (depth * batch) as u64;
                match conn.pipeline(&write_buf, depth * (2 + batch)) {
                    Ok(()) => {
                        completed_c.fetch_add(ops_in_batch, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed_c.fetch_add(ops_in_batch, Ordering::Relaxed);
                        return;
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
    let depth = opts.pipeline_depth;
    let batch = opts.batch;

    let mut handles = Vec::new();
    for worker in 0..opts.concurrency {
        let completed_c = Arc::clone(&completed);
        let failed_c = Arc::clone(&failed);
        let stop_c = Arc::clone(&stop);
        let conn_opts = connect_opts(opts);
        let prefix = opts.prefix.clone();
        let key_size = opts.key_size;
        let db = opts.db;
        let payload = payload.to_vec();
        handles.push(thread::spawn(move || {
            let mut conn = match BenchConn::connect(&conn_opts) {
                Ok(c) => c,
                Err(_) => return,
            };
            if conn.select_db(db).is_err() {
                return;
            }
            let est_op_size = 20 + payload.len() + key_size;
            let mut write_buf = Vec::with_capacity(depth * (40 + batch * est_op_size));
            let mut rng = rand::thread_rng();
            let mut write_seq = 0usize;

            while !stop_c.load(Ordering::Relaxed) && Instant::now() < deadline {
                write_buf.clear();
                for _ in 0..depth {
                    append_soak_transaction(
                        &mut write_buf,
                        read_pct,
                        worker,
                        batch,
                        key_size,
                        &prefix,
                        &payload,
                        &mut write_seq,
                        &mut rng,
                    );
                }
                let ops_in_batch = (depth * batch) as u64;
                if conn.pipeline(&write_buf, depth * (2 + batch)).is_ok() {
                    completed_c.fetch_add(ops_in_batch, Ordering::Relaxed);
                } else {
                    failed_c.fetch_add(ops_in_batch, Ordering::Relaxed);
                    return;
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
    print_summary(
        "SOAK",
        completed.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        start.elapsed(),
    );
    Ok(())
}

fn append_workload_transaction(
    buf: &mut Vec<u8>,
    phase: PhaseKind,
    worker: usize,
    num_ops: u64,
    tx_count: u64,
    batch: usize,
    key_size: usize,
    prefix: &str,
    payload: &[u8],
    rng: &mut impl Rng,
) {
    let mut op_is_read = vec![false; batch];
    let mut all_read = true;
    match phase {
        PhaseKind::Write => all_read = false,
        PhaseKind::Read => {
            for slot in &mut op_is_read {
                *slot = true;
            }
        }
        PhaseKind::Mixed(read_pct) => {
            all_read = true;
            for slot in op_is_read.iter_mut() {
                *slot = rng.gen::<f64>() < read_pct;
                if !*slot {
                    all_read = false;
                }
            }
        }
    }

    if all_read {
        append_header(buf, OP_BEGIN, 1);
        buf.push(BEGIN_READ_ONLY);
    } else {
        append_header(buf, OP_BEGIN, 0);
    }

    for k in 0..batch {
        let key_index = match phase {
            PhaseKind::Mixed(_) => rng.gen_range(0..num_ops as usize),
            _ => tx_count as usize * batch + k,
        };
        append_op(buf, op_is_read[k], worker, key_index, key_size, prefix, payload);
    }
    append_header(buf, OP_COMMIT, 0);
}

fn append_soak_transaction(
    buf: &mut Vec<u8>,
    read_pct: f64,
    worker: usize,
    batch: usize,
    key_size: usize,
    prefix: &str,
    payload: &[u8],
    write_seq: &mut usize,
    rng: &mut impl Rng,
) {
    let mut op_is_read = vec![false; batch];
    let mut key_index = vec![0usize; batch];
    let mut all_read = true;

    for k in 0..batch {
        let mut is_read = rng.gen::<f64>() < read_pct;
        if is_read && *write_seq == 0 {
            is_read = false;
        }
        op_is_read[k] = is_read;
        if is_read {
            let mut span = *write_seq;
            if span > SOAK_KEY_SPACE {
                span = SOAK_KEY_SPACE;
            }
            key_index[k] = if span > 0 { rng.gen_range(0..span) } else { 0 };
        } else {
            key_index[k] = *write_seq % SOAK_KEY_SPACE;
            *write_seq += 1;
            all_read = false;
        }
    }

    if all_read {
        append_header(buf, OP_BEGIN, 1);
        buf.push(BEGIN_READ_ONLY);
    } else {
        append_header(buf, OP_BEGIN, 0);
    }

    for k in 0..batch {
        append_op(
            buf,
            op_is_read[k],
            worker,
            key_index[k],
            key_size,
            prefix,
            payload,
        );
    }
    append_header(buf, OP_COMMIT, 0);
}

fn append_op(
    buf: &mut Vec<u8>,
    is_read: bool,
    worker: usize,
    key_index: usize,
    key_size: usize,
    prefix: &str,
    payload: &[u8],
) {
    let key = make_key(prefix, key_size, worker, key_index);
    let key_bytes = key.as_bytes();
    if is_read {
        append_header(buf, OP_GET, key_bytes.len());
        buf.extend_from_slice(key_bytes);
    } else {
        let total_len = 4 + key_bytes.len() + payload.len();
        append_header(buf, OP_SET, total_len);
        buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(payload);
    }
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
