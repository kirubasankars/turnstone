// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use turnstone_config::{generate_config_artifacts, Config};
use turnstone_database::{open, OpenOptions};
use turnstone_protocol::{encode_frame, OP_REPL_LOG_RANGE, RES_OK};
use turnstone_repl::Manager;

fn setup_env() -> (
    PathBuf,
    HashMap<String, Arc<turnstone_database::Database>>,
    Arc<rustls::ClientConfig>,
) {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.json");
    generate_config_artifacts(
        dir.path(),
        Config {
            tls_cert_file: "certs/server.crt".into(),
            tls_key_file: "certs/server.key".into(),
            tls_ca_file: "certs/ca.crt".into(),
            number_of_databases: 1,
            ..Default::default()
        },
        &config_path,
        &[],
    )
    .unwrap();
    let mut stores = HashMap::new();
    stores.insert(
        "0".into(),
        open(dir.path().join("data/0"), OpenOptions::default()).unwrap(),
    );
    let tls = turnstone_tls::load_mtls(
        dir.path().join("certs/ca.crt"),
        dir.path().join("certs/server.crt"),
        dir.path().join("certs/server.key"),
    )
    .unwrap();
    (dir.keep(), stores, tls)
}

fn load_server_config(dir: &PathBuf) -> Arc<ServerConfig> {
    let cert_pem = std::fs::read(dir.join("certs/server.crt")).unwrap();
    let key_pem = std::fs::read(dir.join("certs/server.key")).unwrap();
    let mut cert_reader = std::io::BufReader::new(cert_pem.as_slice());
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(Result::ok)
        .collect();
    let mut key_reader = std::io::BufReader::new(key_pem.as_slice());
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .filter_map(Result::ok)
        .next()
        .map(PrivateKeyDer::Pkcs8)
        .unwrap();
    Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    )
}

fn write_repl_frame(stream: &mut impl Write, op: u8, body: &[u8]) {
    let crc = crc32fast::hash(body);
    let mut payload = Vec::with_capacity(4 + body.len());
    payload.extend_from_slice(&crc.to_be_bytes());
    payload.extend_from_slice(body);
    stream.write_all(&encode_frame(op, &payload)).unwrap();
}

fn write_status_ok(stream: &mut impl Write) {
    stream.write_all(&encode_frame(RES_OK, &[])).unwrap();
}

fn start_fake_leader(
    server_cfg: Arc<ServerConfig>,
) -> (
    String,
    std::sync::mpsc::Sender<Box<dyn FnOnce(rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>) + Send>>,
    impl FnOnce(),
) {
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp.local_addr().unwrap().to_string();
    let (task_tx, task_rx) = std::sync::mpsc::channel::<Box<dyn FnOnce(rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>) + Send>>();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    std::thread::spawn(move || {
        for stream in tcp.incoming().flatten() {
            if stop2.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let Ok(handler) = task_rx.recv() else {
                break;
            };
            let cfg = server_cfg.clone();
            std::thread::spawn(move || {
                let conn = rustls::ServerConnection::new(cfg).unwrap();
                let tls = rustls::StreamOwned::new(conn, stream);
                handler(tls);
            });
        }
    });
    (
        addr,
        task_tx,
        move || {
            stop.store(true, std::sync::atomic::Ordering::Release);
        },
    )
}

fn wait_for_key(db: &turnstone_database::Database, key: &str, want: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while deadline > std::time::Instant::now() {
        if let Ok(val) = db.get(key) {
            if String::from_utf8_lossy(&val) == want {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("key {key} not replicated, want {want}");
}

#[test]
fn manager_apply_log_range_offset_mismatch() {
    let (dir, stores, tls) = setup_env();
    let server_cfg = load_server_config(&dir);
    let (addr, task_tx, stop) = start_fake_leader(server_cfg);
    let handler = || {
        Box::new(|mut conn: rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>| {
            let mut head = [0u8; 5];
            conn.read_exact(&mut head).unwrap();
            write_status_ok(&mut conn);
            let mut body = Vec::new();
            body.extend_from_slice(&1u32.to_be_bytes());
            body.push(b'0');
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&999u64.to_be_bytes());
            body.extend_from_slice(&999u64.to_be_bytes());
            write_repl_frame(&mut conn, OP_REPL_LOG_RANGE, &body);
            std::thread::sleep(Duration::from_secs(2));
        })
            as Box<dyn FnOnce(rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>) + Send>
    };
    task_tx.send(handler()).unwrap();
    task_tx.send(handler()).unwrap();

    let rm = Manager::new("follower", stores.clone(), tls);
    rm.follow("0", &addr, "0").unwrap();
    std::thread::sleep(Duration::from_millis(800));
    assert!(stores["0"].get("repl-key").is_err());
    assert_eq!(stores["0"].last_log_offset(), 0);
    stop();
}

#[test]
fn manager_apply_log_range_valid_segment() {
    let leader_dir = tempfile::tempdir().unwrap();
    let leader = open(leader_dir.path().join("data"), OpenOptions::default()).unwrap();
    {
        let mut tx = leader.new_transaction(true);
        tx.put(b"repl-key", b"repl-val").unwrap();
        tx.commit().unwrap();
    }
    let (seg, end_off) = leader.read_log_range(0, 1 << 20).unwrap();
    let end_off = end_off as u64;
    leader.close().unwrap();

    let (dir, stores, tls) = setup_env();
    let server_cfg = load_server_config(&dir);
    let (addr, task_tx, stop) = start_fake_leader(server_cfg);
    let seg2 = seg.clone();
    let handler = move || {
        let seg = seg2.clone();
        Box::new(move |mut conn: rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>| {
            let mut head = [0u8; 5];
            conn.read_exact(&mut head).unwrap();
            write_status_ok(&mut conn);
            let mut body = Vec::new();
            body.extend_from_slice(&1u32.to_be_bytes());
            body.push(b'0');
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&0u64.to_be_bytes());
            body.extend_from_slice(&end_off.to_be_bytes());
            body.extend_from_slice(&seg);
            write_repl_frame(&mut conn, OP_REPL_LOG_RANGE, &body);
            std::thread::sleep(Duration::from_secs(2));
        })
            as Box<dyn FnOnce(rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>) + Send>
    };
    task_tx.send(handler()).unwrap();
    task_tx.send(handler()).unwrap();

    let rm = Manager::new("follower", stores.clone(), tls);
    rm.follow("0", &addr, "0").unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        stores["0"].last_log_offset() > 0,
        "follower log offset should advance, got {}",
        stores["0"].last_log_offset()
    );
    wait_for_key(&stores["0"], "repl-key", "repl-val");
    stop();
}
