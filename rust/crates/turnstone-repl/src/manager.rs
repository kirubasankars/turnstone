// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rustls::pki_types::ServerName;
use turnstone_database::Database;
use turnstone_protocol::{
    decode_header, encode_frame, OP_REPL_ACK, OP_REPL_HELLO, OP_REPL_LOG_RANGE, OP_REPL_SAFE_POINT,
    RES_ERR,
};

#[derive(Debug, Clone)]
pub struct Source {
    pub local_db: String,
    pub remote_db: String,
}

pub struct Manager {
    server_id: String,
    peers: Arc<Mutex<HashMap<String, Vec<Source>>>>,
    cancel: Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<()>>>>,
    stores: HashMap<String, Arc<Database>>,
    tls: Arc<rustls::ClientConfig>,
}

impl Manager {
    pub fn new(
        server_id: impl Into<String>,
        stores: HashMap<String, Arc<Database>>,
        tls: Arc<rustls::ClientConfig>,
    ) -> Self {
        Self {
            server_id: server_id.into(),
            peers: Arc::new(Mutex::new(HashMap::new())),
            cancel: Arc::new(Mutex::new(HashMap::new())),
            stores,
            tls,
        }
    }

    pub fn start(&self) {
        let addrs: Vec<String> = self.peers.lock().keys().cloned().collect();
        for addr in addrs {
            self.spawn_connection(&addr);
        }
    }

    pub fn stop_all(&self) {
        let mut cancel = self.cancel.lock();
        for (_, tx) in cancel.drain() {
            let _ = tx.send(());
        }
        self.peers.lock().clear();
    }

    pub fn is_following(&self, db_name: &str) -> bool {
        self.peers
            .lock()
            .values()
            .any(|dbs| dbs.iter().any(|db| db.local_db == db_name))
    }

    pub fn source(&self, db_name: &str) -> (String, String) {
        for (addr, sources) in self.peers.lock().iter() {
            for src in sources {
                if src.local_db == db_name {
                    return (addr.clone(), src.remote_db.clone());
                }
            }
        }
        (String::new(), String::new())
    }

    pub fn follow(
        &self,
        db_name: &str,
        source_addr: &str,
        source_db: &str,
    ) -> Result<(), String> {
        {
            let peers = self.peers.lock();
            if let Some(dbs) = peers.get(source_addr) {
                if dbs.iter().any(|db| db.local_db == db_name) {
                    return Ok(());
                }
            }
        }

        self.remove_db_from_other_peers(db_name, source_addr);

        let mut candidate = self.peers.lock().get(source_addr).cloned().unwrap_or_default();
        candidate.push(Source {
            local_db: db_name.to_string(),
            remote_db: source_db.to_string(),
        });

        self.verify_handshake(source_addr, &candidate)?;

        self.remove_db_from_other_peers(db_name, source_addr);
        {
            let mut peers = self.peers.lock();
            let mut current = peers.get(source_addr).cloned().unwrap_or_default();
            if current.iter().any(|db| db.local_db == db_name) {
                return Ok(());
            }
            current.push(Source {
                local_db: db_name.to_string(),
                remote_db: source_db.to_string(),
            });
            peers.insert(source_addr.to_string(), current);
        }

        if let Some(tx) = self.cancel.lock().remove(source_addr) {
            let _ = tx.send(());
        }
        self.spawn_connection(source_addr);
        Ok(())
    }

    pub fn stop_following(&self, db_name: &str) {
        let mut addr_to_restart: Option<String> = None;
        let mut empty_addr: Option<String> = None;
        {
            let mut peers = self.peers.lock();
            for (addr, dbs) in peers.iter_mut() {
                let before = dbs.len();
                dbs.retain(|db| db.local_db != db_name);
                if dbs.len() != before {
                    addr_to_restart = Some(addr.clone());
                    if dbs.is_empty() {
                        empty_addr = Some(addr.clone());
                    }
                    break;
                }
            }
            if let Some(addr) = empty_addr {
                peers.remove(&addr);
            }
        }
        if let Some(addr) = addr_to_restart {
            if let Some(tx) = self.cancel.lock().remove(&addr) {
                let _ = tx.send(());
            }
            if self.peers.lock().contains_key(&addr) {
                self.spawn_connection(&addr);
            }
        }
    }

    fn remove_db_from_other_peers(&self, db_name: &str, keep_addr: &str) {
        let mut cancel_addrs = Vec::new();
        {
            let mut peers = self.peers.lock();
            for (addr, dbs) in peers.iter_mut() {
                if addr == keep_addr {
                    continue;
                }
                let before = dbs.len();
                dbs.retain(|db| db.local_db != db_name);
                if dbs.len() != before {
                    if dbs.is_empty() {
                        cancel_addrs.push(addr.clone());
                    }
                }
            }
            for addr in &cancel_addrs {
                peers.remove(addr);
            }
        }
        for addr in cancel_addrs {
            if let Some(tx) = self.cancel.lock().remove(&addr) {
                let _ = tx.send(());
            }
        }
    }

    fn verify_handshake(&self, addr: &str, dbs: &[Source]) -> Result<(), String> {
        let mut stream = self.connect(addr)?;
        self.send_hello(&mut stream, dbs)?;
        let (op, payload) = read_frame(&mut stream)?;
        if op == RES_ERR {
            return Err(format!(
                "upstream rejected handshake: {}",
                String::from_utf8_lossy(&payload)
            ));
        }
        Ok(())
    }

    fn spawn_connection(&self, addr: &str) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cancel.lock().insert(addr.to_string(), tx);
        let addr = addr.to_string();
        let server_id = self.server_id.clone();
        let tls = self.tls.clone();
        let stores = self
            .stores
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        let peers = Arc::clone(&self.peers);
        let cancel_map = Arc::clone(&self.cancel);
        std::thread::spawn(move || {
            let mgr = Manager {
                server_id,
                peers,
                cancel: cancel_map,
                stores,
                tls,
            };
            mgr.maintain_connection(&addr, rx);
        });
    }

    fn maintain_connection(&self, addr: &str, cancel: std::sync::mpsc::Receiver<()>) {
        let mut retry = Duration::from_millis(100);
        loop {
            if cancel.try_recv().is_ok() {
                return;
            }
            let dbs = self.peers.lock().get(addr).cloned().unwrap_or_default();
            if dbs.is_empty() {
                return;
            }
            if self.connect_and_sync(addr, &dbs, &cancel).is_err() {
                if cancel.try_recv().is_ok() {
                    return;
                }
                std::thread::sleep(retry);
                retry = (retry * 2).min(Duration::from_secs(3));
                continue;
            }
            retry = Duration::from_millis(100);
        }
    }

    fn connect_and_sync(
        &self,
        addr: &str,
        dbs: &[Source],
        cancel: &std::sync::mpsc::Receiver<()>,
    ) -> Result<(), String> {
        let mut stream = self.connect(addr)?;
        self.send_hello(&mut stream, dbs)?;

        let mut remote_to_local: HashMap<String, Vec<String>> = HashMap::new();
        let mut expected_offset: HashMap<String, u64> = HashMap::new();
        for cfg in dbs {
            remote_to_local
                .entry(cfg.remote_db.clone())
                .or_default()
                .push(cfg.local_db.clone());
        }

        loop {
            if cancel.try_recv().is_ok() {
                return Err("cancelled".into());
            }
            let (op, payload) = read_frame(&mut stream)?;
            if op == RES_ERR {
                return Err(format!(
                    "remote error during stream: {}",
                    String::from_utf8_lossy(&payload)
                ));
            }

            let payload = if op == OP_REPL_SAFE_POINT || op == OP_REPL_LOG_RANGE {
                verify_crc_payload(&payload)?
            } else {
                payload
            };

            if op == OP_REPL_SAFE_POINT {
                handle_safe_point(&payload, &remote_to_local, &self.stores);
                continue;
            }

            if op == OP_REPL_LOG_RANGE {
                apply_log_segment(
                    &payload,
                    &remote_to_local,
                    &self.stores,
                    &mut expected_offset,
                    &mut stream,
                )?;
                continue;
            }
        }
    }

    fn connect(
        &self,
        addr: &str,
    ) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, String> {
        use std::net::ToSocketAddrs;
        let sock = addr
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .next()
            .ok_or_else(|| "resolve failed".to_string())?;
        let tcp = TcpStream::connect_timeout(&sock, Duration::from_secs(10))
            .map_err(|e| e.to_string())?;
        tcp.set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        tcp.set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        let server_name = ServerName::try_from("localhost".to_string()).map_err(|_| "bad sni")?;
        let conn = rustls::ClientConnection::new(self.tls.clone(), server_name)
            .map_err(|e| e.to_string())?;
        Ok(rustls::StreamOwned::new(conn, tcp))
    }

    fn send_hello(
        &self,
        stream: &mut rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
        dbs: &[Source],
    ) -> Result<(), String> {
        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_be_bytes());
        body.extend_from_slice(&(self.server_id.len() as u32).to_be_bytes());
        body.extend_from_slice(self.server_id.as_bytes());
        body.extend_from_slice(&(dbs.len() as u32).to_be_bytes());
        for cfg in dbs {
            let offset = self
                .stores
                .get(&cfg.local_db)
                .map(|st| st.last_log_offset())
                .unwrap_or(0);
            body.extend_from_slice(&(cfg.remote_db.len() as u32).to_be_bytes());
            body.extend_from_slice(cfg.remote_db.as_bytes());
            body.extend_from_slice(&offset.to_be_bytes());
        }
        write_frame(stream, OP_REPL_HELLO, &body)
    }
}

fn write_frame(
    stream: &mut rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
    op: u8,
    body: &[u8],
) -> Result<(), String> {
    let frame = encode_frame(op, body);
    stream.write_all(&frame).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(())
}

fn read_frame(
    stream: &mut rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
) -> Result<(u8, Vec<u8>), String> {
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).map_err(|e| e.to_string())?;
    let hdr = decode_header(&head).ok_or_else(|| "bad header".to_string())?;
    let mut payload = vec![0u8; hdr.payload_len as usize];
    if !payload.is_empty() {
        stream.read_exact(&mut payload).map_err(|e| e.to_string())?;
    }
    Ok((hdr.status_or_op, payload))
}

fn verify_crc_payload(payload: &[u8]) -> Result<Vec<u8>, String> {
    if payload.len() < 4 {
        return Err("packet too short for crc".into());
    }
    let crc_received = u32::from_be_bytes(payload[..4].try_into().unwrap());
    let body = &payload[4..];
    if crc32fast::hash(body) != crc_received {
        return Err("crc mismatch on replication stream".into());
    }
    Ok(body.to_vec())
}

fn read_len_prefixed_string(buf: &[u8], cursor: usize) -> Option<(String, usize)> {
    if cursor + 4 > buf.len() {
        return None;
    }
    let n = u32::from_be_bytes(buf[cursor..cursor + 4].try_into().ok()?) as usize;
    let start = cursor + 4;
    if start + n > buf.len() {
        return None;
    }
    Some((
        String::from_utf8_lossy(&buf[start..start + n]).into_owned(),
        start + n,
    ))
}

fn handle_safe_point(
    payload: &[u8],
    remote_to_local: &HashMap<String, Vec<String>>,
    stores: &HashMap<String, Arc<Database>>,
) {
    let (remote_db, mut cursor) = match read_len_prefixed_string(payload, 0) {
        Some(v) => v,
        None => return,
    };
    cursor += 4;
    if cursor + 8 > payload.len() {
        return;
    }
    let safe = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
    if let Some(locals) = remote_to_local.get(&remote_db) {
        for local in locals {
            if let Some(st) = stores.get(local) {
                st.set_leader_retain_offset(safe);
            }
        }
    }
}

fn apply_log_segment(
    payload: &[u8],
    remote_to_local: &HashMap<String, Vec<String>>,
    stores: &HashMap<String, Arc<Database>>,
    expected_offset: &mut HashMap<String, u64>,
    stream: &mut rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
) -> Result<(), String> {
    let (remote_db, mut cursor) =
        read_len_prefixed_string(payload, 0).ok_or_else(|| "malformed log segment packet".to_string())?;
    cursor += 4;
    if cursor + 16 > payload.len() {
        return Err("malformed log segment header".into());
    }
    let start_off = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
    let end_off = u64::from_be_bytes(payload[cursor + 8..cursor + 16].try_into().unwrap());
    cursor += 16;
    let seg_data = &payload[cursor..];
    if end_off < start_off || (end_off - start_off) as usize != seg_data.len() {
        return Err(format!(
            "log segment offset mismatch (start={start_off} end={end_off} len={})",
            seg_data.len()
        ));
    }

    let locals = remote_to_local
        .get(&remote_db)
        .cloned()
        .unwrap_or_default();
    for local_db in locals {
        let Some(st) = stores.get(&local_db) else {
            continue;
        };
        let mut exp = expected_offset
            .get(&local_db)
            .copied()
            .unwrap_or_else(|| st.last_log_offset());
        if start_off != exp {
            if exp == 0 && start_off > 0 && st.last_log_offset() == 0 && !seg_data.is_empty() {
                st.init_log_at_lsn(start_off).map_err(|e| e.to_string())?;
                exp = start_off;
            } else {
                return Err(format!(
                    "log range start mismatch for db {local_db}: got {start_off} want {exp}"
                ));
            }
        }
        st.apply_log_range(seg_data).map_err(|e| e.to_string())?;
        expected_offset.insert(local_db.clone(), end_off);
        if end_off > 0 {
            let mut ack = Vec::new();
            ack.extend_from_slice(&(remote_db.len() as u32).to_be_bytes());
            ack.extend_from_slice(remote_db.as_bytes());
            ack.extend_from_slice(&end_off.to_be_bytes());
            write_frame(stream, OP_REPL_ACK, &ack)?;
        }
    }
    Ok(())
}
