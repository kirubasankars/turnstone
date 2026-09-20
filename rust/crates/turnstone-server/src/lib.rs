// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

mod replication;

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use rustls::ServerConnection;
use rustls::StreamOwned;
use turnstone_database::{Database, STATE_PRIMARY, STATE_REPLICA, STATE_STEPPING_DOWN, STATE_UNDEFINED};
use turnstone_engine::EngineError;
use turnstone_protocol as protocol;
use turnstone_repl::Manager;
use turnstone_tls::load_server_mtls;

pub const ROLE_CLIENT: &str = "client";
pub const ROLE_ADMIN: &str = "admin";
pub const ROLE_SERVER: &str = "server";
pub const ROLE_BACKUP: &str = "backup";

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("tls cert, key, and ca required")]
    TlsRequired,
    #[error(transparent)]
    Tls(#[from] turnstone_tls::TlsError),
    #[error("{0}")]
    Io(#[from] io::Error),
}

pub struct Server {
    pub(crate) stores: HashMap<String, Arc<Database>>,
    default_db: String,
    id: String,
    addr: String,
    max_conns: usize,
    sem: Arc<Semaphore>,
    wg: Mutex<Vec<()>>,
    active_conns: AtomicI64,
    tls_cert_file: String,
    tls_key_file: String,
    tls_ca_file: String,
    tls_config: Arc<RwLock<Arc<rustls::ServerConfig>>>,
    repl_manager: Option<Arc<Manager>>,
    dev_mode: bool,
    closing: AtomicBool,
    listener: Mutex<Option<TcpListener>>,
    db_conns: Mutex<HashMap<String, i64>>,
    active_clients: Mutex<HashMap<String, HashMap<usize, ()>>>,
    next_conn_id: AtomicU64,
}

struct Semaphore {
    permits: AtomicU32,
    max: u32,
}

impl Semaphore {
    fn new(max: usize) -> Self {
        Self {
            permits: AtomicU32::new(max as u32),
            max: max as u32,
        }
    }

    fn acquire(&self) -> bool {
        loop {
            let cur = self.permits.load(Ordering::Acquire);
            if cur == 0 {
                return false;
            }
            if self
                .permits
                .compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn release(&self) {
        loop {
            let cur = self.permits.load(Ordering::Acquire);
            if cur >= self.max {
                return;
            }
            if self
                .permits
                .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

pub(crate) struct ConnState {
    pub(crate) db_name: String,
    pub(crate) db: Arc<Database>,
    tx: Option<turnstone_engine::Transaction>,
    tx_start: Option<Instant>,
    pub(crate) role: String,
    client_id: String,
}

/// Rust's `TcpListener::bind(":6379")` fails on some hosts; Go accepts it. Map to all interfaces.
pub fn normalize_bind_addr(addr: &str) -> String {
    if let Some(port) = addr.strip_prefix(':') {
        if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) {
            return format!("0.0.0.0:{port}");
        }
    }
    addr.to_string()
}

pub fn new_server(
    id: impl Into<String>,
    addr: impl Into<String>,
    stores: HashMap<String, Arc<Database>>,
    max_conns: usize,
    tls_cert: impl Into<String>,
    tls_key: impl Into<String>,
    tls_ca: impl Into<String>,
    repl_manager: Option<Arc<Manager>>,
    dev_mode: bool,
) -> Result<Arc<Server>, ServerError> {
    let addr = normalize_bind_addr(&addr.into());
    let tls_cert = tls_cert.into();
    let tls_key = tls_key.into();
    let tls_ca = tls_ca.into();
    if tls_cert.is_empty() || tls_key.is_empty() || tls_ca.is_empty() {
        return Err(ServerError::TlsRequired);
    }
    let mut db_names: Vec<_> = stores.keys().cloned().collect();
    db_names.sort();
    let default_db = db_names.first().cloned().unwrap_or_default();
    let tls = load_server_mtls(&tls_ca, &tls_cert, &tls_key)?;
    Ok(Arc::new(Server {
        stores,
        default_db,
        id: id.into(),
        addr,
        max_conns,
        sem: Arc::new(Semaphore::new(max_conns)),
        wg: Mutex::new(Vec::new()),
        active_conns: AtomicI64::new(0),
        tls_cert_file: tls_cert,
        tls_key_file: tls_key,
        tls_ca_file: tls_ca,
        tls_config: Arc::new(RwLock::new(tls)),
        repl_manager,
        dev_mode,
        closing: AtomicBool::new(false),
        listener: Mutex::new(None),
        db_conns: Mutex::new(HashMap::new()),
        active_clients: Mutex::new(HashMap::new()),
        next_conn_id: AtomicU64::new(0),
    }))
}

impl Server {
    pub fn reload_tls(&self) -> Result<(), ServerError> {
        let cfg = load_server_mtls(&self.tls_ca_file, &self.tls_cert_file, &self.tls_key_file)?;
        *self.tls_config.write() = cfg;
        Ok(())
    }

    pub fn addr(&self) -> Option<SocketAddr> {
        self.listener
            .lock()
            .as_ref()
            .and_then(|l| l.local_addr().ok())
    }

    pub fn run(self: &Arc<Self>) -> io::Result<()> {
        {
            let mut guard = self.listener.lock();
            if guard.is_none() {
                let listener = TcpListener::bind(&self.addr)?;
                listener.set_nonblocking(false)?;
                *guard = Some(listener);
            }
        }
        if let Some(rm) = &self.repl_manager {
            rm.start();
        }
        loop {
            if self.closing.load(Ordering::Acquire) {
                return Ok(());
            }
            let (tcp, _) = {
                let guard = self.listener.lock();
                let ln = guard.as_ref().unwrap().try_clone().map_err(io::Error::other)?;
                drop(guard);
                ln.accept()?
            };
            if !self.sem.acquire() {
                let _ = reject_busy(tcp, &self.tls_config.read());
                continue;
            }
            self.active_conns.fetch_add(1, Ordering::Relaxed);
            let srv = Arc::clone(self);
            thread::spawn(move || {
                srv.handle_connection(tcp);
                srv.active_conns.fetch_sub(1, Ordering::Relaxed);
                srv.sem.release();
            });
        }
    }

    fn handle_connection(self: &Arc<Self>, tcp: TcpStream) {
        let cfg = self.tls_config.read().clone();
        let Ok(tls) = ServerConnection::new(cfg) else {
            return;
        };
        let mut stream = StreamOwned::new(tls, tcp);
        if drive_tls_handshake(&mut stream).is_err() {
            return;
        }
        let default_db = self.default_db.clone();
        let db = self.stores.get(&default_db).cloned().unwrap();
        let mut state = ConnState {
            db_name: default_db.clone(),
            db,
            tx: None,
            tx_start: None,
            role: ROLE_CLIENT.to_string(),
            client_id: "unknown".to_string(),
        };
        self.track_conn(&state.db_name, 1);
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        self.register_conn(&state.db_name, conn_id);
        let _guard = ConnGuard {
            server: self,
            db_name: state.db_name.clone(),
            conn_id,
        };
        let mut header = [0u8; protocol::PROTO_HEADER_SIZE];
        loop {
            if self.closing.load(Ordering::Acquire) {
                break;
            }
            let _ = stream.get_mut().set_read_timeout(Some(protocol::IDLE_TIMEOUT));
            if read_full(&mut stream, &mut header).is_err() {
                break;
            }
            if state.client_id == "unknown" {
                let (role, client_id) = peer_identity(&mut stream);
                state.role = role;
                state.client_id = client_id;
            }
            let op = header[0];
            let payload_len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
            if payload_len as u64 > protocol::MAX_COMMAND_SIZE {
                let _ = write_binary_response(&mut stream, protocol::RES_ENTITY_TOO_LARGE, &[]);
                break;
            }
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 && read_full(&mut stream, &mut payload).is_err() {
                break;
            }
            if self.dispatch_command(&mut stream, op, &payload, &mut state) {
                break;
            }
        }
        if let Some(mut tx) = state.tx.take() {
            tx.discard();
        }
        self.track_conn(&state.db_name, -1);
    }

    fn dispatch_command(
        self: &Arc<Self>,
        conn: &mut TlsStream,
        op: u8,
        payload: &[u8],
        st: &mut ConnState,
    ) -> bool {
        if !is_op_allowed(&st.role, op) {
            let _ = write_binary_response(
                conn,
                protocol::RES_ERR,
                format!("Permission Denied for role: {}", st.role).as_bytes(),
            );
            return false;
        }
        if st.tx.is_some() {
            match op {
                protocol::OP_SELECT
                | protocol::OP_REPLICA_OF
                | protocol::OP_STEP_DOWN
                | protocol::OP_PROMOTE
                | protocol::OP_FLUSH_DB
                | protocol::OP_STAT
                | protocol::OP_QUIT => {
                    let _ = write_binary_response(
                        conn,
                        protocol::RES_ERR,
                        b"Command not allowed inside a transaction",
                    );
                    return false;
                }
                _ => {}
            }
        }
        match op {
            protocol::OP_PING => {
                let _ = write_binary_response(conn, protocol::RES_OK, b"PONG");
            }
            protocol::OP_QUIT => return true,
            protocol::OP_SELECT => self.handle_select(conn, payload, st),
            protocol::OP_BEGIN => self.handle_begin(conn, payload, st),
            protocol::OP_COMMIT => self.handle_commit(conn, st),
            protocol::OP_ABORT => self.handle_abort(conn, st),
            protocol::OP_GET => self.handle_get(conn, payload, st),
            protocol::OP_SET => self.handle_set(conn, payload, st),
            protocol::OP_DEL => self.handle_del(conn, payload, st),
            protocol::OP_STAT => self.handle_stat(conn, st),
            protocol::OP_REPL_HELLO => {
                replication::handle_replica_connection(self, conn, payload, st);
                return true;
            }
            _ => {
                let _ = write_binary_response(conn, protocol::RES_ERR, b"Unknown OpCode");
            }
        }
        false
    }

    fn handle_select(&self, conn: &mut TlsStream, payload: &[u8], st: &mut ConnState) {
        let name = String::from_utf8_lossy(payload);
        if let Some(db) = self.stores.get(name.as_ref()) {
            self.track_conn(&st.db_name, -1);
            st.db_name = name.into_owned();
            st.db = Arc::clone(db);
            self.track_conn(&st.db_name, 1);
            let _ = write_binary_response(conn, protocol::RES_OK, &[]);
        } else {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Database not found");
        }
    }

    fn handle_begin(&self, conn: &mut TlsStream, payload: &[u8], st: &mut ConnState) {
        let state = st.db.get_state();
        if state != STATE_PRIMARY && state != STATE_REPLICA {
            let _ = write_binary_response(
                conn,
                protocol::RES_ERR,
                b"Database not available for transactions",
            );
            return;
        }
        if st.tx.is_some() {
            let _ = write_binary_response(conn, protocol::RES_TX_IN_PROGRESS, &[]);
            return;
        }
        let read_only = payload.first().copied() == Some(protocol::BEGIN_READ_ONLY);
        if payload.len() > 1 {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Invalid BEGIN payload");
            return;
        }
        let update = state == STATE_PRIMARY && !read_only;
        st.tx = Some(st.db.new_transaction(update));
        st.tx_start = Some(Instant::now());
        let _ = write_binary_response(conn, protocol::RES_OK, &[]);
    }

    fn handle_commit(&self, conn: &mut TlsStream, st: &mut ConnState) {
        let Some(mut tx) = st.tx.take() else {
            let _ = write_binary_response(conn, protocol::RES_TX_REQUIRED, &[]);
            return;
        };
        if !self.dev_mode {
            if let Some(start) = st.tx_start {
                if start.elapsed() > protocol::MAX_TX_DURATION {
                    let _ = write_binary_response(
                        conn,
                        protocol::RES_TX_TIMEOUT,
                        b"Transaction exceeded 5s limit",
                    );
                    return;
                }
            }
        }
        st.tx_start = None;
        match tx.commit() {
            Ok(()) => {
                if st.db.min_replicas() > 0 {
                    let off = st.db.durable_offset();
                    if let Err(e) = st.db.wait_for_quorum(off, Duration::ZERO, None) {
                        let _ = write_binary_response(conn, protocol::RES_SERVER_BUSY, e.to_string().as_bytes());
                        return;
                    }
                }
                let _ = write_binary_response(conn, protocol::RES_OK, &[]);
            }
            Err(EngineError::WriteConflict) => {
                let _ = write_binary_response(conn, protocol::RES_TX_CONFLICT, b"write conflict detected");
            }
            Err(EngineError::DiskFull) => {
                let _ = write_binary_response(conn, protocol::RES_SERVER_BUSY, b"ERR disk is full");
            }
            Err(e) => {
                let _ = write_binary_response(conn, protocol::RES_ERR, e.to_string().as_bytes());
            }
        }
    }

    fn handle_abort(&self, conn: &mut TlsStream, st: &mut ConnState) {
        if let Some(mut tx) = st.tx.take() {
            tx.discard();
        }
        st.tx_start = None;
        let _ = write_binary_response(conn, protocol::RES_OK, &[]);
    }

    fn handle_get(&self, conn: &mut TlsStream, payload: &[u8], st: &mut ConnState) {
        let Some(tx) = st.tx.as_mut() else {
            let _ = write_binary_response(conn, protocol::RES_TX_REQUIRED, &[]);
            return;
        };
        if st.db.get_state() == STATE_UNDEFINED {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Database undefined (no reads)");
            return;
        }
        let key = String::from_utf8_lossy(payload);
        if !protocol::is_ascii(&key) {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Key must be ASCII");
            return;
        }
        match tx.get(payload) {
            Ok(val) => {
                let _ = write_binary_response(conn, protocol::RES_OK, &val);
            }
            Err(EngineError::KeyNotFound) => {
                let _ = write_binary_response(conn, protocol::RES_NOT_FOUND, &[]);
            }
            Err(EngineError::WriteConflict) => {
                let _ = write_binary_response(conn, protocol::RES_TX_CONFLICT, b"write conflict detected");
            }
            Err(e) => {
                let _ = write_binary_response(conn, protocol::RES_ERR, e.to_string().as_bytes());
            }
        }
    }

    fn handle_set(&self, conn: &mut TlsStream, payload: &[u8], st: &mut ConnState) {
        let state = st.db.get_state();
        if state != STATE_PRIMARY && state != STATE_STEPPING_DOWN {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Read-only/Undefined state");
            return;
        }
        let Some(tx) = st.tx.as_mut() else {
            let _ = write_binary_response(conn, protocol::RES_TX_REQUIRED, &[]);
            return;
        };
        if payload.len() < 4 {
            let _ = write_binary_response(conn, protocol::RES_ERR, &[]);
            return;
        }
        let k_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
        if payload.len() < 4 + k_len {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Invalid payload size");
            return;
        }
        let key = &payload[4..4 + k_len];
        let val = &payload[4 + k_len..];
        if val.len() as u64 > protocol::MAX_VALUE_SIZE {
            let _ = write_binary_response(
                conn,
                protocol::RES_ENTITY_TOO_LARGE,
                b"Value size exceeds 64KB limit",
            );
            return;
        }
        let key_str = String::from_utf8_lossy(key);
        if !protocol::is_ascii(&key_str) {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"Key must be ASCII");
            return;
        }
        match tx.put(key, val) {
            Ok(()) => {
                let _ = write_binary_response(conn, protocol::RES_OK, &[]);
            }
            Err(EngineError::WriteConflict) => {
                let _ = write_binary_response(conn, protocol::RES_TX_CONFLICT, b"write conflict detected");
            }
            Err(e) => {
                let _ = write_binary_response(conn, protocol::RES_ERR, e.to_string().as_bytes());
            }
        }
    }

    fn handle_del(&self, conn: &mut TlsStream, payload: &[u8], st: &mut ConnState) {
        let _ = payload;
        let _ = write_binary_response(conn, protocol::RES_ERR, b"DEL not fully ported");
    }

    fn handle_stat(&self, conn: &mut TlsStream, st: &mut ConnState) {
        let stats = st.db.stats();
        let conns = self.database_conns(&st.db_name);
        let body = serde_json::json!({
            "state": st.db.get_state(),
            "key_count": stats.key_count,
            "conflicts": stats.conflicts,
            "active_connections": conns,
            "log_bytes": stats.log_size,
            "log_allocated_bytes": stats.log_allocated,
            "log_offset": stats.offset,
            "active_txs": stats.active_txs,
            "replica_lag": stats.replica_lag,
            "replicas": stats.replicas,
            "uptime": stats.uptime,
            "min_replicas": st.db.min_replicas(),
        });
        let _ = write_binary_response(conn, protocol::RES_OK, &body.to_string().into_bytes());
    }

    fn track_conn(&self, db: &str, delta: i64) {
        let mut m = self.db_conns.lock();
        *m.entry(db.to_string()).or_insert(0) += delta;
    }

    fn database_conns(&self, db: &str) -> i64 {
        *self.db_conns.lock().get(db).unwrap_or(&0)
    }

    fn register_conn(&self, db: &str, id: u64) {
        self.active_clients
            .lock()
            .entry(db.to_string())
            .or_default()
            .insert(id as usize, ());
    }

    pub fn close_all(self: &Arc<Self>) {
        self.closing.store(true, Ordering::Release);
        if let Some(l) = self.listener.lock().take() {
            let _ = l.set_nonblocking(true);
            drop(l);
        }
        if let Some(rm) = &self.repl_manager {
            rm.stop_all();
        }
        for store in self.stores.values() {
            let _ = store.close();
        }
    }
}

struct ConnGuard<'a> {
    server: &'a Server,
    db_name: String,
    conn_id: u64,
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        if let Some(m) = self.server.active_clients.lock().get_mut(&self.db_name) {
            m.remove(&(self.conn_id as usize));
        }
    }
}

fn is_op_allowed(role: &str, op: u8) -> bool {
    if role == ROLE_ADMIN || role == ROLE_SERVER {
        return true;
    }
    if role == ROLE_CLIENT {
        return matches!(
            op,
            protocol::OP_PING
                | protocol::OP_QUIT
                | protocol::OP_SELECT
                | protocol::OP_BEGIN
                | protocol::OP_COMMIT
                | protocol::OP_ABORT
                | protocol::OP_GET
                | protocol::OP_SET
                | protocol::OP_DEL
                | protocol::OP_MGET
                | protocol::OP_MSET
                | protocol::OP_MDEL
                | protocol::OP_STAT
        );
    }
    false
}

pub fn write_binary_response(w: &mut impl Write, status: u8, body: &[u8]) -> io::Result<()> {
    let mut header = [0u8; protocol::PROTO_HEADER_SIZE];
    header[0] = status;
    header[1..5].copy_from_slice(&(body.len() as u32).to_be_bytes());
    w.write_all(&header)?;
    if !body.is_empty() {
        w.write_all(body)?;
    }
    w.flush()
}

fn drive_tls_handshake(stream: &mut TlsStream) -> io::Result<()> {
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    }
    Ok(())
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<()> {
    let mut off = 0;
    while off < buf.len() {
        off += r.read(&mut buf[off..])?;
    }
    Ok(())
}

fn reject_busy(tcp: TcpStream, cfg: &Arc<rustls::ServerConfig>) -> io::Result<()> {
    let Ok(conn) = ServerConnection::new(Arc::clone(cfg)) else {
        return Ok(());
    };
    let mut stream = StreamOwned::new(conn, tcp);
    let _ = write_binary_response(&mut stream, protocol::RES_SERVER_BUSY, b"Max connections");
    Ok(())
}

fn peer_identity(stream: &mut TlsStream) -> (String, String) {
    let mut role = ROLE_CLIENT.to_string();
    let mut client_id = "unknown".to_string();
    let _ = stream.flush();
    if let Some(certs) = stream.conn.peer_certificates() {
        if let Some(cert) = certs.first() {
            if let Ok(parsed) = x509_parser::parse_x509_certificate(cert.as_ref()) {
                let subject = &parsed.1.tbs_certificate.subject;
                for rdn in subject.iter() {
                    for attr in rdn.iter() {
                        if attr.attr_type().to_string() == "2.5.4.10" {
                            if let Ok(org) = std::str::from_utf8(attr.attr_value().as_bytes()) {
                                if let Some(rest) = org.strip_prefix("TurnstoneDB ") {
                                    role = rest.to_string();
                                }
                            }
                        }
                        if attr.attr_type().to_string() == "2.5.4.3" {
                            if let Ok(cn) = std::str::from_utf8(attr.attr_value().as_bytes()) {
                                client_id = cn.to_string();
                            }
                        }
                    }
                }
            }
        }
    }
    (role, client_id)
}
