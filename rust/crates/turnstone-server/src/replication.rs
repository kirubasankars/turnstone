// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{Read, Write};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rustls::StreamOwned;
use rustls::ServerConnection;
use std::net::TcpStream;
use turnstone_database::{Database, STATE_PRIMARY, STATE_STEPPING_DOWN};
use turnstone_protocol as protocol;

use crate::{write_binary_response, Server, ROLE_BACKUP, ROLE_SERVER};

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

const MAX_REPL_ACK_SIZE: u32 = 64 * 1024;
const MAX_REPLICATION_BATCH_SIZE: i64 = 1024 * 1024;

pub(crate) fn handle_replica_connection(
    server: &Arc<Server>,
    conn: &mut TlsStream,
    payload: &[u8],
    st: &mut crate::ConnState,
) {
    if payload.len() < 8 {
        let _ = write_binary_response(conn, protocol::RES_ERR, b"Payload too short");
        return;
    }
    let mut cursor = 4usize;
    if cursor + 4 > payload.len() {
        let _ = write_binary_response(conn, protocol::RES_ERR, b"ID len truncated");
        return;
    }
    let id_len = u32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    if cursor + id_len > payload.len() {
        let _ = write_binary_response(conn, protocol::RES_ERR, b"ID truncated");
        return;
    }
    let replica_id = String::from_utf8_lossy(&payload[cursor..cursor + id_len]).into_owned();
    cursor += id_len;
    if replica_id.is_empty() || replica_id == "client-unknown" {
        let _ = write_binary_response(conn, protocol::RES_ERR, b"Missing client ID");
        return;
    }
    if cursor + 4 > payload.len() {
        let _ = write_binary_response(conn, protocol::RES_ERR, b"Count truncated");
        return;
    }
    let count = u32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;

    struct Sub {
        name: String,
        offset: u64,
        store: Arc<Database>,
    }
    let mut subs = Vec::new();
    for _ in 0..count {
        if cursor + 4 > payload.len() {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"DB Name len truncated");
            return;
        }
        let n_len = u32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + n_len + 8 > payload.len() {
            let _ = write_binary_response(conn, protocol::RES_ERR, b"DB Name/Offset truncated");
            return;
        }
        let name = String::from_utf8_lossy(&payload[cursor..cursor + n_len]).into_owned();
        cursor += n_len;
        let offset = u64::from_be_bytes(payload[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let Some(store) = server.stores.get(&name).cloned() else {
            let _ = write_binary_response(
                conn,
                protocol::RES_ERR,
                format!("Unknown database: {name}").as_bytes(),
            );
            return;
        };
        if !store.is_valid_replication_cursor(offset) {
            let _ = write_binary_response(
                conn,
                protocol::RES_ERR,
                format!("Invalid replication cursor for DB '{name}'").as_bytes(),
            );
            return;
        }
        if let Some(rm) = &server.repl_manager {
            if rm.is_following(&name) {
                let _ = write_binary_response(
                    conn,
                    protocol::RES_ERR,
                    format!("Cascading replication disabled for DB '{name}'").as_bytes(),
                );
                return;
            }
        }
        let state = store.get_state();
        if state != STATE_PRIMARY && state != STATE_STEPPING_DOWN {
            let _ = write_binary_response(
                conn,
                protocol::RES_ERR,
                format!("Replica handshake rejected: DB '{name}' is {state} (must be PRIMARY)").as_bytes(),
            );
            return;
        }
        let mut slot_role = st.role.clone();
        if replica_id == "turnstone-backup" {
            slot_role = ROLE_BACKUP.to_string();
        } else if slot_role == ROLE_SERVER {
            // keep
        }
        let (start_off, gen) = store.register_replica_hello(&replica_id, offset, &slot_role);
        subs.push(Sub {
            name,
            offset: start_off,
            store: store.clone(),
        });
        let _ = gen;
    }

    if write_binary_response(conn, protocol::RES_OK, &[]).is_err() {
        return;
    }

    let (out_tx, out_rx) = std::sync::mpsc::sync_channel::<ReplPacket>(10);
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sub in subs {
        let out_tx = out_tx.clone();
        let done = Arc::clone(&done);
        thread::spawn(move || {
            let _ = run_log_stream_loop(&sub.name, &sub.store, sub.offset, &out_tx, &done);
        });
    }

    let mut header = [0u8; protocol::PROTO_HEADER_SIZE];
    loop {
        if read_full(conn, &mut header).is_err() {
            break;
        }
        if let Ok(pkt) = out_rx.try_recv() {
            if write_repl_packet(conn, &pkt).is_err() {
                break;
            }
        }
        let ln = u32::from_be_bytes(header[1..5].try_into().unwrap());
        if ln > MAX_REPL_ACK_SIZE {
            break;
        }
        let mut body = vec![0u8; ln as usize];
        if ln > 0 && read_full(conn, &mut body).is_err() {
            break;
        }
        if header[0] == protocol::OP_REPL_ACK && body.len() > 4 {
            let n_l = u32::from_be_bytes(body[0..4].try_into().unwrap()) as usize;
            if body.len() >= 4 + n_l + 8 {
                let db_name = String::from_utf8_lossy(&body[4..4 + n_l]);
                let offset = u64::from_be_bytes(body[4 + n_l..4 + n_l + 8].try_into().unwrap());
                if let Some(store) = server.stores.get(db_name.as_ref()) {
                    store.update_replica_offset(&replica_id, offset);
                }
            }
        }
    }
    done.store(true, Ordering::Relaxed);
}

use std::sync::atomic::Ordering;

struct ReplPacket {
    db_name: String,
    op: u8,
    data: Vec<u8>,
}

fn run_log_stream_loop(
    name: &str,
    store: &Database,
    start_offset: u64,
    out: &std::sync::mpsc::SyncSender<ReplPacket>,
    done: &Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), String> {
    let mut current = start_offset as i64;
    while !done.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(50));
        let head = store.durable_offset();
        if current as u64 >= head {
            continue;
        }
        let (seg, next) = store
            .read_log_range(current, MAX_REPLICATION_BATCH_SIZE)
            .map_err(|e| e.to_string())?;
        if seg.is_empty() {
            continue;
        }
        let mut payload = vec![0u8; 16 + seg.len()];
        payload[0..8].copy_from_slice(&(current as u64).to_be_bytes());
        payload[8..16].copy_from_slice(&(next as u64).to_be_bytes());
        payload[16..].copy_from_slice(&seg);
        out.send(ReplPacket {
            db_name: name.to_string(),
            op: protocol::OP_REPL_LOG_RANGE,
            data: payload,
        })
        .map_err(|e| e.to_string())?;
        current = next;
    }
    Ok(())
}

fn write_repl_packet(conn: &mut TlsStream, p: &ReplPacket) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(p.db_name.len() as u32).to_be_bytes());
    body.extend_from_slice(p.db_name.as_bytes());
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&p.data);
    let crc = crc32fast::hash(&body);
    let mut payload = Vec::with_capacity(4 + body.len());
    payload.extend_from_slice(&crc.to_be_bytes());
    payload.extend_from_slice(&body);
    write_binary_response(conn, p.op, &payload)
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<()> {
    let mut off = 0;
    while off < buf.len() {
        off += r.read(&mut buf[off..])?;
    }
    Ok(())
}
