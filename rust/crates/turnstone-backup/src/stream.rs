// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::ClientConnection;
use rustls::StreamOwned;
use turnstone_protocol as protocol;

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Protocol(String),
    #[error("TLS config is required")]
    TlsRequired,
}

pub struct StreamOptions {
    pub host: String,
    pub db_name: String,
    pub start_lsn: u64,
    pub wait_idle: Duration,
    pub client_id: String,
    pub tls: Option<Arc<rustls::ClientConfig>>,
}

pub struct StreamResult {
    pub base_lsn: u64,
    pub end_lsn: u64,
    pub bytes: i64,
}

pub fn stream_log_range(
    opts: &StreamOptions,
    writer: &mut impl Write,
) -> Result<StreamResult, StreamError> {
    let tls = opts.tls.as_ref().ok_or(StreamError::TlsRequired)?;
    let client_id = if opts.client_id.is_empty() {
        "turnstone-backup"
    } else {
        &opts.client_id
    };
    let wait_idle = if opts.wait_idle.is_zero() {
        Duration::from_secs(2)
    } else {
        opts.wait_idle
    };

    let addr = opts
        .host
        .to_socket_addrs()
        .map_err(StreamError::Io)?
        .next()
        .ok_or_else(|| StreamError::Protocol("no addresses".into()))?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(10))?;
    let server_name = ServerName::try_from(opts.host.as_str())
        .map_err(|_| StreamError::Protocol("invalid server name".into()))?
        .to_owned();
    let conn = ClientConnection::new(Arc::clone(tls), server_name).map_err(|e| {
        StreamError::Protocol(format!("tls: {e}"))
    })?;
    let mut stream = StreamOwned::new(conn, tcp);
    stream.flush()?;

    send_hello(&mut stream, client_id, &opts.db_name, opts.start_lsn)?;

    let mut header = [0u8; protocol::PROTO_HEADER_SIZE];
    read_full(&mut stream, &mut header)?;
    if header[0] != protocol::RES_OK {
        return Err(StreamError::Protocol("handshake rejected".into()));
    }
    let ln = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    if ln > 0 {
        let mut skip = vec![0u8; ln];
        read_full(&mut stream, &mut skip)?;
    }

    let mut result = StreamResult {
        base_lsn: opts.start_lsn,
        end_lsn: opts.start_lsn,
        bytes: 0,
    };
    let mut last_data = std::time::Instant::now();
    let mut expect_off = opts.start_lsn;
    let mut have_expect = false;

    loop {
        stream.get_mut().set_read_timeout(Some(wait_idle))?;
        match read_full(&mut stream, &mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                break;
            }
            Err(e) => return Err(StreamError::Io(e)),
        }
        last_data = std::time::Instant::now();
        let op = header[0];
        let length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; length];
        if length > 0 {
            read_full(&mut stream, &mut payload)?;
        }
        if op == protocol::RES_ERR {
            return Err(StreamError::Protocol(String::from_utf8_lossy(&payload).into_owned()));
        }
        if op != protocol::OP_REPL_SAFE_POINT && op != protocol::OP_REPL_LOG_RANGE {
            continue;
        }
        if payload.len() < 4 {
            return Err(StreamError::Protocol("payload too short for CRC".into()));
        }
        let crc_received = u32::from_be_bytes(payload[0..4].try_into().unwrap());
        let raw_body = &payload[4..];
        if crc32fast::hash(raw_body) != crc_received {
            return Err(StreamError::Protocol("CRC mismatch on replication stream".into()));
        }
        if op == protocol::OP_REPL_LOG_RANGE {
            if raw_body.len() < 16 {
                continue;
            }
            let start_off = u64::from_be_bytes(raw_body[0..8].try_into().unwrap());
            let end_off = u64::from_be_bytes(raw_body[8..16].try_into().unwrap());
            let seg = &raw_body[16..];
            if seg.is_empty() {
                continue;
            }
            if result.bytes == 0 {
                result.base_lsn = start_off;
                expect_off = start_off;
                have_expect = true;
            } else if have_expect && start_off != expect_off {
                return Err(StreamError::Protocol(format!(
                    "log range start mismatch: got {start_off} want {expect_off}"
                )));
            }
            writer.write_all(seg)?;
            result.bytes += seg.len() as i64;
            result.end_lsn = end_off;
            expect_off = end_off;
        }
        let _ = last_data;
    }
    Ok(result)
}

fn send_hello(w: &mut impl Write, client_id: &str, db_name: &str, start_lsn: u64) -> Result<(), StreamError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u32.to_be_bytes());
    payload.extend_from_slice(&(client_id.len() as u32).to_be_bytes());
    payload.extend_from_slice(client_id.as_bytes());
    payload.extend_from_slice(&1u32.to_be_bytes());
    payload.extend_from_slice(&(db_name.len() as u32).to_be_bytes());
    payload.extend_from_slice(db_name.as_bytes());
    payload.extend_from_slice(&start_lsn.to_be_bytes());
    let frame = protocol::encode_frame(protocol::OP_REPL_HELLO, &payload);
    w.write_all(&frame)?;
    w.flush()?;
    Ok(())
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<()> {
    let mut off = 0;
    while off < buf.len() {
        off += r.read(&mut buf[off..])?;
    }
    Ok(())
}
