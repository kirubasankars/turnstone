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

enum BackupConn {
    Plain(TcpStream),
    Tls(StreamOwned<ClientConnection, TcpStream>),
}

impl Read for BackupConn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(t) => t.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for BackupConn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(t) => t.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(t) => t.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

impl BackupConn {
    fn set_read_timeout(&mut self, d: Option<Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(t) => t.set_read_timeout(d),
            Self::Tls(s) => s.get_mut().set_read_timeout(d),
        }
    }
}

pub fn stream_log_range(
    opts: &StreamOptions,
    writer: &mut impl Write,
) -> Result<StreamResult, StreamError> {
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

    let mut stream = if let Some(tls) = opts.tls.as_ref() {
        let server_name = ServerName::try_from(opts.host.as_str())
            .map_err(|_| StreamError::Protocol("invalid server name".into()))?
            .to_owned();
        let conn = ClientConnection::new(Arc::clone(tls), server_name)
            .map_err(|e| StreamError::Protocol(format!("tls: {e}")))?;
        BackupConn::Tls(StreamOwned::new(conn, tcp))
    } else {
        BackupConn::Plain(tcp)
    };
    stream.flush()?;

    send_hello(&mut stream, client_id, &opts.db_name, opts.start_lsn)?;

    let mut header = [0u8; protocol::PROTO_HEADER_SIZE];
    stream.read_exact(&mut header)?;
    if header[0] != protocol::RES_OK {
        return Err(StreamError::Protocol("handshake rejected".into()));
    }
    let ln = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    if ln > 0 {
        let mut skip = vec![0u8; ln];
        stream.read_exact(&mut skip)?;
    }

    let mut result = StreamResult {
        base_lsn: opts.start_lsn,
        end_lsn: opts.start_lsn,
        bytes: 0,
    };
    let mut expect_off = opts.start_lsn;
    let mut have_expect = false;

    loop {
        stream.set_read_timeout(Some(wait_idle))?;
        match stream.read_exact(&mut header) {
            Ok(()) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => return Err(StreamError::Io(e)),
        }
        let op = header[0];
        let length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; length];
        if length > 0 {
            stream.read_exact(&mut payload)?;
        }
        if op == protocol::RES_ERR {
            return Err(StreamError::Protocol(
                String::from_utf8_lossy(&payload).into_owned(),
            ));
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
            return Err(StreamError::Protocol(
                "CRC mismatch on replication stream".into(),
            ));
        }
        if op == protocol::OP_REPL_LOG_RANGE {
            let (start_off, end_off, seg) = parse_log_range_payload(raw_body, &opts.db_name)?;
            if seg.is_empty() {
                continue;
            }
            if result.bytes == 0 {
                result.base_lsn = start_off;
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
    }

    if result.end_lsn > opts.start_lsn {
        let _ = send_repl_ack(&mut stream, &opts.db_name, result.end_lsn);
    }
    send_quit(&mut stream);
    Ok(result)
}

/// Wire layout after CRC: db_name_len + name + reserved u32 + start/end u64 + segment.
pub(crate) fn parse_log_range_payload<'a>(
    raw_body: &'a [u8],
    want_db: &str,
) -> Result<(u64, u64, &'a [u8]), StreamError> {
    if raw_body.len() < 4 {
        return Err(StreamError::Protocol("malformed log range packet".into()));
    }
    let n_len = u32::from_be_bytes(raw_body[0..4].try_into().unwrap()) as usize;
    let mut cursor = 4;
    if cursor + n_len + 4 + 16 > raw_body.len() {
        return Err(StreamError::Protocol("malformed log range db name".into()));
    }
    let db_name = std::str::from_utf8(&raw_body[cursor..cursor + n_len])
        .map_err(|_| StreamError::Protocol("malformed log range db name".into()))?;
    cursor += n_len + 4;
    if db_name != want_db {
        return Err(StreamError::Protocol(format!(
            "unexpected database in stream: {db_name}"
        )));
    }
    let start_off = u64::from_be_bytes(raw_body[cursor..cursor + 8].try_into().unwrap());
    let end_off = u64::from_be_bytes(raw_body[cursor + 8..cursor + 16].try_into().unwrap());
    cursor += 16;
    let seg = &raw_body[cursor..];
    if end_off < start_off || (end_off - start_off) as usize != seg.len() {
        return Err(StreamError::Protocol("log range offset mismatch".into()));
    }
    Ok((start_off, end_off, seg))
}

fn send_hello(
    w: &mut impl Write,
    client_id: &str,
    db_name: &str,
    start_lsn: u64,
) -> Result<(), StreamError> {
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

fn send_repl_ack(w: &mut impl Write, db_name: &str, offset: u64) -> Result<(), StreamError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(db_name.len() as u32).to_be_bytes());
    payload.extend_from_slice(db_name.as_bytes());
    payload.extend_from_slice(&offset.to_be_bytes());
    let frame = protocol::encode_frame(protocol::OP_REPL_ACK, &payload);
    w.write_all(&frame)?;
    w.flush()?;
    Ok(())
}

fn send_quit(w: &mut impl Write) {
    let frame = protocol::encode_frame(protocol::OP_QUIT, &[]);
    let _ = w.write_all(&frame);
    let _ = w.flush();
}

#[cfg(test)]
mod tests {
    use super::parse_log_range_payload;

    #[test]
    fn parse_log_range_payload_ok() {
        let payload = [
            0, 0, 0, 1, b'1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3,
        ];
        let (start, end, data) = parse_log_range_payload(&payload, "1").unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 3);
        assert_eq!(data, &[1, 2, 3]);
    }

    #[test]
    fn parse_log_range_payload_rejects_short() {
        assert!(parse_log_range_payload(&[], "1").is_err());
    }

    #[test]
    fn parse_log_range_payload_rejects_wrong_db() {
        let payload = [
            0, 0, 0, 1, b'2', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
        ];
        assert!(parse_log_range_payload(&payload, "1").is_err());
    }
}
