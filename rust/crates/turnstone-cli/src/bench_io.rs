// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Bench hot-path I/O matching Go `bench.go`: write on the socket, read via `BufReader`.
//! Uses plain TCP when client CA/certs are absent (same rule as `connect.rs`).

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use turnstone_protocol as protocol;
use turnstone_tls::{cert_paths, load_mtls, Role};

use crate::connect::ConnectOptions;

const READ_BUF: usize = 256 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

enum BenchLink {
    Plain(BufReader<TcpStream>),
    Tls(BufReader<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

pub struct BenchConn {
    link: BenchLink,
}

impl BenchConn {
    pub fn connect(opts: &ConnectOptions) -> Result<Self, String> {
        let (ca, cert, key) = cert_paths(&opts.home, Role::Client);
        let addr = opts
            .host
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .next()
            .ok_or_else(|| "no addresses resolved".to_string())?;
        let tcp = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).map_err(|e| e.to_string())?;
        let _ = tcp.set_nodelay(true);

        let link = if ca.exists() {
            let tls = load_mtls(&ca, &cert, &key).map_err(|e| e.to_string())?;
            let host = opts.host.split(':').next().unwrap_or(&opts.host).to_string();
            let server_name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|_| "invalid DNS name for TLS".to_string())?;
            let conn = rustls::ClientConnection::new(tls, server_name).map_err(|e| e.to_string())?;
            let mut stream = rustls::StreamOwned::new(conn, tcp);
            while stream.conn.is_handshaking() {
                stream
                    .conn
                    .complete_io(&mut stream.sock)
                    .map_err(|e| e.to_string())?;
            }
            BenchLink::Tls(BufReader::with_capacity(READ_BUF, stream))
        } else {
            BenchLink::Plain(BufReader::with_capacity(READ_BUF, tcp))
        };

        Ok(Self { link })
    }

    pub fn select_db(&mut self, db: i32) -> Result<(), String> {
        let name = db.to_string();
        let frame = protocol::encode_frame(protocol::OP_SELECT, name.as_bytes());
        self.write_all(&frame)?;
        self.read_one_frame_discard()?;
        Ok(())
    }

    pub fn pipeline(&mut self, write_data: &[u8], expected_responses: usize) -> Result<(), String> {
        self.write_all(write_data)?;
        for _ in 0..expected_responses {
            self.read_one_frame_discard()?;
        }
        Ok(())
    }

    fn write_all(&mut self, data: &[u8]) -> Result<(), String> {
        match &mut self.link {
            BenchLink::Plain(r) => r
                .get_mut()
                .write_all(data)
                .map_err(|e| format!("write failed: {e}")),
            BenchLink::Tls(r) => r
                .get_mut()
                .write_all(data)
                .map_err(|e| format!("write failed: {e}")),
        }
    }

    fn read_one_frame_discard(&mut self) -> Result<(), String> {
        let mut header = [0u8; protocol::PROTO_HEADER_SIZE];
        match &mut self.link {
            BenchLink::Plain(r) => {
                r.read_exact(&mut header)
                    .map_err(|e| format!("read header failed: {e}"))?;
                let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                if len > 0 {
                    discard_n(r, len)?;
                }
            }
            BenchLink::Tls(r) => {
                r.read_exact(&mut header)
                    .map_err(|e| format!("read header failed: {e}"))?;
                let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                if len > 0 {
                    discard_n(r, len)?;
                }
            }
        }
        Ok(())
    }
}

fn discard_n<R: Read>(r: &mut R, mut n: usize) -> Result<(), String> {
    let mut scratch = [0u8; 8192];
    while n > 0 {
        let chunk = n.min(scratch.len());
        r.read_exact(&mut scratch[..chunk])
            .map_err(|e| format!("read body failed: {e}"))?;
        n -= chunk;
    }
    Ok(())
}
