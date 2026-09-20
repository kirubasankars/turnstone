// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustls::ClientConnection;
use rustls::StreamOwned;

use crate::error::ClientError;

const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);
const READ_BUF_CAPACITY: usize = 256 * 1024;

enum Stream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

pub struct Transport {
    inner: Mutex<Option<BufReader<Stream>>>,
}

impl Transport {
    pub fn connect_plain(
        address: &str,
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, ClientError> {
        let addr = address
            .to_socket_addrs()
            .map_err(|e| ClientError::Connection(e.to_string()))?
            .next()
            .ok_or_else(|| ClientError::Connection("no addresses resolved".into()))?;
        let stream = TcpStream::connect_timeout(&addr, connect_timeout)
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        let _ = stream.set_nodelay(true);
        let mut io = BufReader::with_capacity(READ_BUF_CAPACITY, Stream::Plain(stream));
        apply_io_timeouts(io.get_mut(), read_timeout, write_timeout)?;
        Ok(Self {
            inner: Mutex::new(Some(io)),
        })
    }

    pub fn connect_tls(
        address: &str,
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
        config: Arc<rustls::ClientConfig>,
    ) -> Result<Self, ClientError> {
        let host = address.split(':').next().unwrap_or(address).to_string();
        let addr = address
            .to_socket_addrs()
            .map_err(|e| ClientError::Connection(e.to_string()))?
            .next()
            .ok_or_else(|| ClientError::Connection("no addresses resolved".into()))?;
        let tcp = TcpStream::connect_timeout(&addr, connect_timeout)
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        let _ = tcp.set_nodelay(true);
        let server_name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|_| ClientError::Connection("invalid DNS name for TLS".into()))?;
        let conn = ClientConnection::new(config, server_name)
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        let tls = Box::new(StreamOwned::new(conn, tcp));
        let mut io = BufReader::with_capacity(READ_BUF_CAPACITY, Stream::Tls(tls));
        apply_io_timeouts(io.get_mut(), read_timeout, write_timeout)?;
        Ok(Self {
            inner: Mutex::new(Some(io)),
        })
    }

    pub fn round_trip(&self, frame: &[u8]) -> Result<Vec<u8>, ClientError> {
        self.write_all(frame)?;
        self.flush()?;
        let (status, body) = self.read_frame()?;
        crate::error::map_status(status, &body)?;
        Ok(body)
    }

    pub fn flush(&self) -> Result<(), ClientError> {
        let mut guard = self.inner.lock().unwrap();
        let io = guard
            .as_mut()
            .ok_or_else(|| ClientError::Connection("connection closed".into()))?;
        let stream = io.get_mut();
        finish_tls_write(stream)?;
        stream
            .flush()
            .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
        Ok(())
    }

    pub fn pipeline_exchange(
        &self,
        write_data: &[u8],
        expected_responses: usize,
    ) -> Result<(), ClientError> {
        let mut guard = self.inner.lock().unwrap();
        let io = guard
            .as_mut()
            .ok_or_else(|| ClientError::Connection("connection closed".into()))?;
        let stream = io.get_mut();
        drive_client_handshake(stream)?;
        stream
            .write_all(write_data)
            .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
        finish_tls_write(stream)?;

        let mut header = [0u8; turnstone_protocol::PROTO_HEADER_SIZE];
        for _ in 0..expected_responses {
            io.read_exact(&mut header)
                .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
            let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
            if len > 0 {
                discard_exact_buf(io, len)?;
            }
        }
        Ok(())
    }

    pub fn write_all(&self, data: &[u8]) -> Result<(), ClientError> {
        let mut guard = self.inner.lock().unwrap();
        let io = guard
            .as_mut()
            .ok_or_else(|| ClientError::Connection("connection closed".into()))?;
        let stream = io.get_mut();
        drive_client_handshake(stream)?;
        stream
            .write_all(data)
            .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
        Ok(())
    }

    pub fn read_frame(&self) -> Result<(u8, Vec<u8>), ClientError> {
        let mut guard = self.inner.lock().unwrap();
        let io = guard
            .as_mut()
            .ok_or_else(|| ClientError::Connection("connection closed".into()))?;
        read_frame_buffered(io)
    }

    pub fn close(&self) {
        let mut guard = self.inner.lock().unwrap();
        *guard = None;
    }
}

fn finish_tls_write(stream: &mut Stream) -> Result<(), ClientError> {
    if let Stream::Tls(s) = stream {
        while s.conn.wants_write() {
            s.conn
                .write_tls(&mut s.sock)
                .map_err(|e| ClientError::Connection(e.to_string()))?;
        }
    }
    Ok(())
}

fn discard_exact_buf<R: Read>(reader: &mut R, mut n: usize) -> Result<(), ClientError> {
    let mut scratch = [0u8; 8192];
    while n > 0 {
        let chunk = n.min(scratch.len());
        reader
            .read_exact(&mut scratch[..chunk])
            .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
        n -= chunk;
    }
    Ok(())
}

fn drive_client_handshake(stream: &mut Stream) -> Result<(), ClientError> {
    match stream {
        Stream::Tls(s) => {
            while s.conn.is_handshaking() {
                s.conn
                    .complete_io(&mut s.sock)
                    .map_err(|e| ClientError::Connection(e.to_string()))?;
            }
        }
        Stream::Plain(_) => {}
    }
    Ok(())
}

fn read_frame_buffered(io: &mut BufReader<Stream>) -> Result<(u8, Vec<u8>), ClientError> {
    let mut header = [0u8; turnstone_protocol::PROTO_HEADER_SIZE];
    io.read_exact(&mut header)
        .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
    let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        io.read_exact(&mut body)
            .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
    }
    Ok((header[0], body))
}

fn apply_io_timeouts(
    stream: &mut Stream,
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<(), ClientError> {
    let tcp = match stream {
        Stream::Plain(s) => s,
        Stream::Tls(s) => s.get_mut(),
    };
    if !read_timeout.is_zero() {
        tcp.set_read_timeout(Some(read_timeout))
            .map_err(|e| ClientError::Connection(e.to_string()))?;
    }
    if !write_timeout.is_zero() {
        tcp.set_write_timeout(Some(write_timeout))
            .map_err(|e| ClientError::Connection(e.to_string()))?;
    }
    Ok(())
}

pub fn default_io_timeout() -> Duration {
    DEFAULT_IO_TIMEOUT
}
