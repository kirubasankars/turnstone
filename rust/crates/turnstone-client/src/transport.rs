// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustls::ClientConnection;
use rustls::StreamOwned;

use crate::error::ClientError;

const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);

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
    inner: Mutex<Option<Stream>>,
    read_timeout: Duration,
    write_timeout: Duration,
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
        Ok(Self {
            inner: Mutex::new(Some(Stream::Plain(stream))),
            read_timeout,
            write_timeout,
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
        let server_name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|_| ClientError::Connection("invalid DNS name for TLS".into()))?;
        let conn = ClientConnection::new(config, server_name)
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        let tls = Box::new(StreamOwned::new(conn, tcp));
        Ok(Self {
            inner: Mutex::new(Some(Stream::Tls(tls))),
            read_timeout,
            write_timeout,
        })
    }

    pub fn round_trip(&self, frame: &[u8]) -> Result<Vec<u8>, ClientError> {
        let mut guard = self.inner.lock().unwrap();
        let Some(mut stream) = guard.take() else {
            return Err(ClientError::Connection("connection closed".into()));
        };

        let result =
            round_trip_on_stream(&mut stream, frame, self.read_timeout, self.write_timeout);

        match result {
            Ok(body) => {
                *guard = Some(stream);
                Ok(body)
            }
            Err(e) => Err(e),
        }
    }

    pub fn close(&self) {
        let mut guard = self.inner.lock().unwrap();
        *guard = None;
    }
}

fn round_trip_on_stream(
    stream: &mut Stream,
    frame: &[u8],
    read_timeout: Duration,
    write_timeout: Duration,
) -> Result<Vec<u8>, ClientError> {
    set_write_timeout(stream, write_timeout)?;
    stream
        .write_all(frame)
        .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
    stream
        .flush()
        .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;

    set_read_timeout(stream, read_timeout)?;
    let mut header = [0u8; turnstone_protocol::PROTO_HEADER_SIZE];
    stream
        .read_exact(&mut header)
        .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
    let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        stream
            .read_exact(&mut body)
            .map_err(|e| ClientError::Connection(format!("I/O failed: {e}")))?;
    }
    let status = header[0];
    crate::error::map_status(status, &body)?;
    Ok(body)
}

fn set_write_timeout(stream: &mut Stream, timeout: Duration) -> Result<(), ClientError> {
    if timeout.is_zero() {
        return Ok(());
    }
    let tcp = match stream {
        Stream::Plain(s) => s,
        Stream::Tls(s) => s.get_mut(),
    };
    tcp.set_write_timeout(Some(timeout))
        .map_err(|e| ClientError::Connection(e.to_string()))
}

fn set_read_timeout(stream: &mut Stream, timeout: Duration) -> Result<(), ClientError> {
    if timeout.is_zero() {
        return Ok(());
    }
    let tcp = match stream {
        Stream::Plain(s) => s,
        Stream::Tls(s) => s.get_mut(),
    };
    tcp.set_read_timeout(Some(timeout))
        .map_err(|e| ClientError::Connection(e.to_string()))
}

pub fn default_io_timeout() -> Duration {
    DEFAULT_IO_TIMEOUT
}
