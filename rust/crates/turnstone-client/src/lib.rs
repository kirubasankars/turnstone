// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

mod error;
mod pipeline;
mod transport;

pub use error::ClientError;
pub use pipeline::PipelineResponse;
pub use transport::{default_io_timeout, Transport};

use std::sync::Arc;
use std::time::Duration;

use turnstone_protocol as protocol;
use turnstone_tls::{load_mtls, Role};

pub struct ClientConfig {
    pub address: String,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub tls: Option<Arc<rustls::ClientConfig>>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            connect_timeout: Duration::from_secs(5),
            read_timeout: default_io_timeout(),
            write_timeout: default_io_timeout(),
            tls: None,
        }
    }
}

pub struct Client {
    transport: Transport,
}

impl Client {
    pub fn connect(cfg: ClientConfig) -> Result<Self, ClientError> {
        let transport = if let Some(tls) = cfg.tls {
            Transport::connect_tls(
                &cfg.address,
                cfg.connect_timeout,
                cfg.read_timeout,
                cfg.write_timeout,
                tls,
            )?
        } else {
            Transport::connect_plain(
                &cfg.address,
                cfg.connect_timeout,
                cfg.read_timeout,
                cfg.write_timeout,
            )?
        };
        Ok(Self { transport })
    }

    pub fn from_mtls_files(
        address: impl Into<String>,
        ca_file: impl AsRef<std::path::Path>,
        cert_file: impl AsRef<std::path::Path>,
        key_file: impl AsRef<std::path::Path>,
    ) -> Result<Self, ClientError> {
        let tls = load_mtls(ca_file, cert_file, key_file)?;
        Self::connect(ClientConfig {
            address: address.into(),
            tls: Some(tls),
            ..ClientConfig::default()
        })
    }

    pub fn from_home(
        address: impl Into<String>,
        home: impl AsRef<std::path::Path>,
        role: Role,
    ) -> Result<Self, ClientError> {
        let (ca, cert, key) = turnstone_tls::cert_paths(home, role);
        Self::from_mtls_files(address, ca, cert, key)
    }

    pub fn close(self) {
        self.transport.close();
    }

    fn round_trip(&self, op: u8, payload: &[u8]) -> Result<Vec<u8>, ClientError> {
        let frame = protocol::encode_frame(op, payload);
        self.transport.round_trip(&frame)
    }

    /// Sends one or more concatenated request frames without reading responses.
    pub fn write_raw(&self, data: &[u8]) -> Result<(), ClientError> {
        self.transport.write_all(data)
    }

    /// Reads the next response frame from the server.
    pub fn read_response(&self) -> Result<PipelineResponse, ClientError> {
        pipeline::read_response(&self.transport)
    }

    pub fn ping(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_PING, &[])?;
        Ok(())
    }

    pub fn select_db(&self, db_name: &str) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_SELECT, db_name.as_bytes())?;
        Ok(())
    }

    pub fn replica_of(&self, source_addr: &str, source_db: &str) -> Result<(), ClientError> {
        let payload = protocol::encode_replica_of(source_addr, source_db);
        self.round_trip(protocol::OP_REPLICA_OF, &payload)?;
        Ok(())
    }

    pub fn promote(&self, min_replicas: i32) -> Result<(), ClientError> {
        if min_replicas < 0 {
            return Err(ClientError::InvalidMinReplicas(min_replicas));
        }
        let payload = protocol::encode_promote(min_replicas as u32);
        self.round_trip(protocol::OP_PROMOTE, &payload)?;
        Ok(())
    }

    pub fn step_down(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_STEP_DOWN, &[])?;
        Ok(())
    }

    pub fn flush_db(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_FLUSH_DB, &[])?;
        Ok(())
    }

    pub fn stat(&self) -> Result<Vec<u8>, ClientError> {
        self.round_trip(protocol::OP_STAT, &[])
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>, ClientError> {
        if !protocol::is_ascii(key) {
            return Err(ClientError::InvalidKey);
        }
        self.round_trip(protocol::OP_GET, key.as_bytes())
    }

    pub fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, ClientError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let payload = protocol::encode_mget_keys(keys)?;
        let body = self.round_trip(protocol::OP_MGET, &payload)?;
        protocol::decode_mget_response(&body, keys.len()).map_err(ClientError::from)
    }

    pub fn set(&self, key: &str, value: &[u8]) -> Result<(), ClientError> {
        let payload = protocol::encode_set(key, value)?;
        self.round_trip(protocol::OP_SET, &payload)?;
        Ok(())
    }

    pub fn mset(&self, entries: &[(&str, &[u8])]) -> Result<(), ClientError> {
        if entries.is_empty() {
            return Ok(());
        }
        let payload = protocol::encode_mset(entries)?;
        self.round_trip(protocol::OP_MSET, &payload)?;
        Ok(())
    }

    pub fn del(&self, key: &str) -> Result<(), ClientError> {
        if !protocol::is_ascii(key) {
            return Err(ClientError::InvalidKey);
        }
        self.round_trip(protocol::OP_DEL, key.as_bytes())?;
        Ok(())
    }

    pub fn mdel(&self, keys: &[&str]) -> Result<u32, ClientError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let payload = protocol::encode_mdel_keys(keys)?;
        let body = self.round_trip(protocol::OP_MDEL, &payload)?;
        protocol::decode_mdel_count(&body).map_err(ClientError::from)
    }

    pub fn begin(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_BEGIN, &[])?;
        Ok(())
    }

    pub fn begin_read_only(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_BEGIN, &[protocol::BEGIN_READ_ONLY])?;
        Ok(())
    }

    pub fn commit(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_COMMIT, &[])?;
        Ok(())
    }

    pub fn abort(&self) -> Result<(), ClientError> {
        self.round_trip(protocol::OP_ABORT, &[])?;
        Ok(())
    }
}
