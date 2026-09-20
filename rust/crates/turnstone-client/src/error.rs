// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_protocol as protocol;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("key not found")]
    NotFound,
    #[error("key must contain only ASCII characters")]
    InvalidKey,
    #[error("transaction required for this operation")]
    TxRequired,
    #[error("transaction timed out")]
    TxTimeout,
    #[error("transaction conflict detected")]
    TxConflict,
    #[error("transaction already in progress")]
    TxInProgress,
    #[error("server is busy")]
    ServerBusy,
    #[error("entity too large")]
    EntityTooLarge,
    #[error("server memory limit exceeded")]
    MemoryLimit,
    #[error("server error: {0}")]
    Server(String),
    #[error("unknown server status code: 0x{status:02x}, body: {body}")]
    UnknownStatus { status: u8, body: String },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("minReplicas must be >= 0, got {0}")]
    InvalidMinReplicas(i32),
    #[error(transparent)]
    Tls(#[from] turnstone_tls::TlsError),
    #[error(transparent)]
    Payload(#[from] protocol::PayloadError),
}

pub fn map_status(status: u8, body: &[u8]) -> Result<(), ClientError> {
    match status {
        protocol::RES_OK => Ok(()),
        protocol::RES_ERR => Err(ClientError::Server(
            String::from_utf8_lossy(body).into_owned(),
        )),
        protocol::RES_NOT_FOUND => Err(ClientError::NotFound),
        protocol::RES_TX_REQUIRED => Err(ClientError::TxRequired),
        protocol::RES_TX_TIMEOUT => Err(ClientError::TxTimeout),
        protocol::RES_TX_CONFLICT => Err(ClientError::TxConflict),
        protocol::RES_TX_IN_PROGRESS => Err(ClientError::TxInProgress),
        protocol::RES_SERVER_BUSY => Err(ClientError::ServerBusy),
        protocol::RES_ENTITY_TOO_LARGE => Err(ClientError::EntityTooLarge),
        protocol::RES_MEMORY_LIMIT => Err(ClientError::MemoryLimit),
        other => Err(ClientError::UnknownStatus {
            status: other,
            body: String::from_utf8_lossy(body).into_owned(),
        }),
    }
}
