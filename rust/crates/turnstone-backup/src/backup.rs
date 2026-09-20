// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::meta::{
    load_meta, resolve_wal_file, save_meta, Meta, MetaError, TypeDifferential, TypeFull,
    DEFAULT_WAL_FILE,
};
use crate::stream::{stream_log_range, StreamError, StreamOptions};

pub struct BackupOptions {
    pub host: String,
    pub db_name: String,
    pub out_dir: String,
    pub file: String,
    pub ty: String,
    pub from_lsn: u64,
    pub base_meta_path: String,
    pub compress: bool,
    pub wait_idle: Duration,
    pub tls: Option<Arc<rustls::ClientConfig>>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("{0}")]
    Invalid(String),
}

pub fn run_backup(opts: BackupOptions) -> Result<Meta, BackupError> {
    if opts.tls.is_none() {
        return Err(BackupError::Invalid("TLS config is required".into()));
    }
    if opts.ty != TypeFull && opts.ty != TypeDifferential {
        return Err(BackupError::Invalid(format!(
            "invalid backup type {:?} (want full or differential)",
            opts.ty
        )));
    }
    let file = if opts.file.is_empty() {
        DEFAULT_WAL_FILE.to_string()
    } else {
        opts.file
    };

    let mut start_lsn = 0u64;
    let mut parent_sha = String::new();
    if opts.ty == TypeDifferential {
        if opts.from_lsn > 0 {
            start_lsn = opts.from_lsn;
        } else if !opts.base_meta_path.is_empty() {
            let base = load_meta(&opts.base_meta_path)?;
            if base.database != opts.db_name {
                return Err(BackupError::Invalid(format!(
                    "base meta database {:?} does not match db {:?}",
                    base.database, opts.db_name
                )));
            }
            start_lsn = base.end_lsn;
            parent_sha = base.sha256;
        } else {
            return Err(BackupError::Invalid(
                "differential backup requires FromLSN or BaseMetaPath".into(),
            ));
        }
    }

    std::fs::create_dir_all(&opts.out_dir)?;
    let out_path = resolve_wal_file(&opts.out_dir, &file, opts.compress);
    let mut f = std::fs::File::create(&out_path)?;

    let mut hasher = Sha256::new();
    let mut disk_writer = TeeWriter {
        inner: &mut f,
        hash: &mut hasher,
    };

    let stream_res = stream_log_range(
        &StreamOptions {
            host: opts.host,
            db_name: opts.db_name.clone(),
            start_lsn,
            wait_idle: opts.wait_idle,
            client_id: String::new(),
            tls: opts.tls,
        },
        &mut disk_writer,
    )?;

    f.flush()?;

    let meta = Meta {
        timestamp: OffsetDateTime::now_utc(),
        database: opts.db_name,
        ty: opts.ty,
        base_lsn: stream_res.base_lsn,
        end_lsn: stream_res.end_lsn,
        parent_sha256: parent_sha,
        compressed: opts.compress,
        sha256: hex::encode(hasher.finalize()),
    };
    save_meta(&opts.out_dir, &meta)?;
    Ok(meta)
}

struct TeeWriter<'a, W: Write, H: Write> {
    inner: &'a mut W,
    hash: &'a mut H,
}

impl<W: Write, H: Write> Write for TeeWriter<'_, W, H> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hash.write_all(buf)?;
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
