// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::time::Duration;

use turnstone_backup::{
    resolve_restore_chain, resolve_wal_file, run_backup, run_restore, BackupOptions,
    RestoreOptions, TypeDifferential, TypeFull, DEFAULT_WAL_FILE,
};
use turnstone_tls::{cert_paths, load_mtls, Role};

#[derive(Debug, Clone)]
pub struct BackupCliOptions {
    pub home: std::path::PathBuf,
    pub host: String,
    pub db: String,
    pub out_dir: String,
    pub file: String,
    pub ty: String,
    pub from_lsn: u64,
    pub base_meta: String,
    pub compress: bool,
    pub wait_idle: Duration,
}

pub fn run_backup_cmd(opts: BackupCliOptions) -> Result<(), String> {
    let (ca, cert, key) = cert_paths(&opts.home, Role::Admin);
    let tls = if ca.exists() {
        Some(load_mtls(ca, cert, key).map_err(|e| e.to_string())?)
    } else {
        None
    };
    let meta = run_backup(BackupOptions {
        host: opts.host,
        db_name: opts.db,
        out_dir: opts.out_dir.clone(),
        file: if opts.file.is_empty() {
            DEFAULT_WAL_FILE.to_string()
        } else {
            opts.file.clone()
        },
        ty: if opts.ty == TypeDifferential {
            TypeDifferential.to_string()
        } else {
            TypeFull.to_string()
        },
        from_lsn: opts.from_lsn,
        base_meta_path: opts.base_meta,
        compress: opts.compress,
        wait_idle: opts.wait_idle,
        tls,
    })
    .map_err(|e| e.to_string())?;
    let path = resolve_wal_file(&opts.out_dir, &opts.file, meta.compressed);
    println!("Backup successful");
    println!("Location: {}", path.display());
    println!("LSN range: [{}, {})", meta.base_lsn, meta.end_lsn);
    println!("Checksum: {}", meta.sha256);
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RestoreCliOptions {
    pub in_dir: String,
    pub out_home: String,
    pub file: String,
    pub verify: bool,
    pub chain: String,
}

pub fn run_restore_cmd(opts: RestoreCliOptions) -> Result<(), String> {
    let dirs = resolve_restore_chain(&opts.in_dir, &opts.chain).map_err(|e| e.to_string())?;
    let backup_dirs: Vec<String> = dirs
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    let meta = run_restore(RestoreOptions {
        backup_dirs,
        out_home: opts.out_home.clone(),
        file: if opts.file.is_empty() {
            DEFAULT_WAL_FILE.to_string()
        } else {
            opts.file
        },
        verify: opts.verify,
    })
    .map_err(|e| e.to_string())?;
    let db_dir = format!("{}/data/{}", opts.out_home, meta.database);
    println!(
        "Restore complete. Database at {db_dir} (end_lsn={})",
        meta.end_lsn
    );
    Ok(())
}
