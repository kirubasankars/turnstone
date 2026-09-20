// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::path::Path;

use turnstone_client::{Client, ClientConfig, ClientError};
use turnstone_tls::{cert_paths, Role};

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub host: String,
    pub home: std::path::PathBuf,
    pub admin: bool,
    pub debug: bool,
}

pub fn connect(opts: &ConnectOptions) -> Result<Client, ClientError> {
    if opts.debug {
        eprintln!(
            "connecting to {} (role={})",
            opts.host,
            if opts.admin { "admin" } else { "client" }
        );
    }
    let role = if opts.admin { Role::Admin } else { Role::Client };
    let (ca, cert, key) = cert_paths(&opts.home, role);
    if ca.exists() {
        Client::from_mtls_files(&opts.host, ca, cert, key)
    } else {
        Client::connect(ClientConfig {
            address: opts.host.clone(),
            ..ClientConfig::default()
        })
    }
}

pub fn home_exists(home: &Path) -> bool {
    home.is_dir()
}
