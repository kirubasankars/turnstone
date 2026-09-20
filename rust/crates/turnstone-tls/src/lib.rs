// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    Admin,
}

impl Role {
    fn file_stem(self) -> &'static str {
        match self {
            Role::Client => "client",
            Role::Admin => "admin",
        }
    }
}

pub fn cert_paths(home: impl AsRef<Path>, role: Role) -> (PathBuf, PathBuf, PathBuf) {
    let certs = home.as_ref().join("certs");
    (
        certs.join("ca.crt"),
        certs.join(format!("{}.crt", role.file_stem())),
        certs.join(format!("{}.key", role.file_stem())),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse CA certificate PEM from {0}")]
    BadCa(PathBuf),
    #[error("no certificates found in {0}")]
    NoCerts(PathBuf),
    #[error("no private keys found in {0}")]
    NoKeys(PathBuf),
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("invalid private key in {0}")]
    BadKey(PathBuf),
}

pub fn load_mtls(
    ca_file: impl AsRef<Path>,
    cert_file: impl AsRef<Path>,
    key_file: impl AsRef<Path>,
) -> Result<Arc<ClientConfig>, TlsError> {
    let ca_file = ca_file.as_ref();
    let cert_file = cert_file.as_ref();
    let key_file = key_file.as_ref();

    let mut roots = RootCertStore::empty();
    let ca_pem = read_file(ca_file)?;
    let mut ca_reader = BufReader::new(ca_pem.as_slice());
    for cert in rustls_pemfile::certs(&mut ca_reader).flatten() {
        roots.add(cert).map_err(TlsError::Rustls)?;
    }
    if roots.is_empty() {
        return Err(TlsError::BadCa(ca_file.to_path_buf()));
    }

    let cert_chain = read_certs(cert_file)?;
    let key = read_private_key(key_file)?;

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, key)
        .map_err(TlsError::Rustls)?;

    Ok(Arc::new(config))
}

pub fn load_server_mtls(
    ca_file: impl AsRef<Path>,
    cert_file: impl AsRef<Path>,
    key_file: impl AsRef<Path>,
) -> Result<Arc<ServerConfig>, TlsError> {
    let ca_file = ca_file.as_ref();
    let cert_file = cert_file.as_ref();
    let key_file = key_file.as_ref();

    let mut roots = RootCertStore::empty();
    let ca_pem = read_file(ca_file)?;
    let mut ca_reader = BufReader::new(ca_pem.as_slice());
    for cert in rustls_pemfile::certs(&mut ca_reader).flatten() {
        roots.add(cert).map_err(TlsError::Rustls)?;
    }
    if roots.is_empty() {
        return Err(TlsError::BadCa(ca_file.to_path_buf()));
    }

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| TlsError::Rustls(rustls::Error::General(e.to_string())))?;

    let cert_chain = read_certs(cert_file)?;
    let key = read_private_key(key_file)?;

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, key)
        .map_err(TlsError::Rustls)?;

    Ok(Arc::new(config))
}

pub fn load_from_home(home: impl AsRef<Path>, role: Role) -> Result<Arc<ClientConfig>, TlsError> {
    let (ca, cert, key) = cert_paths(home, role);
    load_mtls(ca, cert, key)
}

fn read_file(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn read_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let pem = read_file(path)?;
    let mut reader = BufReader::new(pem.as_slice());
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        return Err(TlsError::NoCerts(path.to_path_buf()));
    }
    Ok(certs)
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let pem = read_file(path)?;
    let mut reader = BufReader::new(pem.as_slice());
    if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .filter_map(Result::ok)
        .next()
    {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }
    let pem = read_file(path)?;
    let mut reader = BufReader::new(pem.as_slice());
    if let Some(key) = rustls_pemfile::rsa_private_keys(&mut reader)
        .filter_map(Result::ok)
        .next()
    {
        return Ok(PrivateKeyDer::Pkcs1(key));
    }
    Err(TlsError::BadKey(path.to_path_buf()))
}
