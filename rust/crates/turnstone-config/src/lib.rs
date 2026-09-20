// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, SanType,
};
use rcgen::Ia5String;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Server configuration (JSON field names match the Go `config.Config` struct).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub id: String,
    pub port: String,
    pub debug: bool,
    pub max_conns: i32,
    #[serde(default)]
    pub tls_cert_file: String,
    #[serde(default)]
    pub tls_key_file: String,
    #[serde(default)]
    pub tls_ca_file: String,
    #[serde(default)]
    pub tls_client_cert_file: String,
    #[serde(default)]
    pub tls_client_key_file: String,
    #[serde(default)]
    pub metrics_addr: String,
    pub number_of_databases: i32,
    #[serde(default)]
    pub log_retention: String,
    pub max_disk_usage_percent: i32,
    pub max_index_arena_bytes: i64,
    pub shared_buffers_bytes: i64,
    pub value_cache_bytes: i64,
    pub mlock: bool,
}

/// Number of isolated keyspaces `0 … N-1`. `n <= 0` is treated as a single database.
pub fn database_count(n: i32) -> i32 {
    if n <= 0 {
        1
    } else {
        n
    }
}

/// Splits an instance-wide byte budget across `n` databases.
pub fn share_bytes(total: i64, n: i32) -> i64 {
    let mut count = n;
    if count < 1 {
        count = 1;
    }
    if total < 0 {
        return total;
    }
    let per = total / i64::from(count);
    if per == 0 && total > 0 {
        1
    } else {
        per
    }
}

/// Returns an absolute path, joining `path` under `home_dir` when it is relative.
pub fn resolve_path(home_dir: impl AsRef<Path>, path: impl AsRef<Path>) -> PathBuf {
    let home_dir = home_dir.as_ref();
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return home_dir.to_path_buf();
    }
    if path.is_absolute() {
        return path.to_path_buf();
    }
    home_dir.join(path)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidateError {
    #[error(
        "max_disk_usage_percent must be between 0 and 100 (0 disables the check), got {0}"
    )]
    MaxDiskUsagePercent(i32),
    #[error("max_index_arena_bytes must be >= 0 (0 disables the check), got {0}")]
    MaxIndexArenaBytes(i64),
}

/// Sanity-checks user-supplied config values (matches Go `ValidateConfig`).
pub fn validate_config(cfg: &Config) -> Result<(), ValidateError> {
    if cfg.max_disk_usage_percent < 0 || cfg.max_disk_usage_percent > 100 {
        return Err(ValidateError::MaxDiskUsagePercent(cfg.max_disk_usage_percent));
    }
    if cfg.max_index_arena_bytes < 0 {
        return Err(ValidateError::MaxIndexArenaBytes(cfg.max_index_arena_bytes));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("error creating home directory: {0}")]
    HomeDir(#[source] std::io::Error),
    #[error("failed to create {dir} directory: {source}")]
    SubDir {
        dir: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create data directory for database {db_id}: {source}")]
    DataDir {
        db_id: String,
        #[source]
        source: std::io::Error,
    },
    #[error("error generating certs: {0}")]
    Certs(#[source] rcgen::Error),
    #[error("error generating config json: {0}")]
    Json(#[source] serde_json::Error),
    #[error("error writing config file: {0}")]
    WriteConfig(#[source] std::io::Error),
    #[error("error writing certificate file: {0}")]
    WriteCert(#[source] std::io::Error),
}

/// Creates a sample directory structure, TLS certificates, and `turnstone.json`-compatible config.
pub fn generate_config_artifacts(
    home_dir: impl AsRef<Path>,
    mut default_cfg: Config,
    config_path: impl AsRef<Path>,
    extra_hosts: &[&str],
) -> Result<(), GenerateError> {
    let home_dir = home_dir.as_ref();
    let config_path = config_path.as_ref();

    fs::create_dir_all(home_dir).map_err(GenerateError::HomeDir)?;

    for d in ["certs"] {
        fs::create_dir_all(resolve_path(home_dir, d)).map_err(|source| GenerateError::SubDir {
            dir: d.to_string(),
            source,
        })?;
    }

    let db_count = database_count(default_cfg.number_of_databases);
    for i in 0..db_count {
        let db_id = i.to_string();
        let db_path = home_dir.join("data").join(&db_id);
        fs::create_dir_all(&db_path).map_err(|source| GenerateError::DataDir {
            db_id,
            source,
        })?;
    }

    let certs_dir = resolve_path(home_dir, &default_cfg.tls_cert_file)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| home_dir.join("certs"));

    generate_certs(&certs_dir, extra_hosts).map_err(GenerateError::Certs)?;
    eprintln!("Certificates generated in: {}", certs_dir.display());

    default_cfg.tls_client_cert_file = "certs/server.crt".to_string();
    default_cfg.tls_client_key_file = "certs/server.key".to_string();

    if default_cfg.log_retention.is_empty() {
        default_cfg.log_retention = "replication".to_string();
    }
    if default_cfg.max_disk_usage_percent == 0 {
        default_cfg.max_disk_usage_percent = 90;
    }
    if default_cfg.id.is_empty() {
        let hostname = std::fs::read_to_string("/etc/hostname")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("HOSTNAME")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| "server".to_string());
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        default_cfg.id = format!("{hostname}-{ts}");
    }

    let data = serde_json::to_string_pretty(&default_cfg).map_err(GenerateError::Json)?;
    fs::write(config_path, data).map_err(GenerateError::WriteConfig)?;
    eprintln!(
        "Sample configuration written to {}",
        config_path.display()
    );
    Ok(())
}

fn generate_certs(out_dir: &Path, extra_hosts: &[&str]) -> Result<(), rcgen::Error> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::OrganizationName, "TurnstoneDB CA");
    ca_params.not_before = OffsetDateTime::now_utc();
    ca_params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365 * 10);
    let ca_cert = ca_params.self_signed(&ca_key)?;
    write_file(&out_dir.join("ca.crt"), &ca_cert.pem(), 0o644)?;

    let mut server_hosts = vec!["localhost"];
    server_hosts.extend(extra_hosts.iter().copied());

    gen_leaf(
        out_dir,
        "server",
        "TurnstoneDB server",
        &server_hosts,
        &ca_cert,
        &ca_key,
    )?;
    gen_leaf(out_dir, "client", "TurnstoneDB client", &[], &ca_cert, &ca_key)?;
    gen_leaf(out_dir, "admin", "TurnstoneDB admin", &[], &ca_cert, &ca_key)?;
    Ok(())
}

fn gen_leaf(
    out_dir: &Path,
    role: &str,
    org: &str,
    hosts: &[&str],
    ca_cert: &Certificate,
    ca_key: &KeyPair,
) -> Result<(), rcgen::Error> {
    let leaf_key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::OrganizationName, org);
    params.distinguished_name.push(DnType::CommonName, role);
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365);
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];

    params.subject_alt_names.push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    params
        .subject_alt_names
        .push(SanType::IpAddress("::1".parse().unwrap()));

    for h in hosts {
        if let Ok(ip) = h.parse::<IpAddr>() {
            params.subject_alt_names.push(SanType::IpAddress(ip));
        } else {
            params
                .subject_alt_names
                .push(SanType::DnsName(Ia5String::try_from(*h).map_err(|_| rcgen::Error::RingUnspecified)?));
        }
    }

    let cert = params.signed_by(&leaf_key, ca_cert, ca_key)?;
    write_file(&out_dir.join(format!("{role}.crt")), &cert.pem(), 0o644)?;
    write_file(
        &out_dir.join(format!("{role}.key")),
        &leaf_key.serialize_pem(),
        0o600,
    )?;
    Ok(())
}

fn write_file(path: &Path, contents: &str, mode: u32) -> Result<(), rcgen::Error> {
    use std::fs::OpenOptions;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    let mut opts = OpenOptions::new();
    opts.create(true).write(true).truncate(true);
    #[cfg(unix)]
    opts.mode(mode);
    opts.open(path)
        .and_then(|mut f| f.write_all(contents.as_bytes()))
        .map_err(|_| rcgen::Error::RingUnspecified)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;
    use x509_parser::pem::parse_x509_pem;

    #[test]
    fn resolve_path_cases() {
        let home = PathBuf::from("/app/home");
        assert_eq!(resolve_path(&home, ""), home);
        assert_eq!(resolve_path(&home, "/etc/config"), PathBuf::from("/etc/config"));
        assert_eq!(
            resolve_path(&home, "data/db"),
            PathBuf::from("/app/home/data/db")
        );
        assert_eq!(resolve_path(&home, "."), home);
    }

    #[test]
    fn generate_config_artifacts_layout() {
        let tmp = tempdir().unwrap();
        let config_path = tmp.path().join("config.json");
        let default_cfg = Config {
            port: ":9999".to_string(),
            max_conns: 1000,
            tls_cert_file: "certs/server.crt".to_string(),
            tls_key_file: "certs/server.key".to_string(),
            tls_ca_file: "certs/ca.crt".to_string(),
            number_of_databases: 1,
            ..Default::default()
        };

        generate_config_artifacts(tmp.path(), default_cfg, &config_path, &[]).unwrap();

        assert!(tmp.path().join("data/0").is_dir());
        assert!(!tmp.path().join("data/1").exists());
        assert!(tmp.path().join("certs").is_dir());

        let data = fs::read_to_string(&config_path).unwrap();
        let loaded: Config = serde_json::from_str(&data).unwrap();
        assert_eq!(loaded.port, ":9999");
        assert_eq!(loaded.number_of_databases, 1);
        assert_eq!(loaded.max_conns, 1000);

        for f in [
            "certs/ca.crt",
            "certs/server.crt",
            "certs/server.key",
            "certs/client.crt",
            "certs/client.key",
            "certs/admin.crt",
            "certs/admin.key",
        ] {
            let path = tmp.path().join(f);
            let meta = fs::metadata(&path).unwrap_or_else(|_| panic!("missing {f}"));
            assert!(meta.len() > 0, "{f} empty");
            if f.ends_with(".key") {
                assert_eq!(
                    meta.permissions().mode() & 0o777,
                    0o600,
                    "key permissions for {f}"
                );
            }
        }
    }

    #[test]
    fn rbac_certificates() {
        let tmp = tempdir().unwrap();
        let config_path = tmp.path().join("config.json");
        let default_cfg = Config {
            tls_cert_file: "certs/server.crt".to_string(),
            tls_key_file: "certs/server.key".to_string(),
            tls_ca_file: "certs/ca.crt".to_string(),
            ..Default::default()
        };
        generate_config_artifacts(tmp.path(), default_cfg, &config_path, &[]).unwrap();

        let cases = [
            ("certs/client.crt", "TurnstoneDB client"),
            ("certs/admin.crt", "TurnstoneDB admin"),
            ("certs/server.crt", "TurnstoneDB server"),
        ];
        for (file, want_org) in cases {
            let pem = fs::read(tmp.path().join(file)).unwrap();
            let (_, rest) = parse_x509_pem(&pem).unwrap();
            let cert = rest.parse_x509().unwrap();
            let org = cert
                .subject()
                .iter_organization()
                .next()
                .and_then(|o| o.as_str().ok())
                .unwrap_or("");
            assert_eq!(org, want_org, "org in {file}");
            if !file.contains("server.crt") {
                let has_client_auth = cert
                    .extended_key_usage()
                    .ok()
                    .flatten()
                    .map(|eku| eku.value.client_auth)
                    .unwrap_or(false);
                assert!(has_client_auth, "{file} missing client auth EKU");
            }
        }
    }

    #[test]
    fn validate_config_cases() {
        let cases: &[(&str, i32, i64, bool)] = &[
            ("disabled", 0, 0, false),
            ("typical", 90, 0, false),
            ("max", 100, 0, false),
            ("negative", -5, 0, true),
            ("over100", 150, 0, true),
            ("negative index arena bytes", 0, -1, true),
            ("disabled index arena limit", 0, 0, false),
        ];
        for (name, pct, arena, want_err) in cases {
            let err = validate_config(&Config {
                max_disk_usage_percent: *pct,
                max_index_arena_bytes: *arena,
                ..Default::default()
            });
            if *want_err {
                assert!(err.is_err(), "{name}: expected error");
            } else {
                assert!(err.is_ok(), "{name}: unexpected {err:?}");
            }
        }
    }

    #[test]
    fn database_count_and_share_bytes() {
        assert_eq!(database_count(4), 4);
        assert_eq!(database_count(0), 1);
        assert_eq!(database_count(-1), 1);
        assert_eq!(share_bytes(64 << 20, 4), 16 << 20);
        assert_eq!(share_bytes(-1, 4), -1);
        assert_eq!(share_bytes(3, 4), 1);
    }
}
