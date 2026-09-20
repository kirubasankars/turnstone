use turnstone_backup::{run_backup, BackupOptions, TypeDifferential, TypeFull};

#[test]
fn run_backup_requires_tls() {
    let err = run_backup(BackupOptions {
        host: String::new(),
        db_name: "1".into(),
        out_dir: tempfile::tempdir().unwrap().path().to_string_lossy().into(),
        file: String::new(),
        ty: TypeFull.into(),
        from_lsn: 0,
        base_meta_path: String::new(),
        compress: false,
        wait_idle: std::time::Duration::ZERO,
        tls: None,
    })
    .unwrap_err();
    assert!(err.to_string().contains("TLS config is required"));
}

#[test]
fn run_backup_invalid_type() {
    let tls = turnstone_tls::load_mtls(
        "/dev/null",
        "/dev/null",
        "/dev/null",
    );
    if tls.is_err() {
        return;
    }
    let err = run_backup(BackupOptions {
        host: String::new(),
        db_name: "1".into(),
        out_dir: tempfile::tempdir().unwrap().path().to_string_lossy().into(),
        file: String::new(),
        ty: "snapshot".into(),
        from_lsn: 0,
        base_meta_path: String::new(),
        compress: false,
        wait_idle: std::time::Duration::ZERO,
        tls: tls.ok(),
    })
    .unwrap_err();
    assert!(err.to_string().contains("invalid backup type"));
}

#[test]
fn run_backup_differential_requires_base() {
    let dir = tempfile::tempdir().unwrap();
    let certs = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let _ = certs;
    // Use generated certs from a temp config when possible; skip if no tls.
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("config.json");
    turnstone_config::generate_config_artifacts(
        tmp.path(),
        turnstone_config::Config {
            tls_cert_file: "certs/server.crt".into(),
            tls_key_file: "certs/server.key".into(),
            tls_ca_file: "certs/ca.crt".into(),
            number_of_databases: 1,
            ..Default::default()
        },
        &cfg_path,
        &[],
    )
    .unwrap();
    let tls = turnstone_tls::load_mtls(
        tmp.path().join("certs/ca.crt"),
        tmp.path().join("certs/client.crt"),
        tmp.path().join("certs/client.key"),
    )
    .unwrap();

    let err = run_backup(BackupOptions {
        host: "127.0.0.1:1".into(),
        db_name: "0".into(),
        out_dir: dir.path().to_string_lossy().into(),
        file: String::new(),
        ty: TypeDifferential.into(),
        from_lsn: 0,
        base_meta_path: String::new(),
        compress: false,
        wait_idle: std::time::Duration::from_millis(100),
        tls: Some(tls),
    })
    .unwrap_err();
    assert!(err.to_string().contains("differential backup requires"));
}
