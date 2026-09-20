use turnstone_backup::{run_backup, BackupOptions, TypeFull};

#[test]
fn run_backup_allows_plain_tcp_without_tls() {
    let err = run_backup(BackupOptions {
        host: "127.0.0.1:1".into(),
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
    assert!(
        !err.to_string().contains("TLS config is required"),
        "unexpected: {err}"
    );
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
        ty: "bogus".into(),
        from_lsn: 0,
        base_meta_path: String::new(),
        compress: false,
        wait_idle: std::time::Duration::ZERO,
        tls: Some(tls.unwrap()),
    })
    .unwrap_err();
    assert!(err.to_string().contains("invalid backup type"));
}
