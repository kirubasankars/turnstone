use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use turnstone_client::{Client, ClientConfig};
use turnstone_config::{generate_config_artifacts, Config};
use turnstone_database::{open, OpenOptions, STATE_PRIMARY};
use turnstone_repl::Manager;
use turnstone_server::new_server;
use turnstone_tls::load_mtls;

fn setup_test_env() -> (tempfile::TempDir, HashMap<String, Arc<turnstone_database::Database>>, Arc<turnstone_server::Server>) {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.json");
    generate_config_artifacts(
        dir.path(),
        Config {
            tls_cert_file: "certs/server.crt".into(),
            tls_key_file: "certs/server.key".into(),
            tls_ca_file: "certs/ca.crt".into(),
            number_of_databases: 4,
            ..Default::default()
        },
        &config_path,
        &[],
    )
    .unwrap();

    let mut stores = HashMap::new();
    for i in 0..4 {
        let db_name = i.to_string();
        let path = dir.path().join("data").join(&db_name);
        let db = open(
            &path,
            OpenOptions {
                retention_strategy: "none".into(),
                max_disk_usage_percent: 90,
                ..Default::default()
            },
        )
        .unwrap();
        db.set_state(STATE_PRIMARY);
        stores.insert(db_name, db);
    }

    let tls = load_mtls(
        dir.path().join("certs/ca.crt"),
        dir.path().join("certs/server.crt"),
        dir.path().join("certs/server.key"),
    )
    .unwrap();
    let rm = Arc::new(Manager::new("test-server", stores.clone(), tls));

    let srv = new_server(
        "test-server",
        "127.0.0.1:0",
        stores.clone(),
        10,
        dir.path().join("certs/server.crt").to_string_lossy().into_owned(),
        dir.path().join("certs/server.key").to_string_lossy().into_owned(),
        dir.path().join("certs/ca.crt").to_string_lossy().into_owned(),
        Some(rm),
        false,
    )
    .unwrap();

    (dir, stores, srv)
}

#[test]
fn server_lifecycle_and_ping() {
    let (_dir, _stores, srv) = setup_test_env();
    let srv_run = Arc::clone(&srv);
    thread::spawn(move || {
        let _ = srv_run.run();
    });
    thread::sleep(Duration::from_millis(200));
    let addr = srv.addr().expect("listening").to_string();
    let client = Client::from_mtls_files(
        &addr,
        _dir.path().join("certs/ca.crt"),
        _dir.path().join("certs/client.crt"),
        _dir.path().join("certs/client.key"),
    )
    .unwrap();
    client.ping().unwrap();
    srv.close_all();
}

#[test]
fn server_set_get_in_tx() {
    let (dir, _stores, srv) = setup_test_env();
    let srv_run = Arc::clone(&srv);
    thread::spawn(move || {
        let _ = srv_run.run();
    });
    thread::sleep(Duration::from_millis(200));
    let addr = srv.addr().expect("listening").to_string();

    let client = Client::from_mtls_files(
        &addr,
        dir.path().join("certs/ca.crt"),
        dir.path().join("certs/client.crt"),
        dir.path().join("certs/client.key"),
    )
    .unwrap();

    client.begin().unwrap();
    client.set("mykey", b"myval").unwrap();
    client.commit().unwrap();

    client.begin().unwrap();
    let val = client.get("mykey").unwrap();
    client.commit().unwrap();
    assert_eq!(val, b"myval");

    srv.close_all();
}

#[test]
fn server_plain_tcp_without_certs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data").join("0");
    std::fs::create_dir_all(&path).unwrap();
    let db = open(
        &path,
        OpenOptions {
            retention_strategy: "none".into(),
            max_disk_usage_percent: 90,
            ..Default::default()
        },
    )
    .unwrap();
    db.set_state(STATE_PRIMARY);

    let mut stores = HashMap::new();
    stores.insert("0".into(), db);

    let missing = dir.path().join("no-such-cert.crt");
    let srv = new_server(
        "plain-test",
        "127.0.0.1:0",
        stores,
        10,
        missing.to_string_lossy().into_owned(),
        missing.to_string_lossy().into_owned(),
        missing.to_string_lossy().into_owned(),
        None,
        true,
    )
    .unwrap();
    assert!(!srv.tls_enabled());

    let srv_run = Arc::clone(&srv);
    thread::spawn(move || {
        let _ = srv_run.run();
    });
    thread::sleep(Duration::from_millis(200));
    let addr = srv.addr().expect("listening").to_string();

    let client = Client::connect(ClientConfig {
        address: addr,
        ..ClientConfig::default()
    })
    .unwrap();
    client.ping().unwrap();

    client.begin().unwrap();
    client.set("k", b"v").unwrap();
    client.commit().unwrap();

    srv.close_all();
}
