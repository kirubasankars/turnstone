// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_engine::{Db, EngineError, Options};

#[test]
fn basic_crud() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();

    {
        let mut tx = db.new_transaction(true);
        tx.put(b"user:1", b"Alice").unwrap();
        tx.put(b"user:2", b"Bob").unwrap();
        tx.commit().unwrap();
    }

    {
        let mut tx = db.new_transaction(false);
        assert_eq!(tx.get(b"user:1").unwrap(), b"Alice");
        assert_eq!(tx.get(b"user:2").unwrap(), b"Bob");
        assert!(matches!(
            tx.get(b"user:3"),
            Err(EngineError::KeyNotFound)
        ));
        tx.discard();
    }

    {
        let mut tx = db.new_transaction(true);
        tx.put(b"user:1", b"Alice Cooper").unwrap();
        tx.commit().unwrap();
    }

    {
        let mut tx = db.new_transaction(false);
        assert_eq!(tx.get(b"user:1").unwrap(), b"Alice Cooper");
        tx.discard();
    }

    db.close().unwrap();
}

#[test]
fn reopen_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        let mut tx = db.new_transaction(true);
        tx.put(b"k", b"v").unwrap();
        tx.commit().unwrap();
        db.close().unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let tx = db.new_transaction(false);
    assert_eq!(tx.get(b"k").unwrap(), b"v");
}
