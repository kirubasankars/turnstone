// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_engine::{Db, Options};

#[test]
fn value_cache_hit_on_repeat_get() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        value_cache_bytes: 1 << 20,
        shared_buffers_bytes: -1,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    {
        let mut tx = db.new_transaction(true);
        tx.put(b"cached", b"payload").unwrap();
        tx.commit().unwrap();
    }
    {
        let tx = db.new_transaction(false);
        assert_eq!(tx.get(b"cached").unwrap(), b"payload");
    }
    {
        let tx = db.new_transaction(false);
        assert_eq!(tx.get(b"cached").unwrap(), b"payload");
    }
    db.close().unwrap();
}
