// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! WAL recovery index: uses `turnstone-hashindex` with a small test helper trait.

pub use turnstone_hashindex::{Index, Version};

use crate::types::TxStatus;

/// Test-only helpers on top of the real index.
pub trait IndexExt {
    fn live_key_count(&self, clog_status: impl Fn(u64) -> TxStatus) -> i64;
    fn get_chain(&self, key: &[u8]) -> Vec<Version>;
}

impl IndexExt for Index {
    fn live_key_count(&self, clog_status: impl Fn(u64) -> TxStatus) -> i64 {
        let mut n = 0i64;
        self.for_each_key(|_key, chain| {
            if let Some(v) = chain.first() {
                if !v.tombstone && clog_status(v.xmin) == TxStatus::Committed {
                    n += 1;
                }
            }
        });
        n
    }

    fn get_chain(&self, key: &[u8]) -> Vec<Version> {
        let mut chain = Vec::new();
        self.walk_versions(key, |v| {
            chain.push(v);
            true
        });
        chain
    }
}
