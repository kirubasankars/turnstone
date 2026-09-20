// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::atomic::{AtomicI64, Ordering};

use crate::errors::ErrArenaLimit;

/// Process-wide cap on hash-index shard buffer bytes.
pub struct SharedBudget {
    max: i64,
    used: AtomicI64,
}

impl SharedBudget {
    /// Returns a shared cap, or `None` when `max <= 0` (unlimited).
    pub fn new(max: i64) -> Option<std::sync::Arc<Self>> {
        if max <= 0 {
            return None;
        }
        Some(std::sync::Arc::new(Self {
            max,
            used: AtomicI64::new(0),
        }))
    }

    pub fn max(&self) -> i64 {
        self.max
    }

    pub fn used(&self) -> i64 {
        self.used.load(Ordering::Acquire)
    }

    pub(crate) fn reserve(&self, delta: i64) -> Result<(), ErrArenaLimit> {
        if delta == 0 {
            return Ok(());
        }
        if delta < 0 {
            self.used.fetch_add(delta, Ordering::AcqRel);
            return Ok(());
        }
        loop {
            let used = self.used.load(Ordering::Acquire);
            let next = used + delta;
            if next > self.max {
                return Err(ErrArenaLimit);
            }
            if self
                .used
                .compare_exchange(used, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}
