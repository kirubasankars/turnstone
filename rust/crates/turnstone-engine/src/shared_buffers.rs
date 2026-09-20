// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

pub const SHARED_BUFFER_PAGE_SIZE: i64 = 8192;
pub const DEFAULT_SHARED_BUFFERS_BYTES: i64 = 64 << 20;
const MIN_PAGES: usize = 16;
const CLOCK_MAX_USAGE: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct BufferTag {
    pub id: u32,
    pub page: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct BufDesc {
    tag: BufferTag,
    valid: bool,
    usage: u32,
    pins: u32,
}

struct Inner {
    descs: Vec<BufDesc>,
    backing: Vec<u8>,
    lookup: HashMap<BufferTag, usize>,
    clock: usize,
}

pub struct SharedBuffers {
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evicts: AtomicU64,
}

impl SharedBuffers {
    pub fn new(bytes: i64) -> Self {
        Self::new_locked(bytes, false).expect("shared buffers alloc")
    }

    pub fn new_locked(bytes: i64, lock: bool) -> Result<Self, String> {
        let mut n = (bytes / SHARED_BUFFER_PAGE_SIZE) as usize;
        if n < MIN_PAGES {
            n = MIN_PAGES;
        }
        let mut backing = vec![0u8; n * SHARED_BUFFER_PAGE_SIZE as usize];
        if lock {
            turnstone_mlock::lock(&mut backing).map_err(|e| e.to_string())?;
        }
        Ok(Self {
            inner: Mutex::new(Inner {
                descs: vec![BufDesc::default(); n],
                backing,
                lookup: HashMap::with_capacity(n),
                clock: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evicts: AtomicU64::new(0),
        })
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn pin<F, R>(&self, tag: BufferTag, fill: F) -> Result<(usize, R), String>
    where
        F: FnOnce(&mut [u8]) -> Result<R, String>,
    {
        let mut inner = self.inner.lock();
        if let Some(&i) = inner.lookup.get(&tag) {
            if inner.descs[i].valid {
                inner.descs[i].pins += 1;
                if inner.descs[i].usage < CLOCK_MAX_USAGE {
                    inner.descs[i].usage += 1;
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                let page = page_slice(&mut inner.backing, i);
                let out = fill(page)?;
                return Ok((i, out));
            }
        }
        let (i, ok) = evict_locked(&mut inner);
        if !ok {
            return Err("shared buffers: no unpinned page".into());
        }
        if inner.descs[i].valid {
            let old_tag = inner.descs[i].tag;
            inner.lookup.remove(&old_tag);
            self.evicts.fetch_add(1, Ordering::Relaxed);
        }
        let page = page_slice(&mut inner.backing, i);
        let out = fill(page).map_err(|e| {
            inner.descs[i] = BufDesc::default();
            e
        })?;
        inner.descs[i] = BufDesc {
            tag,
            valid: true,
            usage: 1,
            pins: 1,
        };
        inner.lookup.insert(tag, i);
        self.misses.fetch_add(1, Ordering::Relaxed);
        Ok((i, out))
    }

    pub fn unpin(&self, i: usize) {
        let mut inner = self.inner.lock();
        if i < inner.descs.len() && inner.descs[i].pins > 0 {
            inner.descs[i].pins -= 1;
        }
    }

    pub fn copy_page(&self, i: usize, off: usize, dest: &mut [u8]) {
        let inner = self.inner.lock();
        if i >= inner.descs.len() {
            return;
        }
        let page = page_slice_ref(&inner.backing, i);
        let n = dest.len().min(page.len().saturating_sub(off));
        dest[..n].copy_from_slice(&page[off..off + n]);
    }

    pub fn apply_write(&self, seg_id: u32, local: i64, data: &[u8]) {
        if data.is_empty() || local < 0 {
            return;
        }
        let mut inner = self.inner.lock();
        let mut pos = local;
        let mut remaining = data;
        while !remaining.is_empty() {
            let page_no = (pos / SHARED_BUFFER_PAGE_SIZE) as u32;
            let off = (pos % SHARED_BUFFER_PAGE_SIZE) as usize;
            let tag = BufferTag {
                id: seg_id,
                page: page_no,
            };
            if let Some(&i) = inner.lookup.get(&tag) {
                if inner.descs[i].valid {
                    let page = page_slice(&mut inner.backing, i);
                    let n = (SHARED_BUFFER_PAGE_SIZE as usize - off).min(remaining.len());
                    page[off..off + n].copy_from_slice(&remaining[..n]);
                    remaining = &remaining[n..];
                    pos += n as i64;
                    continue;
                }
            }
            let skip = (SHARED_BUFFER_PAGE_SIZE as usize - off).min(remaining.len());
            remaining = &remaining[skip..];
            pos += skip as i64;
        }
    }

    pub fn invalidate_segment(&self, id: u32) {
        let mut inner = self.inner.lock();
        let stale: Vec<usize> = inner
            .lookup
            .iter()
            .filter(|(tag, _)| tag.id == id)
            .map(|(_, idx)| *idx)
            .collect();
        for idx in stale {
            if idx < inner.descs.len() {
                inner.descs[idx].valid = false;
            }
        }
        inner.lookup.retain(|tag, _| tag.id != id);
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.lookup.clear();
        for d in &mut inner.descs {
            *d = BufDesc::default();
        }
    }
}

fn page_slice(backing: &mut [u8], i: usize) -> &mut [u8] {
    let off = i * SHARED_BUFFER_PAGE_SIZE as usize;
    &mut backing[off..off + SHARED_BUFFER_PAGE_SIZE as usize]
}

fn page_slice_ref(backing: &[u8], i: usize) -> &[u8] {
    let off = i * SHARED_BUFFER_PAGE_SIZE as usize;
    &backing[off..off + SHARED_BUFFER_PAGE_SIZE as usize]
}

fn evict_locked(inner: &mut Inner) -> (usize, bool) {
    let n = inner.descs.len();
    for _ in 0..n * CLOCK_MAX_USAGE as usize + n {
        let i = inner.clock;
        inner.clock = (i + 1) % n;
        let d = &mut inner.descs[i];
        if d.pins > 0 {
            continue;
        }
        if d.usage > 0 {
            d.usage -= 1;
            continue;
        }
        return (i, true);
    }
    (0, false)
}
