// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::arena::{grow_shard_buffer, new_shard_buffer_locked, ShardBuffer};
use crate::buffer_size::planned_buffer_size;
use crate::encoding::{
    hash_key, read_u32, read_u64, read_version, write_u32, write_u64, write_version,
    FORMAT_VERSION, HEADER_SIZE, INITIAL_SLOTS, MAGIC, MAX_LOAD_FACTOR_DEN, MAX_LOAD_FACTOR_NUM,
    VERSION_NODE_SZ, VERSION_SIZE, HDR_ARENA_OFF_OFF, HDR_ARENA_USED_OFF, HDR_KEY_COUNT_OFF,
    HDR_MAGIC_OFF, HDR_SLOT_COUNT_OFF, HDR_TABLE_OFF_OFF, HDR_VERSION_OFF,
};
use crate::errors::ErrArenaLimit;
use crate::index::IndexInner;
use crate::Version;

pub(crate) struct ShardState {
    pub(crate) buf: Option<ShardBuffer>,
}

pub struct Shard {
    parent: Arc<IndexInner>,
    pub(crate) state: RwLock<ShardState>,
}

impl Shard {
    pub fn empty(parent: Arc<IndexInner>) -> Self {
        Self {
            parent,
            state: RwLock::new(ShardState { buf: None }),
        }
    }

    pub fn new(parent: Arc<IndexInner>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let table_bytes = i64::from(INITIAL_SLOTS) * 8;
        let min_size = HEADER_SIZE as i64 + table_bytes + HEADER_SIZE as i64;
        let mut buf = new_shard_buffer_locked(min_size, parent.mlock)?;
        Self::init_header_on_buf(&mut buf, INITIAL_SLOTS);
        Ok(Self {
            parent,
            state: RwLock::new(ShardState { buf: Some(buf) }),
        })
    }

    pub fn buffer_len(&self) -> i64 {
        self.state
            .read()
            .buf
            .as_ref()
            .map(|b| b.len() as i64)
            .unwrap_or(0)
    }

    pub fn close(&self) {
        let mut st = self.state.write();
        if let Some(mut buf) = st.buf.take() {
            let _ = self.parent.account_delta(-(buf.len() as i64));
            buf.close();
        }
    }

    fn is_closed(st: &ShardState) -> bool {
        st.buf.as_ref().is_none_or(|b| b.is_closed())
    }

    pub(crate) fn parent(&self) -> &Arc<IndexInner> {
        &self.parent
    }

    pub(crate) fn with_shard_buffer<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&ShardBuffer) -> R,
    {
        self.state.read().buf.as_ref().map(f)
    }

    pub(crate) fn init_header_on_buf(buf: &mut ShardBuffer, slot_count: u32) {
        let data = buf.as_mut_slice();
        write_u64(data, HDR_MAGIC_OFF, MAGIC);
        write_u32(data, HDR_VERSION_OFF, FORMAT_VERSION);
        write_u32(data, HDR_SLOT_COUNT_OFF, slot_count);
        write_u32(data, HDR_KEY_COUNT_OFF, 0);
        let table_off = HEADER_SIZE as u64;
        let arena_off = table_off + u64::from(slot_count) * 8;
        write_u64(data, HDR_TABLE_OFF_OFF, table_off);
        write_u64(data, HDR_ARENA_OFF_OFF, arena_off);
        write_u64(data, HDR_ARENA_USED_OFF, 0);
    }

    pub(crate) fn slot_count_on_data(data: &[u8]) -> u32 {
        read_u32(data, HDR_SLOT_COUNT_OFF)
    }

    pub(crate) fn key_count_on_data(data: &[u8]) -> u32 {
        read_u32(data, HDR_KEY_COUNT_OFF)
    }

    pub(crate) fn arena_used_on_data(data: &[u8]) -> u64 {
        read_u64(data, HDR_ARENA_USED_OFF)
    }

    pub(crate) fn set_arena_used_on_data(data: &mut [u8], n: u64) {
        write_u64(data, HDR_ARENA_USED_OFF, n);
    }

    pub(crate) fn arena_off_on_data(data: &[u8]) -> u64 {
        read_u64(data, HDR_ARENA_OFF_OFF)
    }

    fn grow_buf(buf: &mut ShardBuffer, parent: &IndexInner, min_size: i64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if buf.is_closed() {
            return Err("hashindex: shard is closed".into());
        }
        let old_len = buf.len() as i64;
        if min_size <= old_len {
            return Ok(());
        }
        let planned = planned_buffer_size(old_len, min_size);
        let delta = planned - old_len;
        parent.account_delta(delta).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        if let Err(e) = grow_shard_buffer(buf, min_size) {
            let _ = parent.account_delta(-delta);
            return Err(e);
        }
        let adj = buf.len() as i64 - planned;
        if adj != 0 {
            parent
                .account_delta(adj)
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        }
        Ok(())
    }

    fn alloc_buf(buf: &mut ShardBuffer, parent: &IndexInner, size: usize) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let data = buf.as_slice();
        let off = Self::arena_off_on_data(data) + Self::arena_used_on_data(data);
        let need = off as i64 + size as i64;
        if need > buf.len() as i64 {
            Self::grow_buf(buf, parent, need)?;
        }
        let data = buf.as_mut_slice();
        let used = Self::arena_used_on_data(data);
        Self::set_arena_used_on_data(data, used + size as u64);
        Ok(off)
    }

    fn slot_index(data: &[u8], key: &[u8]) -> u32 {
        (hash_key(key) % u64::from(Self::slot_count_on_data(data))) as u32
    }

    fn key_at(data: &[u8], rec_off: u64, key: &[u8]) -> bool {
        if rec_off as usize + 12 > data.len() {
            return false;
        }
        let k_len = read_u32(data, rec_off as usize);
        if k_len as usize != key.len() {
            return false;
        }
        if rec_off as usize + 12 + k_len as usize > data.len() {
            return false;
        }
        &data[rec_off as usize + 12..rec_off as usize + 12 + k_len as usize] == key
    }

    fn find_key_record(data: &[u8], key: &[u8]) -> (u64, bool) {
        let slots = Self::slot_count_on_data(data);
        let start = Self::slot_index(data, key);
        let table = read_u64(data, HDR_TABLE_OFF_OFF) as usize;
        for i in 0..slots {
            let slot = (start + i) % slots;
            let off = read_u64(data, table + slot as usize * 8);
            if off == 0 {
                return (0, false);
            }
            if Self::key_at(data, off, key) {
                return (off, true);
            }
        }
        (0, false)
    }

    fn table_load_high(data: &[u8]) -> bool {
        let slots = Self::slot_count_on_data(data);
        if slots == 0 {
            return true;
        }
        u64::from(Self::key_count_on_data(data) + 1) * MAX_LOAD_FACTOR_DEN
            > u64::from(slots) * MAX_LOAD_FACTOR_NUM
    }

    fn read_key(data: &[u8], rec_off: u64) -> Vec<u8> {
        let k_len = read_u32(data, rec_off as usize) as usize;
        data[rec_off as usize + 12..rec_off as usize + 12 + k_len].to_vec()
    }

    fn version_head(data: &[u8], rec_off: u64) -> u64 {
        read_u64(data, rec_off as usize + 4)
    }

    fn set_version_head(data: &mut [u8], rec_off: u64, head: u64) {
        write_u64(data, rec_off as usize + 4, head);
    }

    fn bump_arena_chain_refs(data: &mut [u8], rec_off: u64, delta: u64) {
        let head = Self::version_head(data, rec_off);
        if head == 0 {
            return;
        }
        let new_head = head + delta;
        Self::set_version_head(data, rec_off, new_head);
        let mut node = new_head;
        loop {
            let next_off = read_u64(data, node as usize + VERSION_SIZE);
            if next_off != 0 {
                write_u64(data, node as usize + VERSION_SIZE, next_off + delta);
                node = next_off + delta;
            } else {
                break;
            }
        }
    }

    fn grow_hash_table(st: &mut ShardState, parent: &IndexInner) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buf = st.buf.as_mut().ok_or("hashindex: shard is closed")?;
        let old_slots = Self::slot_count_on_data(buf.as_slice());
        let new_slots = old_slots.checked_mul(2).ok_or("shard hash table slot overflow")?;
        if new_slots <= old_slots {
            return Err("shard hash table slot overflow".into());
        }
        let table_start = read_u64(buf.as_slice(), HDR_TABLE_OFF_OFF);
        let arena_start = Self::arena_off_on_data(buf.as_slice());
        let used = Self::arena_used_on_data(buf.as_slice());
        let new_table_bytes = u64::from(new_slots) * 8;
        let new_arena_start = table_start + new_table_bytes;
        let need = (new_arena_start + used) as i64;
        Self::grow_buf(buf, parent, need)?;
        let data = buf.as_mut_slice();
        let mut arena_snap = vec![0u8; used as usize];
        if used > 0 {
            arena_snap.copy_from_slice(&data[arena_start as usize..arena_start as usize + used as usize]);
        }
        let old_table_int = table_start as usize;
        let delta = new_arena_start - arena_start;
        struct Entry {
            key: Vec<u8>,
            new_rec_off: u64,
        }
        let mut entries = Vec::new();
        for slot in 0..old_slots {
            let rec_off = read_u64(data, old_table_int + slot as usize * 8);
            if rec_off == 0 {
                continue;
            }
            let key = read_key_from_arena(&arena_snap, rec_off, arena_start).ok_or("shard hash table grow: bad key record")?;
            entries.push(Entry {
                key,
                new_rec_off: rec_off - arena_start + new_arena_start,
            });
        }
        let new_table = table_start as usize;
        for i in 0..new_slots {
            write_u64(data, new_table + i as usize * 8, 0);
        }
        if used > 0 {
            data[new_arena_start as usize..new_arena_start as usize + used as usize].copy_from_slice(&arena_snap);
        }
        for e in &entries {
            Self::bump_arena_chain_refs(data, e.new_rec_off, delta);
            let start = (hash_key(&e.key) % u64::from(new_slots)) as u32;
            let mut inserted = false;
            for i in 0..new_slots {
                let slot_off = new_table + ((start + i) % new_slots) as usize * 8;
                if read_u64(data, slot_off) == 0 {
                    write_u64(data, slot_off, e.new_rec_off);
                    inserted = true;
                    break;
                }
            }
            if !inserted {
                return Err("shard hash table rehash failed".into());
            }
        }
        write_u32(data, HDR_SLOT_COUNT_OFF, new_slots);
        write_u64(data, HDR_ARENA_OFF_OFF, new_arena_start);
        Ok(())
    }

    pub(crate) fn insert_key_slot_on_data(
        data: &mut [u8],
        key: &[u8],
        rec_off: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let slots = Self::slot_count_on_data(data);
        let start = Self::slot_index(data, key);
        let table = read_u64(data, HDR_TABLE_OFF_OFF) as usize;
        for i in 0..slots {
            let slot = (start + i) % slots;
            let slot_off = table + slot as usize * 8;
            if read_u64(data, slot_off) == 0 {
                write_u64(data, slot_off, rec_off);
                let kc = Self::key_count_on_data(data);
                write_u32(data, HDR_KEY_COUNT_OFF, kc + 1);
                return Ok(());
            }
        }
        Err("shard hash table full".into())
    }

    fn find_or_create_key_record(
        st: &mut ShardState,
        parent: &IndexInner,
        key: &[u8],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let buf = st.buf.as_mut().ok_or("hashindex: shard is closed")?;
            let (off, ok) = Self::find_key_record(buf.as_slice(), key);
            if ok {
                return Ok(off);
            }
            if Self::table_load_high(buf.as_slice()) {
                Self::grow_hash_table(st, parent)?;
            }
            let buf = st.buf.as_mut().unwrap();
            let rec_size = 12 + key.len();
            let off = Self::alloc_buf(buf, parent, rec_size)?;
            {
                let data = buf.as_mut_slice();
                write_u32(data, off as usize, key.len() as u32);
                write_u64(data, off as usize + 4, 0);
                data[off as usize + 12..off as usize + 12 + key.len()].copy_from_slice(key);
            }
            match Self::insert_key_slot_on_data(buf.as_mut_slice(), key, off) {
                Ok(()) => return Ok(off),
                Err(_) => {
                    let arena_before = Self::arena_off_on_data(st.buf.as_ref().unwrap().as_slice());
                    Self::grow_hash_table(st, parent)?;
                    let off = off + Self::arena_off_on_data(st.buf.as_ref().unwrap().as_slice()) - arena_before;
                    Self::insert_key_slot_on_data(st.buf.as_mut().unwrap().as_mut_slice(), key, off)?;
                    return Ok(off);
                }
            }
        }
    }

    pub fn put(&self, key: &[u8], ver: Version) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut st = self.state.write();
        if Self::is_closed(&st) {
            return Err("hashindex: shard is closed".into());
        }
        let rec_off = Self::find_or_create_key_record(&mut st, &self.parent, key)?;
        let buf = st.buf.as_mut().unwrap();
        let node_off = Self::alloc_buf(buf, &self.parent, VERSION_NODE_SZ)?;
        let data = buf.as_mut_slice();
        write_version(data, node_off as usize, &ver);
        let head = Self::version_head(data, rec_off);
        write_u64(data, node_off as usize + VERSION_SIZE, head);
        Self::set_version_head(data, rec_off, node_off);
        Ok(())
    }

    pub fn walk_versions<F>(&self, key: &[u8], f: &mut F)
    where
        F: FnMut(Version) -> bool,
    {
        let st = self.state.read();
        let Some(ref buf) = st.buf else {
            return;
        };
        if buf.is_closed() {
            return;
        }
        let data = buf.as_slice();
        let (rec_off, ok) = Self::find_key_record(data, key);
        if !ok {
            return;
        }
        let mut node = Self::version_head(data, rec_off);
        while node != 0 {
            if node as usize + VERSION_NODE_SZ > data.len() {
                break;
            }
            let v = read_version(data, node as usize);
            if !f(v) {
                break;
            }
            node = read_u64(data, node as usize + VERSION_SIZE);
        }
    }

    pub fn for_each_key<F>(&self, f: &mut F)
    where
        F: FnMut(&[u8], &[Version]),
    {
        let st = self.state.read();
        let Some(ref buf) = st.buf else {
            return;
        };
        if buf.is_closed() {
            return;
        }
        let data = buf.as_slice();
        let slots = Self::slot_count_on_data(data);
        let table = read_u64(data, HDR_TABLE_OFF_OFF) as usize;
        for slot in 0..slots {
            let rec_off = read_u64(data, table + slot as usize * 8);
            if rec_off == 0 {
                continue;
            }
            let key = Self::read_key(data, rec_off);
            let chain = self.read_chain_locked(data, rec_off);
            if !chain.is_empty() {
                f(&key, &chain);
            }
        }
    }

    pub fn drop_xid(&self, xid: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut st = self.state.write();
        if Self::is_closed(&st) {
            return Ok(());
        }
        let slots = Self::slot_count_on_data(st.buf.as_ref().unwrap().as_slice());
        let table = read_u64(st.buf.as_ref().unwrap().as_slice(), HDR_TABLE_OFF_OFF) as usize;
        for slot in 0..slots {
            let slot_off = table + slot as usize * 8;
            let rec_off = read_u64(st.buf.as_ref().unwrap().as_slice(), slot_off);
            if rec_off == 0 {
                continue;
            }
            let (new_head, empty) = Self::filter_chain(&mut st, &self.parent, rec_off, |v| v.xmin != xid)?;
            let buf = st.buf.as_mut().unwrap();
            let data = buf.as_mut_slice();
            if empty {
                write_u64(data, slot_off, 0);
                let kc = Self::key_count_on_data(data);
                write_u32(data, HDR_KEY_COUNT_OFF, kc - 1);
            } else {
                Self::set_version_head(data, rec_off, new_head);
            }
        }
        Ok(())
    }

    fn filter_chain(
        st: &mut ShardState,
        parent: &IndexInner,
        rec_off: u64,
        keep: impl Fn(Version) -> bool,
    ) -> Result<(u64, bool), Box<dyn std::error::Error + Send + Sync>> {
        let head = {
            let data = st.buf.as_ref().unwrap().as_slice();
            Self::version_head(data, rec_off)
        };
        let mut kept = Vec::new();
        let mut node = head;
        while node != 0 {
            let data = st.buf.as_ref().unwrap().as_slice();
            if node as usize + VERSION_NODE_SZ > data.len() {
                break;
            }
            let v = read_version(data, node as usize);
            if keep(v) {
                kept.push(v);
            }
            node = read_u64(data, node as usize + VERSION_SIZE);
        }
        if kept.is_empty() {
            return Ok((0, true));
        }
        let mut new_head = 0u64;
        for i in (0..kept.len()).rev() {
            let buf = st.buf.as_mut().unwrap();
            let node_off = Self::alloc_buf(buf, parent, VERSION_NODE_SZ)?;
            let data = buf.as_mut_slice();
            write_version(data, node_off as usize, &kept[i]);
            write_u64(data, node_off as usize + VERSION_SIZE, new_head);
            new_head = node_off;
        }
        Ok((new_head, false))
    }

    pub(crate) fn read_chain_locked(&self, data: &[u8], rec_off: u64) -> Vec<Version> {
        let mut chain = Vec::new();
        let mut node = Self::version_head(data, rec_off);
        while node != 0 {
            if node as usize + VERSION_NODE_SZ > data.len() {
                break;
            }
            chain.push(read_version(data, node as usize));
            node = read_u64(data, node as usize + VERSION_SIZE);
        }
        chain
    }

    pub(crate) fn stats(&self, shard_index: u32) -> crate::compact::ShardStats {
        let st = self.state.read();
        let Some(ref buf) = st.buf else {
            return crate::compact::ShardStats {
                shard_index,
                ..Default::default()
            };
        };
        if buf.is_closed() {
            return crate::compact::ShardStats {
                shard_index,
                ..Default::default()
            };
        }
        let data = buf.as_slice();
        let mut st_out = crate::compact::ShardStats {
            shard_index,
            slot_count: Self::slot_count_on_data(data),
            key_count: Self::key_count_on_data(data),
            arena_used: Self::arena_used_on_data(data),
            allocated_bytes: buf.len() as u64,
            ..Default::default()
        };
        if st_out.key_count > 0 {
            st_out.live_bytes = self.live_bytes_locked(data, None);
        }
        st_out
    }

    pub(crate) fn live_bytes_locked(
        &self,
        data: &[u8],
        filter: Option<&crate::compact::VersionFilter<'_>>,
    ) -> u64 {
        let table = read_u64(data, HDR_TABLE_OFF_OFF) as usize;
        let slots = Self::slot_count_on_data(data);
        let mut live = 0u64;
        for slot in 0..slots {
            let rec_off = read_u64(data, table + slot as usize * 8);
            if rec_off == 0 {
                continue;
            }
            let key = Self::read_key(data, rec_off);
            let chain = self.read_chain_locked(data, rec_off);
            let versions = if let Some(f) = filter {
                f(&key, &chain)
            } else {
                chain
            };
            if versions.is_empty() {
                continue;
            }
            live += (12 + key.len()) as u64 + (versions.len() as u64) * VERSION_NODE_SZ as u64;
        }
        live
    }

    pub(crate) fn compact(
        &self,
        filter: Option<&crate::compact::VersionFilter<'_>>,
    ) -> Result<crate::compact::CompactResult, Box<dyn std::error::Error + Send + Sync>> {
        crate::compact::compact_shard(self, filter)
    }

    pub(crate) fn replace_buffer(&self, next: ShardBuffer) -> Result<(), ErrArenaLimit> {
        let mut st = self.state.write();
        let old_len = st.buf.as_ref().map(|b| b.len() as i64).unwrap_or(0);
        let new_len = next.len() as i64;
        self.parent.account_delta(new_len - old_len)?;
        if let Some(mut old) = st.buf.take() {
            old.close();
        }
        st.buf = Some(next);
        Ok(())
    }
}

fn read_key_from_arena(buf: &[u8], rec_off: u64, arena_start: u64) -> Option<Vec<u8>> {
    let rel = (rec_off - arena_start) as usize;
    if rel + 12 > buf.len() {
        return None;
    }
    let k_len = read_u32(buf, rel) as usize;
    Some(buf[rel + 12..rel + 12 + k_len].to_vec())
}
