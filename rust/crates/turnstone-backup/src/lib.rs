// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

mod backup;
mod meta;
mod restore;
mod stream;

pub use backup::{run_backup, BackupOptions};
pub use meta::{
    load_meta, resolve_restore_chain, resolve_wal_file, resolve_wal_input_file, save_meta,
    validate_restore_chain, Meta, TypeDifferential, TypeFull, DEFAULT_META_FILE, DEFAULT_WAL_FILE,
};
pub use restore::{run_restore, RestoreOptions};
pub use stream::{stream_log_range, StreamOptions, StreamResult};
