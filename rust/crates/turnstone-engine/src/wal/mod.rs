// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

mod alloc;
mod log;
mod logrange;
mod manifest;
mod mmap;
mod recycle;
mod sync;

pub use log::{set_testing_before_sync, CopyForwardOutcome, DataLog};
pub use alloc::set_testing_prealloc_err;
pub use logrange::{validate_frames, LogFrame};

pub use manifest::{
    load_wal_manifest, normalize_wal_segment_size, save_wal_manifest, wal_manifest_tmp_path,
    wal_segment_file_name, WalManifest, WalManifestSegment, DEFAULT_WAL_SEG_SIZE, WAL_DIR_NAME,
    WAL_MANIFEST_NAME, WAL_MANIFEST_VERSION,
};
pub use recycle::{
    create_allocated_wal_file, read_segment_footer, write_segment_footer,
    write_segment_footer_if_allocated, WAL_SEG_FOOTER_MAGIC, WAL_SEG_FOOTER_SIZE,
};
