// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_config::share_bytes;

#[test]
fn share_bytes_splits_evenly() {
    assert_eq!(share_bytes(4096, 4), 1024);
    assert_eq!(share_bytes(0, 4), 0);
    assert_eq!(share_bytes(100, 0), 100);
}
