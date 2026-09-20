// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

pub(crate) fn buffer_alloc_size(n: i64) -> i64 {
    n
}

pub(crate) fn planned_buffer_size(current_len: i64, min_size: i64) -> i64 {
    let mut n = current_len;
    if n == 0 {
        n = min_size;
    }
    while n < min_size {
        n *= 2;
    }
    buffer_alloc_size(n)
}
