#!/usr/bin/env bash
# Copyright (c) 2026 Kiruba Sankar Swaminathan
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root of this source tree.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/rust"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "Installing cargo-llvm-cov..."
  cargo install cargo-llvm-cov --locked
fi

mkdir -p "$ROOT/rust/target/llvm-cov"
cargo llvm-cov --workspace --lcov --output-path "$ROOT/rust/target/llvm-cov/lcov.info"
cargo llvm-cov --workspace --summary-only

echo "LCOV report: rust/target/llvm-cov/lcov.info"
