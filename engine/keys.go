// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import "strings"

const maxListKeysLimit = 1000

// ListKeys returns live (committed, non-tombstone) keys matching prefix.
// offset skips the first N matching keys; limit caps the result size.
func (db *DB) ListKeys(prefix string, offset int, limit int) ([]string, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > maxListKeysLimit {
		limit = maxListKeysLimit
	}

	var keys []string
	skipped := 0
	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		if !isKeyLive(chain, db.clogStatus) {
			return
		}
		k := string(key)
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			return
		}
		if skipped < offset {
			skipped++
			return
		}
		if len(keys) < limit {
			keys = append(keys, k)
		}
	})
	return keys, nil
}

// isKeyLive reports whether the newest committed version of a key is live.
func isKeyLive(chain []indexVersion, clog func(uint64) TxStatus) bool {
	for _, v := range chain {
		if clog(v.xmin) != TxCommitted {
			continue
		}
		return !v.tombstone
	}
	return false
}
