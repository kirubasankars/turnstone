// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package database

// ListKeys returns live keys in this database matching prefix.
func (s *Database) ListKeys(prefix string, offset int, limit int) ([]string, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return nil, nil
	}
	return s.DB.ListKeys(prefix, offset, limit)
}
