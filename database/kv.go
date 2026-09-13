// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package database

import (
	"errors"

	"turnstone/engine"
	"turnstone/protocol"
)

var (
	ErrInvalidKey = errors.New("key must contain only ASCII characters")
	ErrReadOnlyDB = errors.New("database is not writable")
)

// Set stores a value for key.
func (s *Database) Set(key string, value []byte) error {
	if !protocol.IsASCII(key) {
		return ErrInvalidKey
	}
	state := s.GetState()
	if state != StatePrimary && state != StateSteppingDown {
		return ErrReadOnlyDB
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return engine.ErrTxnFinished
	}
	tx := s.DB.NewTransaction(true)
	defer tx.Discard()
	if err := tx.Put([]byte(key), value); err != nil {
		return err
	}
	return tx.Commit()
}

// Del removes a key.
func (s *Database) Del(key string) error {
	if !protocol.IsASCII(key) {
		return ErrInvalidKey
	}
	state := s.GetState()
	if state != StatePrimary && state != StateSteppingDown {
		return ErrReadOnlyDB
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return engine.ErrTxnFinished
	}
	tx := s.DB.NewTransaction(true)
	defer tx.Discard()
	if err := tx.Delete([]byte(key)); err != nil {
		return err
	}
	return tx.Commit()
}
