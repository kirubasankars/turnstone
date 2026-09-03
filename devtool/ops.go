// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package devtool

import (
	"errors"
	"fmt"

	"turnstone/database"
	"turnstone/protocol"
)

func getKey(db *database.Database, key string) ([]byte, error) {
	return db.Get(key)
}

func setKey(db *database.Database, key string, value []byte) error {
	return db.Set(key, value)
}

func delKey(db *database.Database, key string) error {
	return db.Del(key)
}

func mapDataError(err error) (int, string) {
	if err == nil {
		return 200, ""
	}
	switch {
	case errors.Is(err, protocol.ErrKeyNotFound):
		return 404, "key not found"
	case errors.Is(err, database.ErrInvalidKey):
		return 400, "key must be ASCII"
	case errors.Is(err, database.ErrReadOnlyDB):
		return 403, "database is not writable"
	default:
		return 500, fmt.Sprintf("%v", err)
	}
}
