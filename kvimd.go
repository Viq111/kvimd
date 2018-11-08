package kvimd

import (
	"errors"
)

// Define public errors
var (
	ErrInvalidKey  = errors.New("key is not valid")
	ErrKeyNotFound = errors.New("key was not found in database")
	ErrFileTooBig  = errors.New("file size is too big (max 4Gb)")
)

const keySize = 16 // Maybe change it to DB scoped after to have it configurable

// DB is a kvimd database.
// It uses uint32 in a lot of places so this means: each hashmap file is max 4Gb; you can store max 4Gb*4Gb/workers values (a lot)
type DB struct {
	RootPath string

	metadata *Metadata
}

// NewDB returns a new kvimd database
func NewDB(root string, workers, fileSize int) (*DB, error) {
	m := &Metadata{
		workers:  workers,
		hashSize: fileSize,
	}
	if fileSize >= 2<<31 {
		return nil, ErrFileTooBig
	}
	// ToDo: create or load DB at root

	return &DB{
		RootPath: root,
		metadata: m,
	}, nil
}

// Read a value for a given key from the database. If error is nil then value is returned
// Return ErrKeyNotFound if key doesn't exist. Return any non-nil error on other errors
func (d *DB) Read(key []byte) ([]byte, error) {
	// ToDo
	return nil, nil
}

// Write a value for a given key in the database. If write succeed, returned error is nil
// Value might not be persisted directly to disk.
func (d *DB) Write(key, value []byte) error {
	// ToDo
	return nil
}

// Close the database, flushing all pending operations to disk.
// It is not safe to call any Read or Write after a Close
func (d *DB) Close() error {
	// ToDo
	return nil
}
