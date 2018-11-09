package kvimd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const keySize = 16 // Maybe change it to DB scoped after to have it configurable

const (
	rotateHashDiskMaxLoad   = 0.7 // Load factor at which point rotate will create a new HashDisk
	rotateValuesDiskMaxLoad = 0.9 // % of file usage at which point rotate will create a new ValuesDisk
)

// Define public errors
var (
	ErrDBClosed    = errors.New("database is already closed")
	ErrFileTooBig  = errors.New("file size is too big (max 4Gb)")
	ErrInvalidKey  = errors.New("key is not valid")
	ErrKeyNotFound = errors.New("key was not found in database")
	ErrNoSpace     = errors.New("no space left in database") // What you usually want to do here is create a new file
)

// DB is a kvimd database.
// It uses uint32 in a lot of places so this means: each hashmap file is max 4Gb; you can store max 4Gb*4Gb/workers values (a lot)
type DB struct {
	RootPath string
	fileSize uint32
	closed   uint32 // Just a boolean to indicate whether the database is closed. > 0 means it's closed

	// Current opened HashDisk DB. You should always write to the last one (openHashDisk[len-1])
	// When looking up a value, you will need to look in each.
	// Protected by a ReadWriteLock to allow adding a new one
	openHashDiskMutex sync.RWMutex
	openHashDisk      []*hashDisk

	// Current opened ValuesDisk DB.
	// The mutex needs a RLock on any read/write operation to one of database.
	// A (write) Lock must be acquired when the DB is full and we need to create the next one
	// /!\ Caution, Close locks this mutex then the one in openHashDiskMutex so you should never lock in the other order
	// otherwise you'll have a deadlock
	openValuesDiskMutex sync.RWMutex
	openValuesDisk      map[uint32]*valuesDisk
	// The most recently opened (and actively written to) ValuesDisk DB. Need to be used with atomic methods
	currentValuesDiskIndex uint32
}

// NewDB returns a new kvimd database
func NewDB(root string, fileSize uint32) (*DB, error) {
	if fileSize >= 2<<31-1 {
		return nil, ErrFileTooBig
	}

	// Create paths if non-existant
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	// Load all HashDisk databases
	files, err := listFiles(root, hashDiskPattern)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list directory")
	}

	openHashDisk := make([]*hashDisk, len(files))
	for i, f := range files {
		p := filepath.Join(root, f)
		hd, err := newHashDisk(p, int64(fileSize))
		if err != nil {
			// ToDo: close all previous DBs
			return nil, errors.Wrap(err, "failed to open HashDisk database")
		}
		openHashDisk[i] = hd
	}

	// If there are none, create 1
	if len(openHashDisk) == 0 {
		p := filepath.Join(root, "db0.hashdisk")
		hd, err := newHashDisk(p, int64(fileSize))
		if err != nil {
			return nil, errors.Wrap(err, "failed to open HashDisk database")
		}
		openHashDisk = append(openHashDisk, hd)
	}

	// Load all ValuesDisk databases
	files, err = listFiles(root, valuesDiskPattern)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list directory")
	}

	openValuesDisk := make(map[uint32]*valuesDisk)
	maxValuesDiskIndex := uint32(0)
	for i, f := range files {
		// Try to find the highest index
		index, err := getDBNumber(f)
		if err != nil {
			return nil, err
		}
		if uint32(index) > maxValuesDiskIndex {
			maxValuesDiskIndex = uint32(index)
		}

		p := filepath.Join(root, f)
		vd, err := newValuesDisk(p, fileSize, uint32(index))
		if err != nil {
			// ToDo: close all previous DBs
			return nil, errors.Wrap(err, "failed to open ValuesDisk database")
		}
		openValuesDisk[uint32(i)] = vd
	}

	// If there are none, create 1
	if len(openValuesDisk) == 0 {
		p := filepath.Join(root, "db0.valuesdisk")
		vd, err := newValuesDisk(p, fileSize, 0)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open ValuesDisk database")
		}
		openValuesDisk[0] = vd
	}

	db := &DB{
		RootPath: root,
		fileSize: fileSize,

		openHashDisk:           openHashDisk,
		openValuesDisk:         openValuesDisk,
		currentValuesDiskIndex: maxValuesDiskIndex,
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			err := db.rotate()
			closed := atomic.LoadUint32(&db.closed)
			if closed > 0 {
				// We are closed, stop goroutine and don't check the error (likely will be one)
				return
			}
			if err != nil {
				fmt.Printf("kvimd: failed to create new databases: %s\n", err)
			}
		}
	}()
	return db, nil
}

// findKey tries to find and return the value in HashDisk of the key
// If the key is not found, return a ErrKeyNotFound error
func (d *DB) findKey(key []byte) (fileIndex, fileOffset uint32, err error) {
	d.openHashDiskMutex.RLock()
	if len(d.openHashDisk) == 0 {
		d.openHashDiskMutex.RUnlock()
		return 0, 0, ErrDBClosed
	}
	for i := len(d.openHashDisk) - 1; i >= 0; i-- {
		db := d.openHashDisk[i]
		index, offset, err := db.Get(key)
		if err == nil { // The key is there
			d.openHashDiskMutex.RUnlock()
			return index, offset, nil
		} else if err != ErrKeyNotFound {
			d.openHashDiskMutex.RUnlock()
			return 0, 0, err // Something wrong, return
		}
	}
	d.openHashDiskMutex.RUnlock()
	return 0, 0, ErrKeyNotFound
}

// Read a value for a given key from the database. If error is nil then value is returned
// Return ErrKeyNotFound if key doesn't exist. Return any non-nil error on other errors
func (d *DB) Read(key []byte) ([]byte, error) {
	fileIndex, fileOffset, err := d.findKey(key)
	if err != nil {
		return nil, err
	}
	d.openValuesDiskMutex.RLock()
	if len(d.openValuesDisk) == 0 {
		d.openValuesDiskMutex.RUnlock()
		return nil, ErrDBClosed
	}
	value, err := d.openValuesDisk[fileIndex].Get(fileOffset)
	d.openValuesDiskMutex.RUnlock()
	return value, err
}

// Write a value for a given key in the database. If write succeed, returned error is nil
// Value might not be persisted directly to disk.
func (d *DB) Write(key, value []byte) error {
	// Check if the key already exist first (we don't need to override in that case)
	_, _, err := d.findKey(key)
	if err == nil {
		return nil // We found the key already
	}
	if err != ErrKeyNotFound && err != nil {
		return err // Something wrong happened
	}

	// Then write to valuesDisk DB
	d.openValuesDiskMutex.RLock()
	if len(d.openValuesDisk) == 0 {
		d.openValuesDiskMutex.RUnlock()
		return ErrDBClosed
	}
	index := d.currentValuesDiskIndex
	offset, err := d.openValuesDisk[index].Set(value)
	d.openValuesDiskMutex.RUnlock()
	if err != nil {
		return err
	}

	// Then insert into hashDisk DB
	d.openHashDiskMutex.RLock()
	if len(d.openHashDisk) == 0 {
		d.openHashDiskMutex.RUnlock()
		return ErrDBClosed
	}
	dbHash := d.openHashDisk[len(d.openHashDisk)-1]
	dbHash.Lock()
	err = dbHash.Set(key, index, offset)
	dbHash.Unlock()
	d.openHashDiskMutex.RUnlock()
	return err
}

// Close the database, flushing all pending operations to disk.
// It is not safe to call any Read or Write after a Close
func (d *DB) Close() error {
	atomic.StoreUint32(&d.closed, 1)
	d.openHashDiskMutex.Lock()
	defer d.openHashDiskMutex.Unlock()
	d.openValuesDiskMutex.Lock()
	defer d.openValuesDiskMutex.Unlock()

	// Close all the databases
	var errors []error
	for _, vd := range d.openValuesDisk {
		err := vd.Close()
		errors = append(errors, err)
	}
	d.openValuesDisk = nil

	for _, hd := range d.openHashDisk {
		err := hd.Close()
		errors = append(errors, err)
	}
	d.openHashDisk = nil

	return firstError(errors...)
}

// rotate rotates databases when needed:
//   - rotates HashDisk when load factor is high (and we will soon disallow writes)
//   - rotates ValuesDisk when offset is near the max size
func (d *DB) rotate() error {
	// First check HashDisk
	d.openHashDiskMutex.RLock()
	if len(d.openHashDisk) == 0 {
		d.openHashDiskMutex.RUnlock()
		return ErrDBClosed
	}
	nbDBs := len(d.openHashDisk)
	load := d.openHashDisk[nbDBs-1].Load()
	d.openHashDiskMutex.RUnlock()
	if load > rotateHashDiskMaxLoad {
		// We need to rotate
		fmt.Println("kvimd: HashDisk database is full, creating a new one")
		path := createHashDiskPath(uint32(nbDBs))
		newDB, err := newHashDisk(path, int64(d.fileSize))
		if err != nil {
			return err
		}
		d.openHashDiskMutex.Lock()
		d.openHashDisk = append(d.openHashDisk, newDB)
		d.openHashDiskMutex.Unlock()
	}

	// Then check ValuesDisk
	d.openValuesDiskMutex.RLock()
	if len(d.openValuesDisk) == 0 {
		d.openValuesDiskMutex.RUnlock()
		return ErrDBClosed
	}
	index := d.currentValuesDiskIndex
	load = d.openValuesDisk[index].Load()
	d.openValuesDiskMutex.RUnlock()
	if load > rotateValuesDiskMaxLoad {
		// We need to rotate
		fmt.Println("kvimd: ValuesDisk database is full, creating a new one")
		index++
		path := createValuesDiskPath(index)
		db, err := newValuesDisk(path, d.fileSize, index)
		if err != nil {
			return err
		}
		d.openValuesDiskMutex.Lock()
		d.openValuesDisk[index] = db
		d.currentValuesDiskIndex = index
		d.openValuesDiskMutex.Unlock()
	}
	return nil
}
