package kvimd

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	"github.com/DataDog/hyperloglog"
	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const (
	// maxLoad is the load after the one, we will not accept Set anymore
	maxLoad = 0.8
)

var (
	encoding = binary.LittleEndian
)

// hashDisk represents a HashMap of constant key and value size.
// It uses mmap internally. It is **NOT THREAD-SAFE** (you need to acquire hashDisk.Lock())
type hashDisk struct {
	sync.RWMutex
	MaxSize uint32 // Max number of items we can add into the hash. This is computed by the map itself

	emptyValue   []byte
	entries      uint32
	entrySize    uint32
	totalEntries uint32
	file         *os.File
	m            mmap.MMap
}

func newHashDisk(path string, size int64) (*hashDisk, error) {
	// Open or create the file
	f, err := os.OpenFile(path, os.O_RDWR, 0755)
	if os.IsNotExist(err) {
		// File doesn't exist, create and truncate
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create file")
		}
		err = f.Truncate(size)
		if err != nil {
			return nil, errors.Wrap(err, "failed to resize file")
		}
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, errors.Wrap(err, "failed to get file infos")
	}
	size = info.Size()
	entrySize := uint32(keySize + 4 + 4) // An entry is a key, file_index, index_in_file
	entries := uint32(size) / entrySize

	// Mmap the file
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, errors.Wrap(err, "failed to mmap file")
	}

	return &hashDisk{
		MaxSize:    uint32(maxLoad * float64(entries)),
		emptyValue: make([]byte, keySize),
		entries:    entries,
		entrySize:  entrySize,
		file:       f,
		m:          m,
	}, nil
}

// Load returns the load factor of the hashmap.
// If accessed concurrently you need a read lock
func (h *hashDisk) Load() float64 {
	return float64(h.totalEntries) / float64(h.MaxSize)
}

// Set a given value that was stored in another database at fileIndex and fileOffset
// If accessed concurrently you need a write lock
func (h *hashDisk) Set(value []byte, fileIndex, fileOffset uint32) error {
	if bytes.Equal(value, h.emptyValue) {
		return ErrInvalidKey
	}
	if h.totalEntries >= h.MaxSize {
		return ErrNoSpace
	}
	newEntry := true
	// Compute hash
	slot := hyperloglog.MurmurBytes(value) % h.entries
	offset := slot * h.entrySize
	for { // Try to find an empty slot
		slotValue := h.m[offset : offset+keySize]
		if bytes.Equal(slotValue, value) {
			// Found same key, override
			// ToDo: Benchmark with / without a return.
			// Since we are technically unmutable, we could just return
			// Pros of break: we can override data
			// Cons: May be a tiny bit more costly, benchmark
			newEntry = false
			break
		}
		if bytes.Equal(slotValue, h.emptyValue) {
			// Found empty slot
			break
		}
		slot = (slot + 1) % h.entries
		offset = slot * h.entrySize
	}
	// Insert
	indexes := make([]byte, 4+4)
	encoding.PutUint32(indexes[0:4], fileIndex)
	encoding.PutUint32(indexes[4:8], fileOffset)
	copy(h.m[offset:offset+keySize], value)
	copy(h.m[offset+keySize:offset+keySize+8], indexes)
	if newEntry {
		h.totalEntries++
	}
	return nil
}

// Get the location of a value. If the value is not found, return a ErrKeyNotFound
// If accessed concurrently you need a read lock
func (h *hashDisk) Get(value []byte) (fileIndex, fileOffset uint32, err error) {
	if bytes.Equal(value, h.emptyValue) {
		return 0, 0, ErrInvalidKey
	}
	slot := hyperloglog.MurmurBytes(value) % h.entries
	offset := slot * h.entrySize
	for { // Try to find value or an empty slot
		slotValue := h.m[offset : offset+keySize]
		if bytes.Equal(slotValue, value) {
			fileIndex = encoding.Uint32(h.m[offset+keySize : offset+keySize+4])
			fileOffset = encoding.Uint32(h.m[offset+keySize+4 : offset+keySize+8])
			return fileIndex, fileOffset, nil
		}
		if bytes.Equal(slotValue, h.emptyValue) {
			// Found empty slot
			return 0, 0, ErrKeyNotFound
		}
		slot = (slot + 1) % h.entries
		offset = slot * h.entrySize
	}
}

// Close the database. It is not safe to call any Set or Get after calling Close
// Flushes all the data to disk
func (h *hashDisk) Close() error {
	err1 := h.m.Unmap() // Flush mmap to the file
	err2 := h.file.Close()
	return firstError(err1, err2)
}
