package kvimd

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/DataDog/hyperloglog"
	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const (
	// maxLoad is the load after the one, we will not accept Set anymore
	maxLoad = 0.7
)

var (
	ErrMaxHashSize = errors.New("cannot insert more into hashmap")
)

var (
	encoding = binary.LittleEndian
)

// hashDisk represents a HashMap of constant key and value size.
// It uses mmap internally.
// Currently uses linear probing but might want to implement robin hood hashing
type hashDisk struct {
	MaxSize uint32 // Max number of items we can add into the hash. This is computed by the map itself

	emptyValue   []byte
	entries      uint32
	entrySize    uint32
	size         uint32
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
		size:       uint32(size),
		file:       f,
		m:          m,
	}, nil
}

func (h *hashDisk) Set(value []byte, fileIndex, fileOffset uint32) error {
	if h.totalEntries > h.MaxSize {
		return ErrMaxHashSize
	}
	// Compute hash
	slot := hyperloglog.MurmurBytes(value) % h.entries
	offset := slot * h.entrySize
	for { // Try to find an empty slot
		slotValue := h.m[offset : offset+keySize]
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
	h.totalEntries++
	return nil
}

func (h *hashDisk) Get(value []byte) (fileIndex, fileOffset uint32, err error) {
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

func (h *hashDisk) Close() error {
	err1 := h.m.Unmap() // Flush mmap to the file
	err2 := h.file.Close()
	return firstError(err1, err2)
}
