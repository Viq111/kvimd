package kvimd

import (
	"encoding/binary"
	"os"
	"sync/atomic"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

// valuesDisk is a file-backed structure where we write the values of the keys
// It returns the offset at witch the object was written (it's basically a log file)
// It is thread-safe
// Possible improvements:
//   - Do a dicotomy to know what offset to restart on (or read length). This is bc if we crash loop, we will create A LOT of (large) files
type valuesDisk struct {
	FileIndex uint32
	MaxSize   uint32

	file  *os.File
	index uint32 // Current index of the write pointer
	m     mmap.MMap
}

func newValuesDisk(path string, size, fileIndex uint32) (*valuesDisk, error) {
	index := size // We consider for any non-new file that it is full already
	// Open or create the file
	f, err := os.OpenFile(path, os.O_RDWR, 0755)
	if os.IsNotExist(err) {
		// File doesn't exist, create and truncate
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create file")
		}
		err = f.Truncate(int64(size))
		if err != nil {
			return nil, errors.Wrap(err, "failed to resize file")
		}
		index = 0 // For a new file, we can write to it
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, errors.Wrap(err, "failed to get file infos")
	}
	size = uint32(info.Size())

	// Mmap the file
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, errors.Wrap(err, "failed to mmap file")
	}

	return &valuesDisk{
		FileIndex: fileIndex,
		MaxSize:   size,
		file:      f,
		index:     index,
		m:         m,
	}, nil
}

func (v *valuesDisk) Set(value []byte) (uint32, error) {
	length := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(length, uint64(len(value)))
	length = length[:n]

	addedSize := len(length) + len(value)
	newIndex := atomic.AddUint32(&v.index, uint32(addedSize))
	if newIndex >= v.MaxSize {
		// We cannot add a negative uint32 and there is no SubUint32 method so we leave it as is
		return 0, ErrNoSpace // We will need to recreate a file
	}
	index := newIndex - uint32(addedSize) // This is the address reserved to us
	indexInt := int(index)
	copy(v.m[indexInt:indexInt+len(length)], length)
	copy(v.m[indexInt+len(length):indexInt+len(length)+len(value)], value)
	return index, nil
}

func (v *valuesDisk) Get(offset uint32) ([]byte, error) {
	if offset >= v.MaxSize {
		return nil, ErrNoSpace
	}
	offsetInt := int(offset)
	valueSize, varintSize := binary.Uvarint(v.m[offset : offset+binary.MaxVarintLen32])

	ret := make([]byte, valueSize)
	copy(ret, v.m[offsetInt+varintSize:offsetInt+varintSize+int(valueSize)])

	return ret, nil
}

func (v *valuesDisk) Close() error {
	err1 := v.m.Unmap() // Flush mmap to the file
	err2 := v.file.Close()
	return firstError(err1, err2)
}
