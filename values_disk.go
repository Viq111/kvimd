package kvimd

import (
	"encoding/binary"
	"math"
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

	// Now we will try to reset index to where we can start to append again
	var index uint32
	for index < size {
		valueSize, varintSize := binary.Uvarint(m[index : index+binary.MaxVarintLen32])
		if valueSize == 0 {
			// We don't encode a size anymore, we can start appending now
			break
		}
		if valueSize == math.MaxUint32 { // This is the zero value
			index += uint32(varintSize) // Only need to skip the varint
		} else {
			index += uint32(varintSize) + uint32(valueSize)
		}
	}
	if index >= size { // This should not happen
		return nil, ErrCorrupted
	}

	return &valuesDisk{
		FileIndex: fileIndex,
		MaxSize:   size,
		file:      f,
		index:     index,
		m:         m,
	}, nil
}

// Load returns the ratio of currently used space vs total available
func (v *valuesDisk) Load() float64 {
	index := atomic.LoadUint32(&v.index)
	load := float64(index) / float64(v.MaxSize)
	return load
}

// Set a new value on the valuesDisk DB
// Special case to encode a null value: the length will be == to math.MaxUint32
// This will enable us to treat zero-size as the end of the file (and easily check corruption)
func (v *valuesDisk) Set(value []byte) (uint32, error) {
	length := make([]byte, binary.MaxVarintLen32)
	valueLength := uint64(len(value))
	if len(value) == 0 {
		valueLength = uint64(math.MaxUint32)
	}

	n := binary.PutUvarint(length, valueLength)
	length = length[:n]

	addedSize := len(length) + len(value)
	newIndex := atomic.AddUint32(&v.index, uint32(addedSize))
	if newIndex >= v.MaxSize {
		// We cannot add a negative uint32 and there is no SubUint32 method so we leave it as is
		return 0, ErrNoSpace // We will need to recreate a file
	}
	index := int64(newIndex) - int64(addedSize) // This is the address reserved to us
	copy(v.m[index:index+int64(len(length))], length)
	copy(v.m[index+int64(len(length)):index+int64(len(length)+len(value))], value)
	return uint32(index), nil
}

// Get a value from offset. No check is made that you are querying the correct offset
// Special case to encode a null value: the length will be == to binary.MaxVarintLen32
// This will enable us to treat zero-size as the end of the file (and easily check corruption)
func (v *valuesDisk) Get(offset uint32) ([]byte, error) {
	if offset >= v.MaxSize {
		return nil, ErrNoSpace
	}
	offsetInt := int(offset)
	valueSize, varintSize := binary.Uvarint(v.m[offset : offset+binary.MaxVarintLen32])
	if valueSize == math.MaxUint32 {
		// Special case for 0-value
		ret := make([]byte, 0)
		return ret, nil
	}

	ret := make([]byte, valueSize)
	copy(ret, v.m[offsetInt+varintSize:offsetInt+varintSize+int(valueSize)])

	return ret, nil
}

// Close flushes all the data back to disk.
// It is not safe anymore to call any Get/Set after it has been closed
func (v *valuesDisk) Close() error {
	err1 := v.m.Unmap() // Flush mmap to the file
	err2 := v.file.Close()
	return firstError(err1, err2)
}
