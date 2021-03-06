package kvimd

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValuesDiskSetGet(t *testing.T) {
	// Create DB
	dir, err := ioutil.TempDir("", "valuesdisk")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.valuesdisk")

	v, err := newValuesDisk(path, testFileSize, 0)
	require.NoError(t, err)
	defer v.Close()

	tests := make([][]byte, 100)
	for i := range tests {
		l := rand.Intn(2000) // Size is between 0 & 2000
		v := make([]byte, l)
		if l > 0 {
			randbo.Read(v)
		}
		tests[i] = v
	}
	tests[55] = []byte{} // I want to explicitly test a 0-length value

	offsets := make([]uint32, len(tests))
	for i, test := range tests {
		o, err := v.Set(test)
		require.NoError(t, err)
		offsets[i] = o
	}
	// Now read and verify
	for i, test := range tests {
		val, err := v.Get(offsets[i])
		require.NoError(t, err)
		require.Equal(t, test, val)
	}
}

func TestValuesDiskOpenClose(t *testing.T) {
	// Correctly check that we recover the data after closing/opening DB
	// Create DB
	dir, err := ioutil.TempDir("", "valuesdisk")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.valuesdisk")

	v, err := newValuesDisk(path, testFileSize, 0)
	require.NoError(t, err)

	tests := make([][]byte, 100)
	for i := range tests {
		l := rand.Intn(2000) // Size is between 0 & 2000
		v := make([]byte, l)
		if l > 0 {
			randbo.Read(v)
		}
		tests[i] = v
	}
	tests[55] = []byte{} // I want to explicitly test a 0-length value

	offsets := make([]uint32, len(tests))
	for i, test := range tests {
		o, err := v.Set(test)
		require.NoError(t, err)
		offsets[i] = o
	}
	// Close, reopen, verify
	err = v.Close()
	require.NoError(t, err)
	v, err = newValuesDisk(path, testFileSize, 0)
	require.NoError(t, err)
	defer v.Close()

	// Check that we can write to it after reopening by appending
	postWriteValue := make([]byte, 53)
	randbo.Read(postWriteValue)
	postPosition, postErr := v.Set(postWriteValue)
	require.NoError(t, postErr)
	postWriteResult, err := v.Get(postPosition)
	require.NoError(t, err)
	require.Equal(t, postWriteValue, postWriteResult)

	// Now read and verify that we didn't corrupt
	for i, test := range tests {
		val, err := v.Get(offsets[i])
		require.NoError(t, err)
		require.Equal(t, test, val)
	}
}

func TestValuesDiskLoad(t *testing.T) {
	// Create DB
	dir, err := ioutil.TempDir("", "valuesdisk")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.valuesdisk")

	v, err := newValuesDisk(path, testFileSize, 0)
	require.NoError(t, err)
	defer v.Close()

	expectedLoad := 0.2
	totalWrites := int(expectedLoad / 100 * testFileSize)
	for i := 0; i < totalWrites; i++ {
		data := make([]byte, 100)
		randbo.Read(data)
		v.Set(data)
	}
	l := v.Load()
	require.InDelta(t, expectedLoad, l, 0.01, "Load %v is different than expected %v", l, expectedLoad)
	// 2 load calls should be the same
	require.InDelta(t, l, v.Load(), 0.01)
}

func BenchmarkValuesDiskSet(b *testing.B) {
	valueSize := 8
	// Create DB
	dir, err := ioutil.TempDir("", "valuesdisk")
	require.NoError(b, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.valuesdisk")

	size := benchFileSize
	// Make sure that we don't want to write more that what we can.
	// If we do, then increase the DB size
	neededSize := int(float64(b.N*(valueSize+binary.MaxVarintLen32)) * 1.05)
	if neededSize > size {
		size = neededSize
	}
	v, err := newValuesDisk(path, uint32(size), 0)
	if err != nil {
		b.Fatalf("couldn't create the DB: %s", err)
	}
	defer v.Close()

	value := make([]byte, valueSize)
	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		_, err := v.Set(value)
		if err != nil {
			b.Fatalf("failed to set, you probably need to lower your benchmarking time, err=%s", err)
		}
	}
	b.StopTimer()
}

func BenchmarkValuesDiskGet(b *testing.B) {
	// Create DB
	dir, err := ioutil.TempDir("", "valuesdisk")
	require.NoError(b, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.valuesdisk")

	v, err := newValuesDisk(path, benchFileSize, 0)
	if err != nil {
		b.Fatalf("couldn't create the DB: %s", err)
	}
	defer v.Close()

	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(257))
	offset := uint32(0)
	for i := 0; i < 100; i++ {
		offset, _ = v.Set(value)
	}

	b.SetBytes(8)
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		v.Get(offset)
	}
	b.StopTimer()
}
