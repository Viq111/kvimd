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
	// Correctly check that we recover the data after closing/opening DB (and we can't insert anymore)
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

	// Now read and verify
	for i, test := range tests {
		val, err := v.Get(offsets[i])
		require.NoError(t, err)
		require.Equal(t, test, val)
	}

	// Currently check that we cannot write more
	_, err = v.Set(tests[0])
	require.Equal(t, ErrNoSpace, err)
}

func BenchmarkValuesDiskSet(b *testing.B) {
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
	b.SetBytes(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		_, err := v.Set(value)
		if err != nil {
			b.Fatalf("failed to set, you probably need to lower your benchmarking time, err=%s", err)
		}
	}
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
}
