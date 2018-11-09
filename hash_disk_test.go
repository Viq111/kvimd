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

type testCase struct {
	Key []byte
	V1  uint32
	V2  uint32
}

func generateTestCase() testCase {
	val := make([]byte, keySize)
	randbo.Read(val)
	return testCase{
		Key: val,
		V1:  rand.Uint32(),
		V2:  rand.Uint32(),
	}
}

func TestHashDiskWriteRead(t *testing.T) {
	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, testFileSize)
	require.NoError(t, err)
	defer h.Close()

	for i := 0; i < 100; i++ {
		test := generateTestCase()
		err = h.Set(test.Key, test.V1, test.V2)
		require.NoError(t, err)

		returnedA, returnedB, err := h.Get(test.Key)
		require.NoError(t, err)
		require.Equal(t, test.V1, returnedA)
		require.Equal(t, test.V2, returnedB)
	}
}

func TestHashDiskCloseOpen(t *testing.T) {
	// Test that we correctly recover file after reopening
	testCases := 100

	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, testFileSize)
	require.NoError(t, err)

	tests := make([]testCase, testCases)
	for i := 0; i < testCases; i++ {
		test := generateTestCase()
		tests[i] = test
		err = h.Set(test.Key, test.V1, test.V2)
		require.NoError(t, err)
	}

	// Close and reopen
	err = h.Close()
	require.NoError(t, err)
	h, err = newHashDisk(path, testFileSize)
	require.NoError(t, err)
	defer h.Close()

	for _, test := range tests {
		returnedA, returnedB, err := h.Get(test.Key)
		require.NoError(t, err)
		require.Equal(t, test.V1, returnedA)
		require.Equal(t, test.V2, returnedB)
	}
}

func TestHashDiskLoad(t *testing.T) {
	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, testFileSize)
	require.NoError(t, err)
	defer h.Close()

	// At the beginning the load is null (within 0.1%)
	require.InDelta(t, 0, h.Load(), 0.001)

	for i := 0; i < 100; i++ {
		test := generateTestCase()
		err = h.Set(test.Key, test.V1, test.V2)
		require.NoError(t, err)

		returnedA, returnedB, err := h.Get(test.Key)
		require.NoError(t, err)
		require.Equal(t, test.V1, returnedA)
		require.Equal(t, test.V2, returnedB)
	}

	require.True(t, h.Load() > 0)
	require.True(t, h.Load() < 0.1)
}

func BenchmarkHashDiskWrite(b *testing.B) {
	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(b, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, benchFileSize)
	if err != nil {
		b.Fatalf("couldn't create the DB: %s", err)
	}
	defer h.Close()

	b.SetBytes(keySize + 8)
	b.ResetTimer()
	value := make([]byte, keySize)
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		h.Set(value, uint32(i), uint32(i)+3)
	}
}

func BenchmarkHashDiskRead(b *testing.B) {
	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(b, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, benchFileSize)
	if err != nil {
		b.Fatalf("couldn't create the DB: %s", err)
	}
	defer h.Close()

	b.SetBytes(keySize + 8)
	b.ResetTimer()
	value := make([]byte, keySize)
	for i := 1; i < b.N; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		_, _, err = h.Get(value)
		if err != ErrKeyNotFound {
			b.Fatalf("We should not have found any keys, err=%s", err)
		}
	}
}
