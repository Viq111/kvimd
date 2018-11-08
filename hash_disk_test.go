package kvimd

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testSize = 512 << 20 // 512Mb
)

func TestWriteRead(t *testing.T) {
	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(t, err)
	defer os.Remove(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, testSize)
	require.NoError(t, err)
	defer h.Close()
	key := []byte("helloworldaaaaaa")
	require.Equal(t, int(keySize), len(key))

	a := uint32(18)
	b := uint32(23)
	err = h.Set(key, a, b)
	require.NoError(t, err)

	aa, bb, err := h.Get(key)
	require.NoError(t, err)
	require.Equal(t, a, aa)
	require.Equal(t, b, bb)
}

func BenchmarkWrite(b *testing.B) {
	// Create DB
	dir, err := ioutil.TempDir("", "hashdisk")
	require.NoError(b, err)
	defer os.Remove(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, testSize)
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
