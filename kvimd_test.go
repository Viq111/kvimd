package kvimd

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type kvimdTestCase struct {
	Key   []byte
	Value []byte
}

func generateKvimdTest() kvimdTestCase {
	key := make([]byte, keySize)
	randbo.Read(key)
	valueLen := rand.Intn(200) // Value length will be between 0 and 200
	value := make([]byte, valueLen)
	if valueLen > 0 {
		// randbo doesn't like that you read 0 value
		randbo.Read(value)
	}
	return kvimdTestCase{
		Key:   key,
		Value: value,
	}
}

func TestKvimdMaxSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Test that we can only create files that are < 2 << 31-1
	size := uint32(2<<31 - 1)
	_, err = NewDB(dir, size)
	require.Equal(t, ErrFileTooBig, err)
}

func TestKvimdWriteRead(t *testing.T) {
	// Test that we can read what we wrote
	testsSample := 257
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, testFileSize)
	require.NoError(t, err)
	defer func() {
		err = db.Close()
		require.NoError(t, err)
	}()

	tests := make([]kvimdTestCase, testsSample)
	// Add tests and write
	for i := range tests {
		test := generateKvimdTest()
		tests[i] = test
		err = db.Write(test.Key, test.Value)
		require.NoError(t, err)
	}
	// Read test
	for _, test := range tests {
		value, err := db.Read(test.Key)
		require.NoError(t, err)
		require.Equal(t, test.Value, value)
	}
}

func TestKvimdCloseOpen(t *testing.T) {
	// Test that we correctly reload the DB after we close and reopen
	testsSample := 257
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, testFileSize)
	require.NoError(t, err)

	tests := make([]kvimdTestCase, testsSample)
	// Add tests and write
	for i := range tests {
		test := generateKvimdTest()
		tests[i] = test
		err = db.Write(test.Key, test.Value)
		require.NoError(t, err)
	}

	// Now close DB and reopen
	err = db.Close()
	require.NoError(t, err)
	db, err = NewDB(dir, testFileSize)
	require.NoError(t, err)
	defer func() {
		err = db.Close()
		require.NoError(t, err)
	}()

	// Read tests
	for _, test := range tests {
		value, err := db.Read(test.Key)
		require.NoError(t, err)
		require.Equal(t, test.Value, value)
	}
}

func BenchmarkKvimdRandbo(b *testing.B) {
	// Benchmark should to check how fast we can create a test case
	b.SetBytes(keySize + 100)
	for i := 0; i < b.N; i++ {
		generateKvimdTest()
	}
}

func BenchmarkKvimdWrite(b *testing.B) {
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, benchFileSize)
	require.NoError(b, err)
	defer func() {
		err = db.Close()
		require.NoError(b, err)
	}()

	b.SetBytes(keySize + 100) // On average value size is ~100char (rand btw 0 & 200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		test := generateKvimdTest()
		err = db.Write(test.Key, test.Value)
		if err != nil {
			b.Fatalf("Failed to write err=%s", err)
		}
	}
}

func BenchmarkKvimdReadSame(b *testing.B) {
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, benchFileSize)
	require.NoError(b, err)
	defer func() {
		err = db.Close()
		require.NoError(b, err)
	}()

	test := generateKvimdTest()
	err = db.Write(test.Key, test.Value)
	require.NoError(b, err)

	b.SetBytes(int64(len(test.Key) + len(test.Value)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = db.Read(test.Key)
		if err != nil {
			b.Fatalf("Failed to write err=%s", err)
		}
	}
}

func BenchmarkKvimdReadRandom(b *testing.B) {
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, benchFileSize)
	require.NoError(b, err)
	defer func() {
		err = db.Close()
		require.NoError(b, err)
	}()

	b.SetBytes(keySize + 100) // On average value size is ~100char (rand btw 0 & 200)

	testKeys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		test := generateKvimdTest()
		testKeys[i] = test.Key
		err = db.Write(test.Key, test.Value)
		if err != nil {
			b.Fatalf("Failed to write err=%s", err)
		}
	}

	// This is the real test, reread what we just wrote
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Read(testKeys[i])
		if err != nil {
			b.Fatalf("Failed to write err=%s", err)
		}
	}
}
