package kvimd

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	kvimdTestValueAvgSize = 100
)

type kvimdTestCase struct {
	Key   []byte
	Value []byte
}

func generateKvimdTest() kvimdTestCase {
	key := make([]byte, keySize)
	randbo.Read(key)
	valueLen := rand.Intn(kvimdTestValueAvgSize * 2) // Value length will be between 0 and 2*Avg
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

func TestKvimdWriteOnce(t *testing.T) {
	// Setup
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, testFileSize)
	require.NoError(t, err)
	defer func() {
		err = db.Close()
		require.NoError(t, err)
	}()

	// Test that we are actually immutable; i.e: that we don't rewrite a key
	testCase := generateKvimdTest()
	value2 := []byte("second")
	err = db.Write(testCase.Key, testCase.Value)
	require.NoError(t, err)
	err = db.Write(testCase.Key, value2)
	require.NoError(t, err)
	result, err := db.Read(testCase.Key)
	require.NoError(t, err)
	require.Equal(t, result, testCase.Value)
}

func BenchmarkKvimdRandbo(b *testing.B) {
	// Benchmark should to check how fast we can create a test case
	b.SetBytes(keySize + kvimdTestValueAvgSize)
	for i := 0; i < b.N; i++ {
		generateKvimdTest()
	}
}

func BenchmarkKvimdWrite(b *testing.B) {
	dir, err := ioutil.TempDir("", "kvimd")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	// We want to keep the database somewhere (i.e: to run other tests on it), create it there and don't delete at the end
	if out := os.Getenv("KVIMD_OUTPUT_PATH"); out != "" {
		dir = out
	}

	db, err := NewDB(dir, benchFileSize)
	require.NoError(b, err)
	defer func() {
		err = db.Close()
		require.NoError(b, err)
	}()

	b.SetBytes(keySize + kvimdTestValueAvgSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		test := generateKvimdTest()
		err = db.Write(test.Key, test.Value)
		if err != nil {
			b.Fatalf("Failed to write err=%s", err)
		}
	}
	b.StopTimer()
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

	b.SetBytes(keySize + kvimdTestValueAvgSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = db.Read(test.Key)
		if err != nil {
			b.Fatalf("Failed to write err=%s", err)
		}
	}
	b.StopTimer()
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

	b.SetBytes(keySize + kvimdTestValueAvgSize)

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
	b.StopTimer() // Because the defer are slow, stop here
}
