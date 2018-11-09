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
