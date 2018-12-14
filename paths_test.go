package kvimd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathsGetDBNumber(t *testing.T) {
	t.Run("hash_disk", func(t *testing.T) {
		n, err := getDBNumber("db1337.hashdisk")
		require.NoError(t, err)
		require.Equal(t, n, 1337)
	})
	t.Run("values_disk", func(t *testing.T) {
		n, err := getDBNumber("db4545.valuesdisk")
		require.NoError(t, err)
		require.Equal(t, n, 4545)
	})
	t.Run("unknown_pattern", func(t *testing.T) {
		_, err := getDBNumber("random_string.a")
		require.Equal(t, err, errUnknownPattern)
	})
}

func TestPathsCreate(t *testing.T) {
	t.Run("hash_disk", func(t *testing.T) {
		s := createHashDiskPath(45)
		n, err := getDBNumber(s)
		require.NoError(t, err)
		require.Equal(t, n, 45)
	})
	t.Run("values_disk", func(t *testing.T) {
		s := createValuesDiskPath(53)
		n, err := getDBNumber(s)
		require.NoError(t, err)
		require.Equal(t, n, 53)
	})
}
