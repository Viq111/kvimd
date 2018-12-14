package kvimd

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

/*
A bit of a hacky way to run only-once (b.N=1) benchmarks through tests (so we don't compile
benchmarks into the main library as well as they wouldn't appear on the doc site).
We cannot create a separate package because hashDisk is an internal struct (but we still want to
benchmark it).
Usage:
EXTENDED_BENCH_FILE env var must be set to a path where we will write the results
You can then call all the tests that starts with TestExtendedBench
Example:
EXTENDED_BENCH_FILE=/tmp/results go test -run TestExtendedBench
*/

// Run one iteration of the benchmark, setting from minLoad to maxLoad
// Return number of keys set
func benchmarkHashDiskSetWithLoad(t *testing.T, minLoad, maxLoad float64) {
	name := fmt.Sprintf("BenchmarkHashDiskSetWithLoad_%.2f-%.2f", minLoad, maxLoad)
	itemSize := int64(keySize + 4 + 4) // Key + 2 uint32
	// Setup
	dir, err := ioutil.TempDir("", "hashdisk")
	if err != nil {
		t.Fatalf("couldn't create the DB: %s", err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, benchFileSize)
	if err != nil {
		t.Fatalf("couldn't create the DB: %s", err)
	}
	defer h.Close()
	// Forces hashDisk to allow load of up to 1
	h.MaxSize = benchFileSize

	// Generate keys until we are at minLoad
	minItems := int(float64(benchFileSize/itemSize) * minLoad)
	if minItems < 1 {
		minItems = 1
	}
	maxItems := int(float64(benchFileSize/itemSize) * maxLoad)
	value := make([]byte, keySize)
	for i := 1; i < minItems; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		h.Set(value, uint32(i), uint32(i)+3)
	}

	start := time.Now()
	// Loop
	for i := minItems; i < maxItems; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		h.Set(value, uint32(i), uint32(i)+3)
	}
	elapsed := time.Now().Sub(start)
	writeExtendedBenchResult(name, maxItems-minItems, elapsed, itemSize)
}

func TestExtendedBenchHashDiskSetLoad(t *testing.T) {
	if !extendedBench {
		t.Skip()
	}
	t.Run("Load_0-0.5", func(t *testing.T) {
		benchmarkHashDiskSetWithLoad(t, 0, 0.5)
	})
	t.Run("Load_0.7-0.9", func(t *testing.T) {
		benchmarkHashDiskSetWithLoad(t, 0.7, 0.9)
	})
	t.Run("Load_0.9-0.95", func(t *testing.T) {
		benchmarkHashDiskSetWithLoad(t, 0.9, 0.95)
	})
	t.Run("Load_0.95-0.99", func(t *testing.T) {
		benchmarkHashDiskSetWithLoad(t, 0.95, 0.99)
	})
}

func benchmarkHashDiskGetWithLoad(t *testing.T, minLoad, maxLoad float64) {
	name := fmt.Sprintf("BenchmarkHashDiskGetWithLoad_%.2f-%.2f", minLoad, maxLoad)
	itemSize := int64(keySize + 4 + 4) // Key + 2 uint32
	// Setup
	dir, err := ioutil.TempDir("", "hashdisk")
	if err != nil {
		t.Fatalf("couldn't create the DB: %s", err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.hashdisk")

	h, err := newHashDisk(path, benchFileSize)
	if err != nil {
		t.Fatalf("couldn't create the DB: %s", err)
	}
	defer h.Close()
	// Forces hashDisk to allow load of up to 1
	h.MaxSize = benchFileSize

	// Generate keys until we are at maxLoad
	minItems := int(float64(benchFileSize/itemSize) * minLoad)
	if minItems < 1 {
		minItems = 1
	}
	maxItems := int(float64(benchFileSize/itemSize) * maxLoad)
	value := make([]byte, keySize)
	for i := 1; i < maxItems; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		h.Set(value, uint32(i), uint32(i)+3)
	}

	start := time.Now()
	// Loop
	for i := minItems; i < maxItems; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		_, _, err := h.Get(value)
		if err != nil {
			t.Errorf("Got an error on get: %s", err)
			t.FailNow()
		}
	}
	elapsed := time.Now().Sub(start)
	writeExtendedBenchResult(name, maxItems-minItems, elapsed, itemSize)
}

func TestExtendedBenchHashDiskGetLoad(t *testing.T) {
	if !extendedBench {
		t.Skip()
	}
	t.Run("Load_0-0.5", func(t *testing.T) {
		benchmarkHashDiskGetWithLoad(t, 0, 0.5)
	})
	t.Run("Load_0.7-0.9", func(t *testing.T) {
		benchmarkHashDiskGetWithLoad(t, 0.7, 0.9)
	})
	t.Run("Load_0.9-0.95", func(t *testing.T) {
		benchmarkHashDiskGetWithLoad(t, 0.9, 0.95)
	})
	t.Run("Load_0.95-0.99", func(t *testing.T) {
		benchmarkHashDiskGetWithLoad(t, 0.95, 0.99)
	})
}
