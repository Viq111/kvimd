package kvimd

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	randbo_package "github.com/dustin/randbo"
)

const (
	testFileSize  = 128 << 20  // Tests run wtih file size of 128Mb
	benchFileSize = 1024 << 20 // Benchmarks runs with file size of 1Gb
)

var (
	randbo              = randbo_package.New()
	extendedBench       bool
	extendedBenchWriter io.Writer
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

func writeExtendedBenchResult(benchName string, N int, T time.Duration, Bytes int64) {
	r := testing.BenchmarkResult{
		N:     N,
		T:     T,
		Bytes: Bytes,
	}
	line := fmt.Sprintf("%s\t%s\n", benchName, r)
	extendedBenchWriter.Write([]byte(line))
}

func TestMain(m *testing.M) {
	extendedBenchPath := os.Getenv("EXTENDED_BENCH_FILE")
	if extendedBenchPath != "" {
		// We will execute the extended benchmarks
		f, err := os.Create(extendedBenchPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create extended benchmark file: %s, exiting\n", err)
			os.Exit(1)
		}
		defer f.Close()
		extendedBenchWriter = f
		extendedBench = true
	}
	os.Exit(m.Run())
}
