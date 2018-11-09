package kvimd

import (
	randbo_package "github.com/dustin/randbo"
)

const (
	testFileSize  = 128 << 20  // Tests run wtih file size of 128Mb
	benchFileSize = 1024 << 20 // Benchmarks runs with file size of 1Gb
)

var (
	randbo = randbo_package.New()
)
