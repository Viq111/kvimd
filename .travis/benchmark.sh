#!/bin/bash
# Get utilities
go get golang.org/x/perf/cmd/benchstat

# Run benchmark against current branch
go test -run NoTests -bench . -count 5 > /tmp/new

# Run bebchnark against master
git reset --hard origin/master
go test -run NoTests -bench . -count 5 > /tmp/master

benchstat /tmp/master /tmp/new
