#!/bin/bash
set -e

if [ "$TRAVIS_PULL_REQUEST" = "false" ]
then
    echo "Skipping benchmarks, this is not a pull request"
    exit

fi

# Get utilities
go get golang.org/x/perf/cmd/benchstat

# Run benchmark against current branch
echo "Running benchmarks on PR branch $(git rev-parse HEAD)..."
time EXTENDED_BENCH_FILE=/tmp/new go test -run TestExtendedBench -v -count 5

# Run bebchnark against master
echo "Running benchmarks on $TRAVIS_BRANCH branch..."
git reset --hard $TRAVIS_BRANCH
time EXTENDED_BENCH_FILE=/tmp/master go test -run TestExtendedBench -v -count 5

echo "#########################################################"
echo "Results:"
benchstat /tmp/master /tmp/new
