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
export MERGE_COMMIT=$(git rev-parse HEAD)
echo "Running extended benchmarks on PR branch ..."
time EXTENDED_BENCH_FILE=/tmp/new go test -timeout 0 -run TestExtendedBench -v

# Run bebchnark against master
echo "Running benchmarks on $TRAVIS_BRANCH branch..."
git reset --hard $TRAVIS_BRANCH
time EXTENDED_BENCH_FILE=/tmp/master go test -timeout 0 -run TestExtendedBench -v

git reset --hard $MERGE_COMMIT
echo "#########################################################"
echo "Results:"
benchstat /tmp/master /tmp/new | tee /tmp/diff
go run $(pwd)/.travis/pr_commenter.go "Extended benchmarks" /tmp/diff
