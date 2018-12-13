#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" = "false" ]
then
    echo "Skipping benchmarks, this is not a pull request"
    exit

fi

# Get utilities
go get golang.org/x/perf/cmd/benchstat

# Run benchmark against current branch
echo "Running benchmarks on current commit..."
go test -run NoTests -bench . -count 5 > /tmp/new

# Run bebchnark against master
git reset --hard $TRAVIS_BRANCH
echo "Running benchmarks on $TRAVIS_BRANCH branch..."
go test -run NoTests -bench . -count 5 > /tmp/master

benchstat /tmp/master /tmp/new
