#!/bin/sh
set -eu

for workers in 1 2 4 8 16 32; do
    PGTASK_BENCH_WORKERS="$workers" ./scripts/run-benchmark.sh
done
