#!/bin/sh
set -eu

for workers in 1 2 4 8 16 32; do
    PGTASK_BENCH_WORKERS="$workers" docker compose --profile benchmark run --rm benchmark
done
