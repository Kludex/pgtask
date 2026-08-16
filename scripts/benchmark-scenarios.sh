#!/bin/sh
set -eu

for scenario in noop cpu-bound io-bound rate-limited delayed-burst retry-storm retained-history; do
    PGTASK_BENCH_SCENARIO="$scenario" ./scripts/run-benchmark.sh
done

PGTASK_BENCH_SCENARIO=worker-death PGTASK_BENCH_WORKERS=2 ./scripts/run-benchmark.sh
PGTASK_BENCH_SCENARIO=multi-scheduler PGTASK_BENCH_WORKERS=4 ./scripts/run-benchmark.sh
