#!/bin/sh
set -eu

result_dir=$(mktemp -d)
trap 'rm -rf "$result_dir"' EXIT

PGTASK_BENCH_SCENARIO=retry-storm \
    PGTASK_BENCH_TASKS=${PGTASK_BENCH_NOISY_TASKS:-10000} \
    docker compose --profile benchmark run --rm benchmark >"$result_dir/noisy.json" &
noisy_pid=$!

PGTASK_BENCH_SCENARIO=noop \
    PGTASK_BENCH_TASKS=${PGTASK_BENCH_FAST_TASKS:-1000} \
    docker compose --profile benchmark run --rm benchmark >"$result_dir/fast.json" &
fast_pid=$!

wait "$fast_pid"
wait "$noisy_pid"
printf '%s\n' "Fast queue:"
sed -n '/^{/,/^}/p' "$result_dir/fast.json"
printf '%s\n' "Noisy queue:"
sed -n '/^{/,/^}/p' "$result_dir/noisy.json"
