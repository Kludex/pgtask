#!/bin/sh
set -eu

postgres_container=$(docker compose ps --quiet postgres)
if [ -z "$postgres_container" ]; then
    echo "start the PostgreSQL service with: docker compose up -d postgres" >&2
    exit 1
fi

cleanup() {
    docker unpause "$postgres_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

PGTASK_BENCH_SCENARIO=database-disconnect \
PGTASK_BENCH_TASKS=${PGTASK_BENCH_TASKS:-10000} \
docker compose --profile benchmark run --rm benchmark &
benchmark_pid=$!

sleep 1
docker pause "$postgres_container" >/dev/null
sleep 2
docker unpause "$postgres_container" >/dev/null
wait "$benchmark_pid"
