#!/bin/sh
set -eu

: "${PGTASK_DATABASE_URL:?set PGTASK_DATABASE_URL}"
container="${PGTASK_POSTGRES_CONTAINER:-}"
report="$(mktemp)"
cpu_samples="$(mktemp)"

cleanup() {
    rm -f "$report" "$cpu_samples"
}
trap cleanup EXIT INT TERM

psql "$PGTASK_DATABASE_URL" --set ON_ERROR_STOP=1 --set phase=before --tuples-only --no-align \
    --file scripts/postgres-metrics.sql

cargo run --quiet --package pgtask-bench --bin pgtask-bench >"$report" &
benchmark_pid=$!
while kill -0 "$benchmark_pid" 2>/dev/null; do
    if [ -n "$container" ]; then
        docker stats --no-stream --format '{{.CPUPerc}}' "$container" | tr -d '%' >>"$cpu_samples"
    fi
    sleep 0.2
done
wait "$benchmark_pid"

cat "$report"
if [ -s "$cpu_samples" ]; then
    awk 'BEGIN { maximum = 0 } { if ($1 > maximum) maximum = $1 } END { printf "{\"postgres_cpu_peak_percent\":%.2f}\n", maximum }' "$cpu_samples"
fi

psql "$PGTASK_DATABASE_URL" --set ON_ERROR_STOP=1 --set phase=after --tuples-only --no-align \
    --file scripts/postgres-metrics.sql
