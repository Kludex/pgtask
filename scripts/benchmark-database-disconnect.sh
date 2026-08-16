#!/bin/sh
set -eu

postgres_deployment=pgtask-pgtask-postgres

cleanup() {
    kubectl exec "deployment/$postgres_deployment" -- sh -c 'kill -CONT -1; kill -CONT 1' >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

PGTASK_BENCH_SCENARIO=database-disconnect \
PGTASK_BENCH_TASKS=${PGTASK_BENCH_TASKS:-10000} \
./scripts/run-benchmark.sh &
benchmark_pid=$!

sleep 1
kubectl exec "deployment/$postgres_deployment" -- sh -c 'kill -STOP -1; kill -STOP 1'
sleep 2
kubectl exec "deployment/$postgres_deployment" -- sh -c 'kill -CONT -1; kill -CONT 1'
wait "$benchmark_pid"
