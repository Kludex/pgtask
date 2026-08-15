#!/usr/bin/env bash
set -eu

: "${PGTASK_DATABASE_URL:?set PGTASK_DATABASE_URL}"
: "${PGTASK_COVERAGE_OWNER:?set PGTASK_COVERAGE_OWNER to the migration role}"

eval "$(cargo llvm-cov show-env --sh)"
cargo llvm-cov clean --workspace

for artifact in python/pgtask/_native.*.so; do
    if test -e "$artifact"; then
        rm "$artifact"
    fi
done

cargo test --workspace --all-features --all-targets --locked -- --test-threads=1
cargo build --workspace --all-features --bins --locked

for scenario in \
    noop \
    cpu-bound \
    delayed-burst \
    database-disconnect \
    io-bound \
    multi-scheduler \
    rate-limited \
    retained-history \
    retry-storm
do
    PGTASK_BENCH_SCENARIO="$scenario" \
    PGTASK_BENCH_TASKS=2 \
    PGTASK_BENCH_BATCH_SIZE=1 \
    PGTASK_BENCH_WORKERS=1 \
    PGTASK_BENCH_CONCURRENCY=1 \
    PGTASK_BENCH_TIMEOUT_SECONDS=15 \
    PGTASK_BENCH_RETRY_ATTEMPTS=2 \
        target/debug/pgtask-bench >/dev/null
done

PGTASK_BENCH_SCENARIO=worker-death \
PGTASK_BENCH_TASKS=1 \
PGTASK_BENCH_BATCH_SIZE=1 \
PGTASK_BENCH_WORKERS=2 \
PGTASK_BENCH_CONCURRENCY=1 \
PGTASK_BENCH_TIMEOUT_SECONDS=15 \
    target/debug/pgtask-bench >/dev/null

if PGTASK_BENCH_SCENARIO=unknown target/debug/pgtask-bench >/dev/null 2>&1; then
    exit 1
fi
if PGTASK_BENCH_TASKS=0 target/debug/pgtask-bench >/dev/null 2>&1; then
    exit 1
fi
if PGTASK_BENCH_SCENARIO=retry-storm PGTASK_BENCH_RETRY_ATTEMPTS=1 target/debug/pgtask-bench >/dev/null 2>&1; then
    exit 1
fi
if PGTASK_BENCH_SCENARIO=worker-death PGTASK_BENCH_WORKERS=1 target/debug/pgtask-bench >/dev/null 2>&1; then
    exit 1
fi
if uv run --no-sync python -c \
    'import os; env = os.environb.copy(); env[b"PGTASK_BENCH_SCENARIO"] = b"\xff"; os.execve(b"target/debug/pgtask-bench", [b"pgtask-bench"], env)' \
    >/dev/null 2>&1; then
    exit 1
fi
if uv run --no-sync python -c \
    'import os; env = os.environb.copy(); env[b"PGTASK_BENCH_TASKS"] = b"\xff"; os.execve(b"target/debug/pgtask-bench", [b"pgtask-bench"], env)' \
    >/dev/null 2>&1; then
    exit 1
fi

coverage_worker_pid=
coverage_web_pid=
cleanup() {
    if test -n "$coverage_worker_pid"; then
        kill -TERM "$coverage_worker_pid" >/dev/null 2>&1 || true
    fi
    if test -n "$coverage_web_pid"; then
        kill -TERM "$coverage_web_pid" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

PGTASK_QUEUE=coverage-smoke \
PGTASK_CONCURRENCY=1 \
PGTASK_HEALTH_ADDRESS=127.0.0.1:0 \
    target/debug/pgtask-smoke worker >/dev/null 2>&1 &
coverage_worker_pid=$!
sleep 1
PGTASK_QUEUE=coverage-smoke PGTASK_SMOKE_TASKS=2 target/debug/pgtask-smoke enqueue >/dev/null
kill -TERM "$coverage_worker_pid"
wait "$coverage_worker_pid"
coverage_worker_pid=

if target/debug/pgtask-smoke >/dev/null 2>&1; then
    exit 1
fi
if target/debug/pgtask-smoke unknown >/dev/null 2>&1; then
    exit 1
fi
if PGTASK_CONCURRENCY=0 target/debug/pgtask-smoke worker >/dev/null 2>&1; then
    exit 1
fi
if PGTASK_SCHEDULER_ENABLED=invalid target/debug/pgtask-smoke worker >/dev/null 2>&1; then
    exit 1
fi
if PGTASK_QUEUE='' target/debug/pgtask-smoke enqueue >/dev/null 2>&1; then
    exit 1
fi
if uv run --no-sync python -c \
    'import os; env = os.environb.copy(); env[b"PGTASK_CONCURRENCY"] = b"\xff"; os.execve(b"target/debug/pgtask-smoke", [b"pgtask-smoke", b"worker"], env)' \
    >/dev/null 2>&1; then
    exit 1
fi
if uv run --no-sync python -c \
    'import os; env = os.environb.copy(); env[b"PGTASK_SCHEDULER_ENABLED"] = b"\xff"; os.execve(b"target/debug/pgtask-smoke", [b"pgtask-smoke", b"worker"], env)' \
    >/dev/null 2>&1; then
    exit 1
fi

target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" migrate >/dev/null
target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" \
    queue put coverage-cli --terminal-retention-seconds 0 >/dev/null
target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" queue pause coverage-cli >/dev/null
target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" queue resume coverage-cli >/dev/null
if target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" queue pause coverage-cli-missing >/dev/null 2>&1; then
    exit 1
fi
if target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" queue resume coverage-cli-missing >/dev/null 2>&1; then
    exit 1
fi
target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" retention coverage-cli --limit 1 >/dev/null
target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" \
    cancel 00000000-0000-0000-0000-000000000001 >/dev/null
target/debug/pgtask --database-url "$PGTASK_DATABASE_URL" configure-grants \
    --owner "$PGTASK_COVERAGE_OWNER" \
    --producer pgtask_test_producer \
    --worker pgtask_test_worker \
    --observer pgtask_test_observer \
    --administrator pgtask_test_administrator >/dev/null

PGTASK_WEB_ADDRESS=127.0.0.1:58123 target/debug/pgtask-web >/dev/null 2>&1 &
coverage_web_pid=$!
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl --fail --silent http://127.0.0.1:58123/ >/dev/null; then
        break
    fi
    sleep 1
done
curl --fail --silent http://127.0.0.1:58123/ >/dev/null
kill -TERM "$coverage_web_pid"
wait "$coverage_web_pid" || true
coverage_web_pid=

PGTASK_WEB_ADDRESS=127.0.0.1:58124 \
PGTASK_WEB_ADMINISTRATOR=true \
    target/debug/pgtask-web >/dev/null 2>&1 &
coverage_web_pid=$!
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl --fail --silent http://127.0.0.1:58124/ >/dev/null; then
        break
    fi
    sleep 1
done
curl --fail --silent http://127.0.0.1:58124/ >/dev/null
kill -TERM "$coverage_web_pid"
wait "$coverage_web_pid" || true
coverage_web_pid=

cargo llvm-cov report \
    --ignore-filename-regex 'crates/pgtask-python/src/lib.rs' \
    --text \
    --show-missing-lines \
    --output-path target/llvm-cov/missing.txt
cargo llvm-cov report \
    --ignore-filename-regex 'crates/pgtask-python/src/lib.rs' \
    --json \
    --output-path target/llvm-cov/report.json
cargo llvm-cov report \
    --ignore-filename-regex 'crates/pgtask-python/src/lib.rs' \
    --fail-under-lines 98

uv run --no-sync maturin develop --all-features
uv run --no-sync pytest --cov=pgtask --cov=tests --cov-report=term-missing
