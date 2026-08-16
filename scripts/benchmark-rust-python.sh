#!/bin/sh
set -eu

PGTASK_BENCH_SCENARIO=noop cargo run --quiet --package pgtask-bench --bin pgtask-bench
uv run --project sdks/python python scripts/benchmark-python.py
