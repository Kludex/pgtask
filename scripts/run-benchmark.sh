#!/bin/sh
set -eu

: "${PGTASK_DATABASE_URL:=postgresql://pgtask:pgtask@localhost:54329/pgtask}"
export PGTASK_DATABASE_URL

exec cargo run --quiet --package pgtask-bench --bin pgtask-bench
