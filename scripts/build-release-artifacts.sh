#!/usr/bin/env bash
set -eu

version=$(./scripts/check-release-version.sh)
mkdir -p dist

cargo package --workspace --allow-dirty --locked
cargo build --workspace --all-features --bins --release --locked
uv run --no-sync maturin build --release --locked --out dist
uv build --sdist --out-dir dist
helm package charts/pgtask --version "$version" --app-version "$version" --destination dist

if test "${PGTASK_BUILD_IMAGE:-true}" = true; then
    docker build --tag "pgtask:$version" .
fi
