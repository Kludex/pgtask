#!/bin/sh
set -eu

image="${PGTASK_KIND_IMAGE:-pgtask-kind:test}"
target="${PGTASK_KIND_TARGET:-aarch64-unknown-linux-gnu}"
target_directory="target/$target/release"
staging_directory="$(mktemp -d)"

cleanup() {
    rm -rf "$staging_directory"
}
trap cleanup EXIT INT TERM

for binary in pgtask pgtask-bench pgtask-smoke pgtask-web; do
    test -x "$target_directory/$binary"
    cp "$target_directory/$binary" "$staging_directory/$binary"
done

docker build --pull=false --file Dockerfile.prebuilt --tag "$image" "$staging_directory"
