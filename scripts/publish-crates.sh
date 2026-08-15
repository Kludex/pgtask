#!/usr/bin/env bash
set -eu

version=${1:?pass the release version}
crates=(
    pgtask-core
    pgtask-otel
    pgtask-postgres
    pgtask-worker
    pgtask
    pgtask-cli
    pgtask-web
)

crate_status() {
    curl \
        --silent \
        --show-error \
        --output /dev/null \
        --write-out '%{http_code}' \
        --user-agent 'pgtask-release/0.1 (https://github.com/Kludex/pgtask)' \
        "https://crates.io/api/v1/crates/$1/$version"
}

wait_for_crate() {
    for _attempt in {1..60}; do
        if test "$(crate_status "$1")" = 200; then
            return
        fi
        sleep 5
    done
    echo "crates.io did not index $1 $version within five minutes" >&2
    exit 1
}

for crate in "${crates[@]}"; do
    status=$(crate_status "$crate")
    if test "$status" = 200; then
        echo "$crate $version is already published"
        continue
    fi
    if test "$status" != 404; then
        echo "crates.io returned HTTP $status for $crate $version" >&2
        exit 1
    fi
    cargo publish --locked -p "$crate"
    wait_for_crate "$crate"
done
