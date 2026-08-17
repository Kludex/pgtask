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

# crates.io rate limits new crates, and this workspace publishes several at once. A 429 names the
# time it will accept the next one, so wait for it rather than failing a release halfway through.
publish_crate() {
    attempt=1
    while true; do
        if output=$(cargo publish --locked -p "$1" 2>&1); then
            printf '%s\n' "$output"
            return 0
        fi
        printf '%s\n' "$output"
        if ! printf '%s' "$output" | grep -q "429 Too Many Requests" || test "$attempt" -ge 5; then
            return 1
        fi
        retry_at=$(printf '%s' "$output" | sed -n 's/.*Please try again after \([^.]*\) and see.*/\1/p')
        wait_seconds=60
        if test -n "$retry_at"; then
            now=$(date -u +%s)
            then=$(date -u -d "$retry_at" +%s 2>/dev/null || echo "")
            if test -n "$then" && test "$then" -gt "$now"; then
                wait_seconds=$((then - now + 15))
            fi
        fi
        echo "crates.io rate limited $1; waiting ${wait_seconds}s before attempt $((attempt + 1))"
        sleep "$wait_seconds"
        attempt=$((attempt + 1))
    done
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
    publish_crate "$crate"
    wait_for_crate "$crate"
done
