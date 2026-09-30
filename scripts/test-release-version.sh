#!/usr/bin/env bash
set -eu

for version in 0.1.0 1.0.0 12.345.6789; do
    test "$(./scripts/validate-release-version.sh "$version")" = "$version"
done

for version in 1.2 1.2.3.4 01.2.3 1.02.3 1.2.03 1.2.3-alpha '1/;print qq(injected);#.2.3'; do
    if ./scripts/validate-release-version.sh "$version" >/dev/null 2>&1; then
        printf 'accepted invalid release version %s\n' "$version" >&2
        exit 1
    fi
done
