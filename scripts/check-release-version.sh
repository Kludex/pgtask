#!/usr/bin/env bash
set -eu

versions=$(cargo metadata --locked --no-deps --format-version 1 | jq -r '[.packages[].version] | unique | .[]')
if test "$(printf '%s\n' "$versions" | wc -l | tr -d ' ')" != 1; then
    printf 'workspace packages do not share one version:\n%s\n' "$versions" >&2
    exit 1
fi

version=$versions
app_version=$(sed -n 's/^appVersion: "\([^"]*\)"/\1/p' charts/pgtask/Chart.yaml)
if test "$app_version" != "$version"; then
    printf 'chart appVersion %s does not match engine version %s\n' "$app_version" "$version" >&2
    exit 1
fi

npm_version=$(jq -r .version sdks/typescript/package.json)
if test "$npm_version" != "$version"; then
    printf 'npm package version %s does not match engine version %s\n' "$npm_version" "$version" >&2
    exit 1
fi

if test -n "${RELEASE_VERSION:-}" && test "$RELEASE_VERSION" != "$version"; then
    printf 'release version %s does not match engine version %s\n' "$RELEASE_VERSION" "$version" >&2
    exit 1
fi

printf '%s\n' "$version"
