#!/usr/bin/env bash
# Write one version into every manifest that carries it.
#
# The release workflow derives the version from the tag and calls this before building, so a release
# does not depend on someone having edited five files to match the tag they pushed.
set -eu

version=${1:?usage: set-release-version.sh <version>}
case $version in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *)
        printf 'version %s is not major.minor.patch\n' "$version" >&2
        exit 1
        ;;
esac

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

# Every crate inherits this, and maturin reads it for the wheel.
perl -0pi -e 's/(\[workspace\.package\].*?\nversion = ")[^"]*(")/${1}'"$version"'${2}/s' Cargo.toml

# Path dependencies also carry a version, because crates.io resolves by version rather than by path.
for manifest in crates/*/Cargo.toml sdks/python/Cargo.toml; do
    perl -pi -e 's/^(pgtask[a-z-]* = \{ path = "[^"]*", version = ")[^"]*(")/${1}'"$version"'${2}/' "$manifest"
done

for package in sdks/typescript/package.json sdks/typescript/package-lock.json; do
    tmp=$(mktemp)
    jq --arg version "$version" '
        .version = $version
        | if .packages? and .packages[""]? then .packages[""].version = $version else . end
    ' "$package" >"$tmp"
    mv "$tmp" "$package"
done

# The chart version tracks the engine, and appVersion is the image tag the chart deploys.
perl -pi -e 's/^version: .*/version: '"$version"'/; s/^appVersion: ".*"/appVersion: "'"$version"'"/' \
    charts/pgtask/Chart.yaml

# Workspace members appear in the lock file, so refresh it or a --locked build fails.
cargo update --workspace --quiet

./scripts/check-release-version.sh
