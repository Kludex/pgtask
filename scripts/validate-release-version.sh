#!/usr/bin/env bash
set -eu

version=${1:?usage: validate-release-version.sh <version>}
if [[ ! $version =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'version %s is not major.minor.patch\n' "$version" >&2
    exit 1
fi

printf '%s\n' "$version"
