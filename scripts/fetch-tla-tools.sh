#!/usr/bin/env bash
# Fetch the TLA+ tools jar that check-tla.sh and verify-tla-coverage.sh need.
# The jar is deliberately not committed; specs/.gitignore excludes it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$REPO/specs/tla2tools.jar"
VERSION="${TLA2TOOLS_VERSION:-v1.7.4}"
SHA256="${TLA2TOOLS_SHA256:-936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88}"
URL="https://github.com/tlaplus/tlaplus/releases/download/$VERSION/tla2tools.jar"

verify() {
    local actual
    actual="$(shasum -a 256 "$1" | cut -d ' ' -f 1)"
    if [[ "$actual" != "$SHA256" ]]; then
        echo "unexpected SHA-256 for $1: $actual" >&2
        return 1
    fi
}

if [[ -f "$JAR" ]]; then
    verify "$JAR"
    echo "already present: $JAR"
    exit 0
fi

trap 'rm -f "$JAR.tmp"' EXIT
echo "fetching $VERSION"
curl -fsSL --retry 3 "$URL" -o "$JAR.tmp"
verify "$JAR.tmp"
mv "$JAR.tmp" "$JAR"
echo "wrote $JAR"
