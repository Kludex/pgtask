#!/usr/bin/env bash
# Fetch the TLA+ tools jar that check-tla.sh and verify-tla-coverage.sh need.
# The jar is deliberately not committed; specs/.gitignore excludes it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$REPO/specs/tla2tools.jar"
VERSION="${TLA2TOOLS_VERSION:-v1.7.4}"
URL="https://github.com/tlaplus/tlaplus/releases/download/$VERSION/tla2tools.jar"

if [[ -f "$JAR" ]]; then
    echo "already present: $JAR"
    exit 0
fi

echo "fetching $VERSION"
curl -fsSL "$URL" -o "$JAR.tmp"
mv "$JAR.tmp" "$JAR"
echo "wrote $JAR"
