#!/usr/bin/env bash
# Vacuity guard for the TLA+ specs.
#
# An invariant also holds in a model that never reaches the interesting states,
# so a clean TLC run on its own proves nothing. Each coverage predicate is the
# negation of a state we need reachable. TLC reporting it violated is the
# witness that the state IS reachable; TLC finding no violation means the model
# has gone inert and the safety results are worthless.
#
#   ./scripts/verify-tla-coverage.sh
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPECS="$REPO/specs"
JAR="${TLA2TOOLS_JAR:-$SPECS/tla2tools.jar}"
JAVA_BIN="${JAVA_BIN:-}"

if [[ -z "$JAVA_BIN" ]]; then
    for candidate in /opt/homebrew/opt/openjdk/bin/java /usr/local/opt/openjdk/bin/java "$(command -v java 2>/dev/null)"; do
        if [[ -n "$candidate" && -x "$candidate" ]]; then JAVA_BIN="$candidate"; break; fi
    done
fi
if [[ -z "$JAVA_BIN" ]]; then
    echo "no java found; set JAVA_BIN" >&2
    exit 1
fi
if [[ ! -f "$JAR" ]]; then
    echo "tla2tools.jar not found at $JAR; set TLA2TOOLS_JAR or run scripts/fetch-tla-tools.sh" >&2
    exit 1
fi

# spec:base-config:coverage predicates
CASES=(
    "WaitProtocol:SignalWait:CoverParks CoverResumes CoverResolved"
    "WaitProtocol:ResultWait:CoverParks CoverResumes NoConcurrentRegistration"
    "TaskLifecycle:TaskLifecycle:CoverRunning CoverRetried CoverStaleHandler CoverExhausted CoverConcurrentHandlers"
)

failures=0

for entry in "${CASES[@]}"; do
    spec="${entry%%:*}"
    rest="${entry#*:}"
    base="${rest%%:*}"
    predicates="${rest#*:}"

    for predicate in $predicates; do
        config="$SPECS/.coverage-$base-$predicate.cfg"
        # Reuse the base config's CONSTANTS, but check only this one predicate.
        {
            grep -E "^(SPECIFICATION|CONSTANTS|[[:space:]]+[A-Za-z_]+ *=)" "$SPECS/$base.cfg" || true
        } > "$config"
        printf 'INVARIANT\n    %s\n' "$predicate" >> "$config"

        output="$(cd "$SPECS" && "$JAVA_BIN" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
            -config "$(basename "$config")" -deadlock "$spec.tla" 2>&1)"
        rm -f "$config"

        if grep -qi "Invariant $predicate is violated" <<<"$output"; then
            echo "  reachable   $spec/$base :: ${predicate#Cover}"
        else
            echo "  UNREACHABLE $spec/$base :: ${predicate#Cover}  <-- model is inert here"
            failures=$((failures + 1))
        fi
    done
done

echo
if (( failures > 0 )); then
    echo "$failures coverage predicate(s) unreachable: the safety results above them are vacuous."
    exit 1
fi
echo "All coverage predicates reachable; the safety runs are meaningful."
