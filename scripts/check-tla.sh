#!/usr/bin/env bash
# Model-check the pgtask specs.
#
# Some configurations are expected to FAIL: they model a protocol that is known
# to be broken, and the counterexample TLC prints is the bug report. Each case
# below declares the outcome it expects, so this script fails both when a
# "pass" case regresses and when a "fail" case stops reproducing.
#
#   ./scripts/check-tla.sh
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
[[ -n "$JAVA_BIN" ]] || { echo "no java found; set JAVA_BIN" >&2; exit 1; }
[[ -f "$JAR" ]] || { echo "tla2tools.jar not found at $JAR" >&2; exit 1; }

# spec : config : expected violation (empty means no violation) : what it means
CASES=(
    "TaskLifecycle:TaskLifecycle::lease fencing, retry budget and recovery are sound"
    "TaskLifecycle:TaskLifecycleLarge::the same at 3 tasks, safety only"
    "WaitProtocol:SignalWait::wait_for_signal is serialised against emit_signal"
    "WaitProtocol:ResultWait:NoLostWakeup:the pre-fix result path, kept as the counterexample"
    "WaitProtocol:ResultWaitRecheck:NoLostWakeup:re-reading the source after registering does not fix it"
    "WaitProtocol:ResultWaitFixed::the shipped result path, locking the child row"
)

failures=0

for entry in "${CASES[@]}"; do
    IFS=':' read -r spec config expected_violation description <<<"$entry"
    output="$(cd "$SPECS" && "$JAVA_BIN" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
        -workers auto -config "$config.cfg" -deadlock "$spec.tla" 2>&1)"
    states="$(grep -oE '[0-9.,]+ distinct states found' <<<"$output" | head -1)"

    if [[ -z "$expected_violation" ]] && grep -q "No error has been found" <<<"$output"; then
        echo "  ok       $config [pass as expected, $states] - $description"
    elif [[ -n "$expected_violation" ]] \
        && grep -Fqx "Error: Invariant $expected_violation is violated." <<<"$output"; then
        echo "  ok       $config [$expected_violation failed as expected, $states] - $description"
    elif grep -qE "^Error: (Invariant|Action property|Temporal properties|Property)" <<<"$output"; then
        echo "  MISMATCH $config did not produce the expected result - $description"
        grep -E "^Error:" <<<"$output" | head -3 | sed 's/^/           /'
        failures=$((failures + 1))
    else
        echo "  ERROR    $config - TLC did not run cleanly"
        sed -n '1,15p' <<<"$output" | sed 's/^/           /'
        failures=$((failures + 1))
    fi
done

echo
if (( failures > 0 )); then
    echo "$failures configuration(s) did not match expectations."
    exit 1
fi
echo "All configurations matched expectations."
