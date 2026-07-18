#!/bin/sh
# Reproducible memory soak for the release gate (flat RSS
# curve). Runs the hermetic run-loop soak (TestSoakMemoryFlat) for a chosen
# duration and tees the heap/RSS series to a timestamped log so the curve is
# inspectable after a long run.
#
#   scripts/soak.sh            # default 24h (the GA gate)
#   scripts/soak.sh 5m         # short belief run
#   LIVCK_SOAK_DURATION=1h scripts/soak.sh
#
# This is the run-loop/buffer/sender/aggregator memory-flatness check. It does
# NOT stand in for the real-collector RSS-vs-MemoryMax=64M validation, which is
# observed against a live enrolled agent. ASCII only.
set -eu

DURATION="${1:-${LIVCK_SOAK_DURATION:-24h}}"

# Resolve the agent repo root (this script lives in <root>/scripts).
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="soak-${TS}.log"

echo "livck-agent soak: duration=${DURATION} log=${LOG}"
echo "(timeout disabled; interrupt with Ctrl-C to stop early)"

# -timeout 0 disables the go test watchdog so a multi-hour run is not killed.
LIVCK_SOAK_DURATION="${DURATION}" go test \
	-run '^TestSoakMemoryFlat$' -v -timeout 0 \
	./internal/runner/ 2>&1 | tee "${LOG}"
