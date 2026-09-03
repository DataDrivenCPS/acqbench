#!/usr/bin/env bash
# Re-measure the 50-app registration storm only, twice per ref, on a fresh
# server each time. Wrapped in caffeinate: the first run of this phase was
# corrupted by the machine idle-sleeping mid-measurement.
set -u

ROOT="${ROOT:?set ROOT}"
WT="${WT:?set WT}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/results"
mkdir -p "$OUT"

for rep in 1 2; do
  for ref in main branch; do
    echo "=== $ref rep$rep"
    caffeinate -dimsu "$WT/$ref/.venv/bin/python" "$HERE/run_phase.py" \
      --ref "$ref" --phase register --root "$ROOT" \
      --counts 50 --interval 2.0 --budget 300 \
      --out "$OUT/register50_${ref}_r${rep}.json" \
      >> "$OUT/$ref.register50.log" 2>&1
    echo "    exit=$?"
    sleep 20   # let Ray and the OS settle between servers
  done
done
echo "ALL DONE"
