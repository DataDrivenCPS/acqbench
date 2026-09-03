#!/usr/bin/env bash
# Run every phase for both refs, one server at a time.
#
# Sequential on purpose: two acquirium servers on one laptop would share CPU,
# RAM and the Ray object store, and every number here is a contention number.
set -u

ROOT="${ROOT:?set ROOT to the bench root (templates/, work/)}"
WT="${WT:?set WT to the worktree root holding main/ and branch/}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/results"
mkdir -p "$OUT"

run() {  # run <ref> <phase> <outfile> <extra args...>
  local ref="$1" phase="$2" out="$3"; shift 3
  echo "=== $ref/$phase -> $out"
  "$WT/$ref/.venv/bin/python" "$HERE/run_phase.py" \
    --ref "$ref" --phase "$phase" --root "$ROOT" --out "$OUT/$out" "$@" \
    >> "$OUT/$ref.$phase.log" 2>&1
  echo "    exit=$?"
}

for ref in main branch; do
  run "$ref" register "register_$ref.json"        --counts 1,10,25,50 --interval 2.0 --budget 300
  run "$ref" register "register_serial_$ref.json" --counts 10 --interval 2.0 --budget 300 --serial
  run "$ref" steady   "steady_$ref.json"          --counts 25 --interval 1.0 --seconds 90
  run "$ref" overrun  "overrun_$ref.json"         --interval 1.0 --sleep-s 5.0 --seconds 60 --run-timeout 3.0
done

# EnvSpec exists only on the branch; running it on main records the API error.
for ref in branch main; do
  run "$ref" envspec "envspec_$ref.json" --interval 5.0
done
echo "ALL DONE"
