#!/bin/bash
# sync then async, sequentially, detached
set -u
ROOT=${ROOT:-$HOME/acq_async/bench}; V=$HOME/acq_async/venv; DATA=$HOME/dpr_1000; N=${N:-100}
mkdir -p "$ROOT"; cd "$HOME/acq_async"
TAGSUFFIX=${TAGSUFFIX:-}; PROBE=${PROBE_URI:+--probe-uri "$PROBE_URI"}
for mode in ${MODES:-sync async}; do
  echo "[$(date -u +%FT%TZ)] === $mode$TAGSUFFIX ==="
  "$V/bin/python" "$HOME/acq_async/async_bench.py" --mode $mode --tag "$mode$TAGSUFFIX" --files $N --root "$ROOT" --venv "$V" --data "$DATA" $PROBE 2>&1 | grep -v "^$"
done
echo "[$(date -u +%FT%TZ)] ALL DONE"
