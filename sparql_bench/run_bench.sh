#!/bin/bash
# Timed run: serial per-query latency + concurrency sweep + read-under-write.
# Uses the probe results to size reps per query, and a corpus filtered to the
# queries that completed within budget on BOTH branches.
set -u

ROOT=/Users/umsaka/Documents/DDCPS/NAWI/acquirium_benchmark_suite
WT=/private/tmp/claude-502/-Users-umsaka-Documents-DDCPS-NAWI-acquirium-benchmark-suite/8e2918e3-e969-4353-ab40-299e4677ad51/scratchpad/wt
RES="$ROOT/sparql_bench/results"

for b in main derived; do
  echo "################ TIMED RUN: $b ################"
  "$WT/$b/.venv/bin/python" "$ROOT/sparql_bench/bench.py" \
    --branch "$b" \
    --corpus "$RES/viable_corpus.json" \
    --probes "$RES/probe_${b}.json" \
    --out "$RES/$b.json" \
    --reps 10 --warmup 2 \
    --readers 1,2,4,8,16 --ops-per-reader 40 --mix-size 12 \
    --ruw-ops 15 --ruw-writes 12 \
    > "$RES/$b.stdout" 2> "$RES/$b.stderr"
  echo "$b exit=$?"
  grep -v "^  \[" "$RES/$b.stderr" | tail -22
done
echo "TIMED RUN COMPLETE"
