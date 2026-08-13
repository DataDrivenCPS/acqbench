#!/bin/bash
# Probe pass: one watchdog-bounded execution per query.
#
# On main, sparql_query holds the store's global RLock for the whole query, so a
# query abandoned at the budget keeps that lock forever and every later query in
# the process blocks on it. The probe therefore records the timeout, exits, and
# is restarted here at the next index. Results accumulate in an append-only
# JSONL so restarts never lose work.
set -u

ROOT=/Users/umsaka/Documents/DDCPS/NAWI/acquirium_benchmark_suite
WT=/private/tmp/claude-502/-Users-umsaka-Documents-DDCPS-NAWI-acquirium-benchmark-suite/8e2918e3-e969-4353-ab40-299e4677ad51/scratchpad/wt
OUT="$ROOT/sparql_bench/results/probe"
mkdir -p "$OUT"

N=$(python3 -c "import json;print(len(json.load(open('$ROOT/sparql_bench/extracted_queries.json'))))")
echo "corpus size: $N"

for b in main derived; do
  JSONL="$OUT/$b.jsonl"
  : > "$JSONL"
  : > "$OUT/$b.stderr"
  start=0
  while [ "$start" -lt "$N" ]; do
    echo "=== $b resume at $start ===" >> "$OUT/$b.stderr"
    "$WT/$b/.venv/bin/python" "$ROOT/sparql_bench/bench.py" \
      --branch "$b" --probe-only --probe-budget 20 --slice "$start:" \
      --corpus "$ROOT/sparql_bench/extracted_queries.json" \
      --out "$JSONL" \
      >/dev/null 2>> "$OUT/$b.stderr"
    rc=$?
    done_n=$(wc -l < "$JSONL" | tr -d ' ')
    echo "$b: exit=$rc processed=$done_n/$N"
    if [ "$done_n" -le "$start" ]; then
      echo "$b: no progress at index $start, aborting"
      break
    fi
    start=$done_n
  done
  echo "$b PROBE DONE ($(wc -l < "$JSONL" | tr -d ' ') records)"
done
echo "PROBE COMPLETE"
