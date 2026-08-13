# Graph-store SPARQL bench — `main` vs `graph-store-derived-cache`

Replays real DPR SPARQL queries against a disposable `OxigraphGraphStore` on both
branches and measures query latency, concurrency behaviour, and write/read
interference. Report: `../reports/graph_store_sparql_bench.html`.

## Why this exists

The two branches expose different query APIs and, more importantly, different
locking:

| | `main` | `graph-store-derived-cache` |
|---|---|---|
| API | `sparql_query(q, use_union=)` | `sparql_query(q, include_dependencies=, wait_for_fresh=)` |
| Lock scope | Oxigraph call **and** row materialization | Oxigraph call only |
| Inference | none | `shifty.infer`, background thread, published to a named graph |
| Closure refresh | synchronous, on the caller's thread | deferred to the background rebuild |

`bench.py`'s `Store` adapter normalizes the API so one corpus runs unchanged on
both.

## Pipeline

```
extract_queries.py      .run_logs  ->  extracted_queries.json   (185 unique DPR queries)
run_probe.sh            one watchdog-bounded run per query per branch -> results/probe/*.jsonl
merge_probes.py         -> results/{viable_corpus,divergent,dead,probe_<branch>}.json
run_bench.sh            timed run both branches   -> results/<branch>.json
hol_test.py             head-of-line blocking     -> results/hol_<branch>.json
make_report.py          -> ../reports/graph_store_sparql_bench.html
```

Both branches are installed into their own venv from a detached git worktree of
`../../acquirium`, so neither the working tree nor the branch checkout is touched.
Set `WT` in the two shell scripts to wherever those worktrees live.

## Two traps this harness works around

**Abandoned queries hold the lock.** On `main`, `sparql_query` holds the store's
global `RLock` for the whole query. A query abandoned at a watchdog timeout keeps
holding it, so every later query in that process also times out — the first probe
run produced a cascade of false timeouts. `run_probe.sh` therefore records the
timeout, exits, and restarts at the next index; results accumulate in an
append-only JSONL so restarts never lose work.

**A fixed rep count does not terminate.** Query latencies span 0.3 ms to >20 s.
`plan_reps()` sizes reps from the probe time (10 reps under 20 ms, down to 2 above
2 s) to spend roughly constant time per query instead of a constant rep count.

## Reading the results

The uniform concurrency sweep in `bench.py` is a **negative result** — neither
branch scales, because Oxigraph evaluation is still inside the lock and row
materialization is GIL-bound. The architectural difference only shows up when
query costs differ, which is what `hol_test.py` isolates. Do not quote the sweep
as evidence either way without the head-of-line numbers next to it.

`--heavy-id` pins the expensive query so both branches contend with the same one;
without it each branch picks its own slowest, which is not comparable.
