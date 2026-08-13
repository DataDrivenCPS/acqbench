# acquirium branch benchmark — ums-duckdb-fix vs main

Launched 2026-08-12 on morgan. Runs unattended; nothing to babysit.

## What it does

For each branch (**ums-duckdb-fix first**, then main):

1. clone the branch from GitHub, build a venv (uv, python 3.12), `pip install -e .`
2. start a real `acquirium server` (DuckDB backend, fresh data dir, `recreate = true`)
3. push the real `DPRTrailerCSVDriver` at it via `acquirium driver start`, pointed
   at `/home/mete/dpr_1` (200 CSVs, 893 MB)
4. time the ingest from the driver's own per-file "inserted N row(s)" log lines
5. run the two HTTP query templates (`POST /timeseries_info`, `GET /timeseries`)
   across 14 selectivities, warmups discarded
6. shut down, record the `.duckdb` size and a `COUNT(*)` cross-check

The server is started **without** `[[drivers]]` and the driver is pushed
separately, so ingest timing excludes server boot.

## Where things land

| path | contents |
|---|---|
| `launch.log` | full progress log — `tail -f` this |
| `bench.log` | same, appended across runs |
| `results/ums-duckdb-fix.json` | raw numbers, written incrementally |
| `results/main.json` | ditto |
| `results/RESULTS.md` | side-by-side summary, rewritten after each branch |
| `<branch>/run/server.log` | server + driver log for that branch |

## Check status

```sh
tail -f ~/acq_bench/launch.log
cat ~/acq_bench/results/RESULTS.md
pgrep -f bench_branches.py      # empty => finished (or died; check launch.log)
```

## Expected duration

`ums-duckdb-fix` ingests at ~240k rows/s; main is far slower (10–30k rows/s
measured elsewhere). Budget roughly 30 min for the fix branch and up to several
hours for main. Hard caps: 8 h ingest per branch, and it aborts a branch after
30 min with no newly ingested file.

## Notes and caveats

- The `system_metrics` driver from your `acquirium.toml` is omitted so it cannot
  pollute row counts.
- **Issue surfaced during setup:** all 112 `point_uri`s in
  `property_to_column.json` register with `value_kind="text"`, so numeric
  readings land in `text_value` and `value_mode="numeric"` returns nothing for
  them. The driver's `_register_streams_from_lookup()` re-registers the lookup
  bindings without a `value_kind`, clobbering the kind inferred from the data
  (unmapped streams infer correctly: 114 numeric / 35 text). The probe measures
  this per branch as `value_kind_probe.mapped_points_numeric_capable`
  (expected 0 of 20). The numeric-mode query shape therefore addresses an
  unmapped stream by computed ref URI instead.

## Re-run

```sh
rm -rf ~/acq_bench/results ~/acq_bench/ums-duckdb-fix ~/acq_bench/main
setsid ~/acq_bench/launch.sh < /dev/null > /dev/null 2>&1 &
```
