## Benchmark: `main` vs `ums-duckdb-fix` vs `+ idx_ref` vs `ums-duckdb-fix-2`

Full-stack runs — real `acquirium server` + real `DPRTrailerCSVDriver` pushed over
HTTP, DuckDB backend, fresh store per configuration (`recreate = true`).

| | |
|---|---|
| Data | 200 DPR SCADA CSVs, 893 MB → 139.8M rows across 261 streams |
| Environment | `cloud-vm` container: 4 CPUs, 16 GiB cap, Ubuntu 24.04, Python 3.12 |
| Commits | `main` @ `068e0bc6` · `ums-duckdb-fix` @ `e16f05fa` · `ums-duckdb-fix-2` @ `bc5bc8df` |
| `+ idx_ref` | `ums-duckdb-fix` @ `e16f05fa` plus one line in `ensure_table`:<br>`f"CREATE INDEX IF NOT EXISTS idx_ref ON {TIMESERIES_TABLE} (ref_id)"` |
| Method | ingest timed from the driver's own per-file `inserted N row(s)` lines; queries over HTTP with `Accept: arrow.stream`, iterator fully drained, 20 reps after 3 discarded warmups |
| Verified | index audit + per-table row counts read after shutdown; all four `COUNT(*)` identical at 139,777,767 |

### Ingest

| metric | `main` | `ums-duckdb-fix` | `+ idx_ref` | **`ums-duckdb-fix-2`** |
|---|---:|---:|---:|---:|
| wall time | 51.1 min | 7.7 min | 52.9 min | **6.9 min** |
| throughput | 45,642 rows/s | 302,337 rows/s | 44,061 rows/s | **336,688 rows/s** |
| DB size | 23.95 GB | 7.43 GB | 10.65 GB | **0.64 GB** |
| bytes/row | 171.3 | 53.2 | 76.2 | **4.6** |
| vs `main` (ingest) | — | 6.6× | 0.97× | **7.4×** |
| vs `main` (size) | — | 3.2× smaller | 2.2× smaller | **37× smaller** |

`ums-duckdb-fix-2` is the fastest ingest **and** by far the smallest store — 616 MB
for the same 139.8M rows that cost `main` 23 GB. The size win comes from writing
the cold tier sorted, which lets DuckDB's compression work properly (RLE on the
repeated `ref_id`, delta on the monotonic `ts`); the earlier branch inserted in
arbitrary order and got 53.2 B/row for the same schema.

Its ingest rate of 336,688 rows/s **includes** the mid-ingest query pass that ran
concurrently; excluding that 21.3 s window it is 354,890 rows/s.

### Query latency (p50 ms, 20 reps) — steady state

`ums-duckdb-fix-2` column is the "right after ingestion" pass.

| query | rows | `main` | `ums-duckdb-fix` | `+ idx_ref` | **`fix-2`** |
|---|---:|---:|---:|---:|---:|
| `timeseries_info` batch (112 uris) | — | 287.6 | 154.4 | 152.4 | **762.7** ⚠ |
| `timeseries_info` single | — | 119.0 | 94.9 | 110.2 | **49.6** |
| full range | 535,547 | 183.3 | 398.9 | 428.7 | **119.7** |
| 30 days | 211,095 | 74.0 | 160.3 | 158.8 | **59.0** |
| 7 days | 59,675 | 25.3 | 50.2 | 50.1 | **18.0** |
| 1 day | 8,553 | 6.9 | 10.2 | 10.1 | **6.6** |
| 1 hour | 348 | **3.5** | 4.1 | 4.2 | 4.4 |
| 10 min | 50 | **3.3** | 3.3 | 3.5 | 4.4 |
| 1 min | 4 | **2.9** | 3.1 | 3.2 | 3.7 |
| limit 1000 | 1,000 | 35.2 | **7.3** | 7.4 | 52.5 ⚠ |
| latest 100 desc | 100 | **3.6** | 4.4 | 4.3 | 51.9 ⚠ |
| coalesce, 1 day | 8,553 | 7.7 | 10.8 | 10.6 | **6.6** |
| sparse stream, full range | 535,547 | 178.5 | 396.0 | 427.1 | **120.0** |
| numeric stream, 1 day | 8,553 | 6.8 | 10.4 | 10.4 | **6.3** |
| numeric stream, full range | 535,547 | 179.3 | 397.4 | 508.2 | **118.0** |
| empty range | 0 | **2.4** | 2.6 | 2.5 | 3.7 |

### `ums-duckdb-fix-2`: the three query passes

| query | during ingest | right after | +20 min |
|---|---:|---:|---:|
| `timeseries_info` batch | 77.2 | 762.7 | 994.4 |
| `timeseries_info` single | 22.0 | 49.6 | 49.6 |
| full range | 71.8 *(286,893 rows)* | 119.7 *(535,547)* | 117.0 |
| 30 days | 52.1 | 59.0 | 59.6 |
| 7 days | 18.7 | 18.0 | 18.1 |
| 1 day | 5.9 | 6.6 | 6.0 |
| limit 1000 | 40.4 | 52.5 | 53.5 |
| latest 100 desc | 43.1 | 51.9 | 52.9 |
| sparse full range | 88.8 *(299,701)* | 120.0 *(535,547)* | 119.2 |
| empty range | 3.3 | 3.7 | 3.3 |

Table state after shutdown: **`timeseries_cold` 135,881,559 · `timeseries_hot`
3,897,513 · `timeseries` view 139,777,767.**

### Reading these numbers

- **The 20-minute pass is identical to the "right after" pass.** Nothing
  compacts. `_maybe_flush_hot()` is only ever called from inside an insert, and
  the branch has no timer, background thread, or server endpoint that could
  trigger a flush — so once ingestion stops, the 3.9M rows left in `timeseries_hot`
  (below the 10M threshold) stay there indefinitely, and every read keeps paying
  the union-view cost. **An idle system never settles.** If the intent is for
  reads to get cheaper after a write burst, something has to drain the hot tier —
  a timer, an idle hook, or an explicit `flush_hot()` exposed over HTTP.
- **Querying during ingestion is cheap, which is the good news.** Normalised for
  result size, the mid-ingest pass runs at ~4,000 rows/ms against ~4,475 rows/ms
  when idle — about 11% slower under concurrent writes. The append-only hot tier
  means readers are not parked behind the writer.
- **Wide scans are the big win: 119.7 ms vs `main`'s 183.3 and the fix branch's
  398.9.** Sorted, well-compressed cold storage means far less I/O per scan.
  Combined with 7.4× ingest and a 37× smaller file, this is a strong result.
- **Three shapes regress badly and share one cause — `timeseries` is now a view.**
  `timeseries_info` batch (762.7 ms, 5× worse than the fix branch), `limit 1000`
  (52.5 vs 7.3 ms) and `latest 100 desc` (51.9 vs 4.4 ms) all lose because the
  union + `row_number()` dedup must be materialised before a `LIMIT` or an
  aggregate can be applied — the planner can no longer stop early or read a
  sorted prefix. `latest 100 desc` is the one I would flag hardest: fetching the
  newest 100 points of a stream is a dashboard-grade call, and it went from 4.4 ms
  to 51.9 ms. Cheaper hot-tier dedup, or a fast path that skips the view when hot
  is empty for the queried stream, would recover these.
- **`+ idx_ref` remains a clear no.** It costs 6.9× ingest throughput and +43%
  storage for query latency within noise of the unindexed branch — DuckDB will not
  choose an ART index for a predicate matching 0.4% of rows over 261 distinct keys.
- **Not fixed by any configuration:** all 112 `point_uri`s from
  `property_to_column.json` register with `value_kind="text"`, so
  `value_mode="numeric"` returns nothing for any semantically-addressed point
  (0 of 20 on **all four** configs). Root cause is driver-side —
  `DPRTrailerCSVDriver._register_streams_from_lookup()` re-registers the lookup
  bindings without a `value_kind`, clobbering the kind inferred from the data.

<details>
<summary>Caveats and notes</summary>

- Single run per configuration, single client thread; the only concurrency tested
  is the mid-ingest pass on `fix-2`.
- The "during ingest" pass fired at 103/200 files (71.8M rows), so its absolute
  latencies are over roughly half the data — compare per-row, not per-query.
- Query windows are anchored on a real row (newest timestamp via
  `limit=1&order=desc`), not the midpoint of the reported range: DPR exports
  contain occasional decades-off timestamps that otherwise put windows in a void.
- The `system_metrics` driver was omitted so it could not pollute row counts.
- `main` and `ums-duckdb-fix` were measured at the commits listed above;
  `ums-duckdb-fix` has since advanced to `fd94e01`.
- `Acquirium.insert_graph()` now takes a required keyword-only `source_id`, which
  `dpr_scripts/dpr_csv_driver.py` does not pass — the driver raises `TypeError` in
  `setup()` and never starts on current `ums-duckdb-fix` / `ums-duckdb-fix-2`. The
  benchmark's copy of the driver probes the signature so one script runs on every
  branch; **the checked-in driver still needs this fix.**

</details>
