# Acquirium issue 140: three DuckDB write paths on 1,000 DPR files

**Date:** 2026-11-11
**Issue:** https://github.com/DataDrivenCPS/acquirium/issues/140 — "Database gets very bloated compared to raw data"
**Companion reports:** `db_bloat_issue140_duckdb_2026-11-08.md` (full set, baseline: 189 GB, 87 % unique-constraint index), `db_bloat_issue140_timescale_2026-11-09.md`

**Result in one line:** on the same 1,000 files (4.50 GB CSV, 694.7 M observations) the current store writes a **36.1 GB** file in **42 min**; dropping the `UNIQUE (ref_id, ts)` constraint and the delete-before-insert gives **3.87 GB in 24 min** (9.3× smaller, 1.78× faster) but stores cross-file duplicates and is slightly slower on small reads; a staging table with the main table kept sorted by `(ref_id, ts)` gives **1.21 GB live data (2.78 GB file incl. free blocks) in 31 min including the final merge**, keeps upsert semantics, and is the fastest reader (geometric mean p50 0.85× of baseline, point reads 2.5–4× faster). The sorted layout is what makes the difference: zonemaps prune on `ref_id`, and `ts`/`ref_id` compress 3–18× better.

---

## 1. Setup

| Item | Value |
|---|---|
| Host | `exouser@149.165.173.175` (Jetstream VM, Ubuntu 24.04, 32 vCPU / 122 GB, no swap), nothing else running (Timescale container stopped) |
| Base code | public `DataDrivenCPS/acquirium`, `main` @ `1b12d64`; DuckDB 2.0.0, Polars 2.1.0, Ray 2.60.0, Python 3.12 |
| Variants | git worktrees of the host clone: `~/acq_v2` (branch `bench/v2`), `~/acq_v3` (branch `bench/v3`); each with its own venv (`~/venv_v2`, `~/venv_v3`); baseline uses `~/acquirium` + `~/venv_main`. Only `src/acquirium/Storage/duckdb_store.py` differs |
| Data | `~/data/dpr1000`: the first 1,000 files of the full set in sorted filename order (2023-03-06 → 2024-03-07), **4,501,099,832 B = 4.50 GB**, hard links to `~/data/dpr_all` |
| Driver | `backend_bench/dpr_driver.py:DPRDriver`, `interval = 30`, `insert_batch_rows = 50000`, wide CSV format, same config as the earlier reports |
| Server | `timeseries_backend = "duckdb"`, `recreate = true` for ingest; restarted with `recreate = false` for the query pass |
| Orchestration | `~/bench/run_all.sh` in tmux, fully unattended (see section 7); results in `~/var_runs/v1..v3` |
| Query workload | `backend_bench/queries.py`: 50 queries (48 `GET /timeseries` shapes + 2 `POST /timeseries_info`), generated once on v1's store and replayed identically on v2 and v3; per query 1 cold request, 2 warmup, **10 timed** repetitions; p50 reported |

### The three write paths

**v1 — baseline (`main`).** `timeseries(... UNIQUE (ref_id, ts))` plus `idx_timeseries_last_revision`. Each batch: `DELETE FROM timeseries USING incoming WHERE ref_id = … AND ts = …`, then `INSERT … SELECT`. The unique constraint is an ART index that DuckDB persists uncompressed and never compacts.

**v2 — no index, plain insert.** The `UNIQUE (ref_id, ts)` line is removed from the DDL and the DELETE is removed from `_insert_frame`; everything else is unchanged (`idx_timeseries_last_revision` kept). Consequence: a `(ref_id, ts)` pair that appears in two files is stored twice (21,948 extra rows here, 0.003 %), and reads return both copies.

**v3 — staging table + sorted main table.** Batches are appended to `timeseries_staging` (same columns, no constraints). When staging reaches `ACQUIRIUM_DUCKDB_STAGING_ROWS` rows (100 M in this run), or on an explicit `merge_staging(force=True)`, the main table is rebuilt inside the write transaction:

```sql
CREATE TABLE timeseries_new AS
SELECT … FROM (
    SELECT … FROM timeseries AS m ANTI JOIN timeseries_staging AS s USING (ref_id, ts)
    UNION ALL
    SELECT … FROM (SELECT …, row_number() OVER (PARTITION BY ref_id, ts ORDER BY last_revision DESC) rn
                   FROM timeseries_staging) WHERE rn = 1
) ORDER BY ref_id, ts;
DROP TABLE timeseries; ALTER TABLE timeseries_new RENAME TO timeseries;
CREATE INDEX idx_timeseries_last_revision ON timeseries (last_revision);
DROP TABLE timeseries_staging; CREATE TABLE timeseries_staging (…);
```

Staging wins over main on a collision, newest `last_revision` wins inside staging, so the semantics equal the baseline upsert. All reads (`timeseries`, `timeseries_info[_batch]`, `timeseries_exists_batch`, the `timeseries_streams` view) go through a view `timeseries_all = timeseries UNION ALL timeseries_staging`, so unmerged rows are visible immediately. The main table has no unique constraint; because it is written in `(ref_id, ts)` order every row group's min/max zonemap for `ref_id` covers one or two streams and `ts` is monotonic within a stream. The orchestrator calls the final merge offline after the server stops (`~/bench/final_merge.py`). All three variants passed the same smoke test (overlapping inserts, forced merges, windowed/desc/limit reads, sortedness check) before the run.

---

## 2. Ingest

Wall time is first → last `… .csv — inserted` line in `server.log`; v3 adds the final merge, which the orchestrator ran after the server stopped.

| | v1 baseline | v2 no index | v3 staging + sorted |
|---|---|---|---|
| Files | 1,000 | 1,000 | 1,000 |
| Rows logged by the driver | 694,674,021 | 694,674,021 | 694,674,021 |
| Rows stored | 694,652,073 | 694,674,021 (+21,948 duplicates) | 694,652,073 |
| Ingest wall (driver) | **42 min 05 s** (2,525 s) | **23 min 40 s** (1,420 s) | **28 min 41 s** (1,721 s) |
| Final merge (SQL / wall incl. commit + checkpoint) | — | — | 11.2 s / **130 s** |
| Total to a queryable, merged store | 42 min 05 s | 23 min 40 s | **30 min 51 s** (1,851 s) |
| Mean rate (driver wall) | 275 k rows/s | **489 k rows/s** | 404 k rows/s (375 k incl. final merge) |
| Speed-up vs v1 (total) | 1.00× | **1.78×** | 1.36× |
| Server RSS peak | 11.4 GB | 10.5 GB | **45.5 GB** (sort during merges; 7.5 GB between merges) |
| Ray RSS | 3.7 GB | 3.7 GB | 3.7 GB |
| Server shutdown | 11 s | 11 s | 10 s |
| Tracebacks / errors | 0 / 0 | 0 / 0 | 0 / 0 |

### Rate over the run (files per 5-minute window, from `progress.log`)

| Window | v1 | v2 | v3 |
|---|---|---|---|
| 0–5 min | 126 | 202 | 203 |
| 5–10 | 113 | 192 | 183 |
| 10–15 | 114 | 237 | 194 |
| 15–20 | 107 | 202 | 138 |
| 20–25 | 148 | 167 (end at 23:40) | 150 |
| 25–30 | 110 | — | 132 (end at 28:41) |
| 30–35 | 106 | — | — |
| 35–40 | 121 | — | — |
| 40–42 | 55 | — | — |

v1 is index-bound from the first minute (its rate is ~55 % of v2's and does not recover). v2 is flat. v3 matches v2 until the main table is a few hundred million rows, then each merge stalls the driver for longer:

### What a staging merge really costs (v3, from `server.log` timestamps)

| Merge | Main table after | CTAS SQL | Driver stalled (last insert before → first insert after) |
|---|---|---|---|
| 1 | 101 M | 3.7 s | 20 s |
| 2 | 201 M | 5.2 s | 40 s |
| 3 | 302 M | 6.4 s | 58 s |
| 4 | 402 M | 7.5 s | 74 s |
| 5 | 503 M | 8.4 s | 92 s |
| 6 | 603 M | 9.7 s | 109 s |
| final (offline) | 695 M | 11.2 s | 130 s wall |

The SQL itself is cheap (0.016 s per million rows). The stall is the **commit + checkpoint** of the new main table: DuckDB compresses and writes every row group of the rebuilt table (≈ 0.16 s per million rows), and the write lock is held throughout. The six in-run merges stalled ingest for 393 s in total; v3's driver wall minus those stalls is 1,328 s, i.e. the same as v2. With a threshold of 100 M rows the rewrite volume is quadratic in table size, so the threshold must grow with the table (or merges should be time-based and rarer, or only the final merge should run after a bulk load).

---

## 3. Storage

`used` / `free` from `pragma_database_size`; data vs index from distinct block ownership (`duck_blocks.py`); `compact` = `CREATE TABLE AS SELECT * FROM src.timeseries` into a fresh file (order preserved) plus the `last_revision` index, i.e. the live bytes without free blocks and without the unique index.

| | v1 baseline | v2 no index | v3 staging + sorted |
|---|---|---|---|
| File size | **36,076,007,424 B (36.1 GB)** | **3,869,519,872 B (3.87 GB)** | **2,782,670,848 B (2.78 GB)** |
| vs raw CSV (4.50 GB) | **8.0×** | 0.86× | 0.62× (file) / **0.27× (live)** |
| Used blocks | 137,512 = 36.0 GB | 14,614 = 3.83 GB | 4,645 = 1.22 GB |
| Free blocks | 107 = 28 MB | 147 = 39 MB | **5,970 = 1.56 GB** (the dropped pre-merge main table; reused by later writes, never returned to the OS) |
| `timeseries` column data (distinct blocks) | 4.5 GB (6.5 B/row) | 3.7 GB (5.3 B/row) | **1.1 GB (1.6 B/row)** |
| Indexes + metadata | **31.5 GB** (ART for `UNIQUE (ref_id, ts)` ≈ 45 B/row, + `last_revision` 0.2 GB) | 0.1 GB (`last_revision` only) | 0.2 GB (`last_revision` only) |
| Compact copy, data only | — | 3.53 GB | **1.06 GB** |
| Compact copy, + `last_revision` index | — | 3.68 GB | **1.21 GB** |
| Row groups / avg rows per row group | 6,115 / 113.6 k | 10,751 / 64.6 k | 5,654 / 122.9 k (full) |
| Bytes per row, whole file | 51.9 | 5.6 | 4.0 (file) / 1.7 (live) |

### Why the sorted table is 3.4× smaller than the unsorted one

Per-column blocks (`pragma_storage_info`, segment-attributed so columns overlap slightly; relative sizes are what matter):

| Column | v1 unsorted | v2 unsorted | v3 sorted by (ref_id, ts) | Encodings seen |
|---|---|---|---|---|
| `ts` | 2.84 GB | 2.91 GB | **0.86 GB** | BitPacking (deltas are tiny when ts is monotonic per stream) |
| `numeric_value` | 2.07 GB | 1.62 GB | **0.65 GB** | ALP, RLE, Roaring (runs of equal values are contiguous per stream) |
| `text_value` | 1.32 GB | 1.31 GB | **0.65 GB** | DICT_FSST (one stream's status words per row group) |
| `ref_id` | 1.10 GB | 1.45 GB | **0.06 GB** | RLE / Constant (one ref per row group) |
| `last_revision` | 0.23 GB | 0.01 GB | 0.64 GB | RLE: revisions are interleaved once sorted by ref |
| `deleted`, `last_stream_version` | ~0 | ~0 | ~0 | Constant |

In the unsorted layouts a row group holds all 246 streams interleaved, so `ref_id` takes 4 bytes of entropy per row and the `ts` deltas are per-file rather than per-stream. Sorting also fills every row group (122.9 k rows vs 64.6 k in v2, where the parallel append created half-full row groups), so there are half as many row groups to open per query. The one column that gets worse is `last_revision`, which is interleaved after the sort; it is still only 0.6 GB and could be dropped from the sort order's concerns by storing it per batch instead of per row.

---

## 4. Query performance

Same store state for every variant: server restarted with `recreate = false`, so the first request of each query is a cold read from a fresh buffer pool. Query set generated on v1 (`~/var_runs/queries_1000.json`): P1/P2/P3/T1/T2 have 3,046,745 rows each, P4 1,099,784, 112 live streams, anchor 2024-03-11 10:47:16 UTC, first row 2023-03-06. `R1` (the computed `UV-Ultraviolet Intensity` ref) is present in this data. p50 of 10 timed repetitions; bold = fastest; last column is v3 / v1.

| Metric | v1 baseline | v2 no index | v3 staging + sorted |
|---|---|---|---|
| Sum of p50 over 50 queries | 10.82 s | 10.37 s | 9.66 s |
| Geometric mean of p50 ratio vs v1 (all 50) | 1.00 | 1.13 | 0.84 |
| Same, small reads only (≤ 10 k rows, 24 queries) | 1.00 | 1.30 | 0.78 |
| Same, full scans only (≥ 1 M rows, 12 queries) | 1.00 | 0.94 | 0.91 |
| Queries where the variant is fastest | 3 | 7 | 40 |
| Errors / expect-empty mismatches | 0 / 0 | 0 / 0 | 0 / 0 |

| # | Query | Rows | Cold ms v1 / v2 / v3 | v1 p50 | v2 p50 | v3 p50 | v3/v1 |
|---|---|---|---|---|---|---|---|
| 1 | P1 full range | 3,046,745 (v2: 3046838) | 1028 / 1029 / 877 | 814.7 | **733.5** | 746.7 | 0.92 |
| 2 | P1 full range desc | 3,046,745 (v2: 3046838) | 863 / 745 / 781 | 760.6 | 807.6 | **714.6** | 0.94 |
| 3 | P1 90 days | 719,148 | 228 / 260 / 229 | 195.7 | 217.9 | **194.9** | 1.00 |
| 4 | P1 30 days | 208,988 | 101 / 97 / 100 | 94.4 | **77.2** | 80.3 | 0.85 |
| 5 | P1 7 days | 41,781 | 40 / 51 / 36 | 31.4 | 42.3 | **28.5** | 0.91 |
| 6 | P1 1 day | 8,641 | 17 / 22 / 16 | 14.7 | 19.0 | **14.5** | 0.99 |
| 7 | P1 6 hours | 2,161 | 13 / 16 / 12 | 11.9 | 15.4 | **10.9** | 0.92 |
| 8 | P1 1 hour | 361 | 11 / 16 / 11 | 10.8 | 15.9 | **10.0** | 0.93 |
| 9 | P1 10 min | 61 | 12 / 16 / 11 | 10.5 | 15.0 | **10.3** | 0.98 |
| 10 | P1 1 min | 7 | 11 / 16 / 12 | 11.7 | 15.9 | **10.9** | 0.93 |
| 11 | P1 1 day, 45 days back | 8,640 | 17 / 24 / 16 | 15.6 | 22.8 | **14.9** | 0.95 |
| 12 | P1 1 hour, 45 days back | 360 | 12 / 18 / 16 | 11.3 | 16.4 | **11.1** | 0.98 |
| 13 | P2 30 days | 208,988 | 89 / 77 / 84 | 69.2 | 95.0 | **59.0** | 0.85 |
| 14 | P2 1 day | 8,641 | 33 / 34 / 24 | 16.2 | 21.4 | **14.5** | 0.90 |
| 15 | P2 1 hour | 361 | 12 / 17 / 11 | 11.6 | 15.1 | **10.4** | 0.89 |
| 16 | P2 numeric full | 3,046,745 (v2: 3046838) | 782 / 781 / 794 | 765.0 | 822.7 | **692.1** | 0.90 |
| 17 | T1 text full range | 3,046,745 (v2: 3046838) | 773 / 832 / 791 | 765.4 | 813.2 | **719.1** | 0.94 |
| 18 | T1 text 1 day | 8,641 | 47 / 72 / 58 | 18.0 | 20.4 | **15.6** | 0.86 |
| 19 | T1 text 1 hour | 361 | 13 / 17 / 12 | 11.4 | 16.2 | **10.2** | 0.89 |
| 20 | T1 text mode 1 day | 8,641 | 18 / 23 / 17 | 16.9 | 20.8 | **14.7** | 0.87 |
| 21 | R1 ref-uri full range | 3,046,745 (v2: 3046838) | 734 / 768 / 715 | 908.8 | 839.1 | **806.8** | 0.89 |
| 22 | R1 ref-uri 7 days | 41,781 | 72 / 90 / 82 | 35.1 | 38.2 | **32.4** | 0.92 |
| 23 | P1 latest 1 desc | 1 | 46 / 79 / 16 | 33.1 | 48.9 | **13.2** | 0.40 |
| 24 | P1 latest 100 desc | 100 | 19 / 27 / 17 | 16.6 | 23.9 | **14.9** | 0.90 |
| 25 | P1 limit 1000 asc | 1,000 | 18 / 28 / 13 | 18.0 | 24.7 | **12.3** | 0.68 |
| 26 | P1 30 days limit 10000 | 10,000 | 43 / 40 / 25 | 39.0 | 45.2 | **25.0** | 0.64 |
| 27 | P1 numeric 30 days | 208,988 | 78 / 86 / 93 | 84.0 | **67.8** | 72.4 | 0.86 |
| 28 | P1 coalesce 1 day | 8,641 | 31 / 33 / 32 | 19.4 | 21.0 | **17.8** | 0.91 |
| 29 | EMPTY P1 window after data | 0 | 7 / 8 / 7 | **5.8** | 6.2 | 6.1 | 1.04 |
| 30 | EMPTY unregistered uri | 0 | 4 / 4 / 4 | 3.6 | 3.9 | **3.6** | 0.99 |
| 31 | P1 1 year | 3,005,443 (v2: 3005536) | 1011 / 889 / 792 | 851.5 | **804.4** | 811.9 | 0.95 |
| 32 | P1 180 days | 1,425,076 (v2: 1425169) | 425 / 469 / 426 | 358.7 | 378.9 | **355.9** | 0.99 |
| 33 | P1 full range numeric | 3,046,737 (v2: 3046830) | 757 / 713 / 754 | 1006.2 | **727.1** | 809.2 | 0.80 |
| 34 | P1 90 days desc | 719,148 | 258 / 254 / 237 | **192.7** | 208.4 | 202.9 | 1.05 |
| 35 | P1 90 days limit 100000 | 100,000 | 84 / 101 / 61 | 77.8 | 79.7 | **45.1** | 0.58 |
| 36 | P1 1 year latest 1000 desc | 1,000 | 28 / 38 / 20 | 21.7 | 26.8 | **18.3** | 0.84 |
| 37 | P1 1 day, 1 year back | 8,134 | 19 / 22 / 16 | 14.8 | 19.3 | **13.9** | 0.94 |
| 38 | P1 1 hour, 1 year back | 354 | 12 / 16 / 11 | 11.2 | 15.8 | **10.3** | 0.92 |
| 39 | P1 7 days, 180 days back | 60,476 | 44 / 49 / 38 | 44.0 | 44.7 | **37.1** | 0.84 |
| 40 | P3 full range | 3,046,745 (v2: 3046838) | 908 / 772 / 794 | 904.8 | **735.1** | 802.9 | 0.89 |
| 41 | P3 30 days | 208,988 | 113 / 126 / 113 | 82.4 | **70.0** | 82.6 | 1.00 |
| 42 | P3 1 hour | 361 | 24 / 21 / 23 | 12.2 | 13.8 | **12.0** | 0.98 |
| 43 | P4 sparse full range | 1,099,784 (v2: 1099877) | 299 / 260 / 254 | 287.2 | 280.9 | **268.3** | 0.93 |
| 44 | P4 sparse 30 days | 204,112 | 97 / 101 / 100 | 78.7 | 74.8 | **66.0** | 0.84 |
| 45 | T1 text 30 days | 208,988 | 81 / 74 / 71 | **69.3** | 77.9 | 70.0 | 1.01 |
| 46 | T1 latest 1 desc | 1 | 54 / 76 / 27 | 31.3 | 47.2 | **11.3** | 0.36 |
| 47 | T2 text full range | 3,046,745 (v2: 3046838) | 725 / 784 / 712 | 841.6 | 760.3 | **709.1** | 0.84 |
| 48 | R1 ref-uri 1 year | 3,005,443 (v2: 3005536) | 759 / 736 / 708 | 840.2 | 780.2 | **746.7** | 0.89 |
| 49 | INFO all live streams | 112 | 266 / 293 / 232 | 220.6 | 225.7 | **188.3** | 0.85 |
| 50 | INFO P1 single | 1 | 57 / 58 / 21 | 51.7 | 54.4 | **12.7** | 0.25 |

**Reading the table.**

- **Full-stream scans (3 M rows, 0.7–1.0 s) are the same everywhere.** They are bound by Arrow serialisation of 3 M rows over HTTP, not by storage; the unique index was never used for reads in the baseline either.
- **Point and LIMIT reads are where layout shows.** `P1 latest 1 desc` 33 → 13 ms, `T1 latest 1 desc` 31 → 11 ms, `INFO P1 single` 52 → 13 ms, `limit 1000 asc` 18 → 12 ms, `90 days limit 100000` 78 → 45 ms, `30 days limit 10000` 39 → 25 ms. In v1/v2 a `WHERE ref_id = ?` read has to open every row group whose `ts` range matches (all 246 streams share each row group); in v3 the `ref_id` zonemap skips every row group but the stream's own.
- **v2 is slower than v1 on small reads** (geomean 1.13×, e.g. `P1 1 hour` 10.8 → 15.9 ms): same unsorted layout but 10,751 half-filled row groups instead of 6,115, so more segments to probe. Removing the index alone buys nothing on reads.
- **Correctness.** v3 returns exactly the baseline's row counts on all 50 queries. v2 returns 93 extra rows on every P1/T1/T2/R1 full scan (the cross-file duplicates) and would need `DISTINCT ON`/dedup at read time.
- **Cold vs warm** differences are small for all three (sum of cold 11.2 / 11.2 / 10.3 s vs sum of p50 10.8 / 10.4 / 9.7 s); the files are small enough to sit in the page cache after the ingest.

---

## 5. Summary and what to take from it

| | v1 baseline | v2 no index | v3 staging + sorted |
|---|---|---|---|
| File size, 1,000 files | 36.1 GB | 3.87 GB | 2.78 GB (1.21 GB live) |
| Projected full set (3.55 B rows, 23.6 GB raw) | 189 GB (measured) | ≈ 20 GB | ≈ 6 GB live (+ transient free blocks ≈ size of the main table) |
| Ingest, 1,000 files | 42 min | 24 min | 31 min (24 min + merge stalls) |
| Reads (geomean p50 vs v1) | 1.00 | 1.13× slower | **0.85× (faster)**; point reads 2.5–4× faster |
| Upsert semantics kept | yes | **no** (duplicates stored) | yes (staging wins, newest revision wins) |
| Risk / cost | ART index is 87 % of the file and never shrinks | duplicates leak into reads; row groups half-filled | merge = full rewrite of the main table; RSS 45 GB at 600 M rows; 1.5 GB free blocks after each rebuild |

1. **The unique constraint is the bloat and the ingest bottleneck.** Removing it (v2) is 9.3× smaller and 1.8× faster on its own.
2. **Sorting is worth more than the index removal for size and reads.** v3's live data is 3.4× smaller than v2's and reads are faster across the board, because zonemaps on `ref_id` finally work and every column compresses per stream.
3. **The merge needs a better policy than "every 100 M rows".** Each rebuild costs ≈ 0.16 s per million rows in the main table (checkpoint), so the schedule should be size-proportional (e.g. merge when staging ≥ 25 % of main, or on a timer during idle periods), and a bulk load should merge once at the end. Alternatives that avoid full rewrites: partition the main table per `ref_id` range or per month so a merge rewrites only the partitions the staging rows touch, or append sorted staging runs and let reads scan `main ∪ runs` (LSM-style) with periodic compaction.
4. **`last_revision` as a per-row column is now the biggest column in the sorted layout** (0.64 GB of 1.1 GB). Its index is small, but the column itself could live in a per-batch table keyed by `(ref_id, ts range)` instead.
5. **Free blocks after a rebuild** are reused by later inserts but DuckDB does not truncate the file; a `COPY`/compact step after a bulk load (18 s here) returns the file to its live size.

---

## 6. Caveats

- One run per variant; the 50-query p50s are stable (p95 within 10–20 % of p50) but single-run ingest walls carry a few percent of noise (v1's 20–25 min window shows a +35 % blip).
- v3's final merge ran outside the server (`final_merge.py`), so its 130 s is not in the driver wall; it is added in the "total" row.
- v3's 45 GB RSS is the sort of 500–600 M rows inside DuckDB's buffer pool (memory limit = 80 % of RAM). On a smaller host the merge would spill to disk and take longer.
- The 1,000 files are the oldest 1,000 of the set, time-ordered; a stream-interleaved arrival order would not change the sorted layout's result but would make v1/v2's `ts` zonemaps worse than measured.
- `timeseries_exists_batch` / `timeseries_info_batch` in v3 scan `main UNION ALL staging`; after the final merge staging is empty, so the union costs nothing here.

---

## 7. Reproduction

Host paths:

| Path | Content |
|---|---|
| `~/bench/run_all.sh` | the orchestrator: per variant, fresh run dir, server boot, driver, per-30 s `progress.log`, driver stop, clean shutdown, (v3) `final_merge.py`, `duck_storage.py` / `duck_blocks.py` / `duck_compact.py`, restart with `recreate = false`, `queries.py gen` (v1 only) and `queries.py run` |
| `~/bench/variants/v2/duckdb_store.py`, `~/bench/variants/v3/duckdb_store.py` | the two patched stores (also in the worktrees `~/acq_v2`, `~/acq_v3`) |
| `~/bench/test_store.py` | the smoke test run before launch |
| `~/var_runs/<v>/` | `server.toml`, `driver.toml`, `server_clean.log`, `progress.log`, `ingest.txt`, `storage.txt`, `blocks.txt`, `compact.txt`, `queries.json`, `queries.stderr`, `final_merge.log` (v3) |
| `~/var_runs/queries_1000.json` | the frozen 50-query set |
| `~/var_runs/run_all.log` | master timeline (21:20 → 23:11 UTC, 1 h 51 min for all three) |

Local copies of the patched stores, the orchestrator and all result files are in this session's scratchpad (`variants/` and `var_results/`).
