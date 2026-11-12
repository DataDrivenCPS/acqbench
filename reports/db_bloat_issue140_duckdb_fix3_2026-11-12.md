# Acquirium issue 140: branch `ums-duckdb-fix-3` on 1,000 files, then the full set

**Date:** 2026-11-12
**Issue:** https://github.com/DataDrivenCPS/acquirium/issues/140
**Branch:** `ums-duckdb-fix-3` @ `8cef704` (sections 2–6) @ `64e7dc0` after the PR revisions (section 7), and @ `d0cc2c4` read-path fix, queries only (section 8) (two commits on top of v0.4.0a9: `01ec7aa` drop the `UNIQUE (ref_id, ts)` constraint, `8cef704` stage each write in a temp table and insert it sorted by `(ref_id, ts)`; plus a migration that rebuilds existing tables once to drop the constraint)
**Companion reports:** `db_bloat_issue140_duckdb_2026-11-08.md` (baseline, full set), `db_bloat_issue140_duckdb_variants_2026-11-11.md` (v1/v2/v3 on the same 1,000 files)

**Result in one line:** on 1,000 files fix-3 ingests in **24 min 46 s** (baseline 42 min) into a **3.82 GB** file (baseline 36.1 GB) and reads at **0.86× of baseline p50**, fastest of the four variants on 28 of 50 queries; continuing the same store with the remaining 4,193 files took **1 h 42 min** at a flat 40 files/min with no slowdown from the index-free delete, ending at **3,554,166,660 rows in 20.4 GB (0.86× the 23.6 GB of CSV; the baseline was 188.9 GB)**, i.e. **2 h 07 min total versus 4 h 33 min**. The one problem is on the read side and is not the branch's fault: on the 31,903-row-group table, every windowed read costs ~0.8 s regardless of window size because DuckDB 2.0's `partitioned_execution` optimizer rewrites `ORDER BY ts` into a union of 20 range-partitioned scans, each re-checking zonemaps over all row groups. Disabling that optimizer on read connections brings a 1-hour read from 860 ms to 45 ms and the whole workload from 81 s to 43 s.

---

## 1. What the branch changes

| | baseline `main` | `ums-duckdb-fix-3` |
|---|---|---|
| DDL | `UNIQUE (ref_id, ts)` (ART index, 45 B/row, never compacted) | no unique constraint; `CHECK` and `idx_timeseries_last_revision` kept |
| Write path per batch | `DELETE … USING incoming` (index-assisted) then `INSERT … SELECT` | `CREATE TEMP TABLE staging AS SELECT …` → `DELETE FROM timeseries USING staging` (hash join, no index) → `INSERT … SELECT … FROM staging ORDER BY ref_id, ts` |
| Layout on disk | arrival order | each batch is a run sorted by stream then time; batches themselves are in arrival order |
| Upsert semantics | one row per `(ref_id, ts)` | same (delete-then-insert), verified by the smoke test |
| Migration | — | `drop_timeseries_unique` rebuilds an existing table once (needs space for a second copy) |

The two open questions going in were whether the hash-join delete stays cheap at billions of rows without an index, and what the per-batch sort buys on reads. Both are answered below.

Setup is identical to the variants report (same host, data, driver config, 50-query set `~/var_runs/queries_1000.json`, 10 timed reps + 2 warmup, server restarted with `recreate = false` before each query pass). The branch runs from worktree `~/acq_fix3` with venv `~/venv_fix3`; the main checkout is untouched. Orchestrator: `~/bench/run_fix3.sh` (tmux, unattended), results in `~/var_runs/fix3/phaseA_*`, `phaseB_*`, `phaseC_*`.

---

## 2. Phase A: the same 1,000 files as v1/v2/v3

### Ingest and storage

| | v1 baseline | v2 no index | v3 staging + sorted | **fix-3** |
|---|---|---|---|---|
| Ingest wall (first → last file) | 42 min 05 s | 23 min 40 s | 28 min 41 s + 130 s merge | **24 min 46 s** (1,486 s) |
| Rows/s | 275 k | 489 k | 404 k | **467 k** |
| Rows stored | 694,652,073 | 694,674,021 (dupes) | 694,652,073 | 694,652,073 |
| File size | 36.1 GB | 3.87 GB | 2.78 GB (1.21 live) | **3.82 GB** |
| vs raw 4.50 GB | 8.0× | 0.86× | 0.62× / 0.27× | 0.85× |
| Column data (distinct blocks) | 4.5 GB | 3.7 GB | 1.1 GB | 3.6 GB |
| Indexes + metadata | 31.5 GB | 0.1 GB | 0.2 GB | 0.1 GB |
| Compact copy, data + `last_revision` index | — | 3.68 GB | 1.21 GB | 3.56 GB |
| Row groups / avg rows per row group | 6,115 / 113.6 k | 10,751 / 64.6 k | 5,654 / 122.9 k | 6,147 / 113.0 k |
| Server RSS peak | 11.4 GB | 10.5 GB | 45.5 GB | 9.7 GB |
| Errors | 0 | 0 | 0 | 0 |

Per-column blocks (segment-attributed): `ts` 2.96 GB, `numeric_value` 1.66 GB, `ref_id` 1.55 GB, `text_value` 1.36 GB, `last_revision` 7 MB. The per-batch sort does not change the sizes relative to v2 (every row group still holds all 246 streams, so `ref_id` and `ts` carry the same entropy); what it fixes relative to v2 is the row-group fill: the single `INSERT … ORDER BY` appends one sorted run per batch instead of a parallel append that left half-filled row groups.

### Queries (p50 ms, bold = fastest of the four; last three columns are the full-store passes of sections 3 and 4)

| Metric | v1 baseline | v2 no index | v3 staging + sorted | fix-3 |
|---|---|---|---|---|
| Sum of p50 over 50 queries | 10.82 s | 10.37 s | 9.66 s | **9.48 s** |
| Geomean p50 vs v1, all 50 | 1.00 | 1.13 | **0.84** | 0.86 |
| Same, small reads (≤ 10 k rows, 24) | 1.00 | 1.30 | **0.78** | 0.85 |
| Same, full scans (≥ 1 M rows, 12) | 1.00 | 0.94 | 0.91 | **0.89** |
| Fastest on N of 50 | 2 | 2 | 18 | **28** |
| Row counts equal to v1 | — | no (duplicates) | yes | yes |

| # | Query | Rows (1,000 files) | v1 | v2 | v3 | fix-3 A | Rows (full) | fix-3 B | fix-3 C |
|---|---|---|---|---|---|---|---|---|---|
| 1 | P1 full range | 3,046,745 | 814.7 | 733.5 | 746.7 | **694.1** | 15,609,477 | 5724 | 4155 |
| 2 | P1 full range desc | 3,046,745 | 760.6 | 807.6 | **714.6** | 727.8 | 15,609,477 | 5745 | 4237 |
| 3 | P1 90 days | 719,148 | 195.7 | 217.9 | 194.9 | **194.3** | 719,148 | 1154 | 229 |
| 4 | P1 30 days | 208,988 | 94.4 | 77.2 | 80.3 | **67.6** | 208,988 | 1022 | 103 |
| 5 | P1 7 days | 41,781 | 31.4 | 42.3 | **28.5** | 29.5 | 41,781 | 937 | 69 |
| 6 | P1 1 day | 8,641 | 14.7 | 19.0 | 14.5 | **14.2** | 8,641 | 843 | 50 |
| 7 | P1 6 hours | 2,161 | 11.9 | 15.4 | **10.9** | 11.2 | 2,161 | 829 | 46 |
| 8 | P1 1 hour | 361 | 10.8 | 15.9 | **10.0** | 10.6 | 361 | 860 | 45 |
| 9 | P1 10 min | 61 | 10.5 | 15.0 | **10.3** | 10.6 | 61 | 816 | 43 |
| 10 | P1 1 min | 7 | 11.7 | 15.9 | 10.9 | **10.6** | 7 | 849 | 41 |
| 11 | P1 1 day, 45 days back | 8,640 | 15.6 | 22.8 | 14.9 | **14.3** | 8,640 | 798 | 52 |
| 12 | P1 1 hour, 45 days back | 360 | 11.3 | 16.4 | 11.1 | **10.1** | 360 | 810 | 42 |
| 13 | P2 30 days | 208,988 | 69.2 | 95.0 | **59.0** | 60.7 | 208,988 | 921 | 98 |
| 14 | P2 1 day | 8,641 | 16.2 | 21.4 | 14.5 | **14.1** | 8,641 | 826 | 50 |
| 15 | P2 1 hour | 361 | 11.6 | 15.1 | 10.4 | **10.0** | 361 | 805 | 44 |
| 16 | P2 numeric full | 3,046,745 | 765.0 | 822.7 | **692.1** | 739.5 | 15,610,581 | 5375 | 4087 |
| 17 | T1 text full range | 3,046,745 | 765.4 | 813.2 | 719.1 | **702.8** | 15,609,892 | 5256 | 3986 |
| 18 | T1 text 1 day | 8,641 | 18.0 | 20.4 | 15.6 | **15.0** | 8,641 | 801 | 73 |
| 19 | T1 text 1 hour | 361 | 11.4 | 16.2 | 10.2 | **10.1** | 361 | 803 | 56 |
| 20 | T1 text mode 1 day | 8,641 | 16.9 | 20.8 | 14.7 | **13.9** | 8,641 | 831 | 55 |
| 21 | R1 ref-uri full range | 3,046,745 | 908.8 | 839.1 | 806.8 | **761.9** | 15,595,571 | 5561 | 4046 |
| 22 | R1 ref-uri 7 days | 41,781 | 35.1 | 38.2 | 32.4 | **29.1** | 41,781 | 836 | 80 |
| 23 | P1 latest 1 desc | 1 | 33.1 | 48.9 | **13.2** | 28.8 | 1 | 197 | 194 |
| 24 | P1 latest 100 desc | 100 | 16.6 | 23.9 | 14.9 | **14.4** | 100 | 108 | 97 |
| 25 | P1 limit 1000 asc | 1,000 | 18.0 | 24.7 | **12.3** | 16.2 | 1,000 | 108 | 95 |
| 26 | P1 30 days limit 10000 | 10,000 | 39.0 | 45.2 | **25.0** | 33.6 | 10,000 | 124 | 121 |
| 27 | P1 numeric 30 days | 208,988 | 84.0 | 67.8 | 72.4 | **65.5** | 208,988 | 802 | 103 |
| 28 | P1 coalesce 1 day | 8,641 | 19.4 | 21.0 | **17.8** | 18.6 | 8,641 | 937 | 63 |
| 29 | EMPTY P1 window after data | 0 | **5.8** | 6.2 | 6.1 | 5.8 | 259,195 | 972 | 111 |
| 30 | EMPTY unregistered uri | 0 | 3.6 | 3.9 | **3.6** | 3.7 | 0 | 4 | 4 |
| 31 | P1 1 year | 3,005,443 | 851.5 | 804.4 | 811.9 | **784.6** | 3,005,443 | 1647 | 790 |
| 32 | P1 180 days | 1,425,076 | 358.7 | 378.9 | **355.9** | 375.2 | 1,425,076 | 1279 | 400 |
| 33 | P1 full range numeric | 3,046,737 | 1006.2 | **727.1** | 809.2 | 784.7 | 15,608,208 | 5221 | 4528 |
| 34 | P1 90 days desc | 719,148 | **192.7** | 208.4 | 202.9 | 201.7 | 719,148 | 973 | 277 |
| 35 | P1 90 days limit 100000 | 100,000 | 77.8 | 79.7 | **45.1** | 54.1 | 100,000 | 1996 | 132 |
| 36 | P1 1 year latest 1000 desc | 1,000 | 21.7 | 26.8 | 18.3 | **16.6** | 1,000 | 101 | 102 |
| 37 | P1 1 day, 1 year back | 8,134 | 14.8 | 19.3 | 13.9 | **12.9** | 8,134 | 930 | 54 |
| 38 | P1 1 hour, 1 year back | 354 | 11.2 | 15.8 | 10.3 | **10.0** | 354 | 912 | 46 |
| 39 | P1 7 days, 180 days back | 60,476 | 44.0 | 44.7 | 37.1 | **37.0** | 60,476 | 1020 | 87 |
| 40 | P3 full range | 3,046,745 | 904.8 | **735.1** | 802.9 | 775.2 | 15,609,520 | 5181 | 4194 |
| 41 | P3 30 days | 208,988 | 82.4 | 70.0 | 82.6 | **61.6** | 208,988 | 973 | 128 |
| 42 | P3 1 hour | 361 | 12.2 | 13.8 | 12.0 | **10.1** | 361 | 893 | 57 |
| 43 | P4 sparse full range | 1,099,784 | 287.2 | 280.9 | **268.3** | 268.4 | 10,268,639 | 3964 | 2949 |
| 44 | P4 sparse 30 days | 204,112 | 78.7 | 74.8 | 66.0 | **63.8** | 204,112 | 823 | 118 |
| 45 | T1 text 30 days | 208,988 | 69.3 | 77.9 | 70.0 | **63.0** | 208,988 | 1023 | 118 |
| 46 | T1 latest 1 desc | 1 | 31.3 | 47.2 | **11.3** | 30.1 | 1 | 207 | 216 |
| 47 | T2 text full range | 3,046,745 | 841.6 | 760.3 | 709.1 | **674.1** | 15,613,329 | 5242 | 3917 |
| 48 | R1 ref-uri 1 year | 3,005,443 | 840.2 | 780.2 | 746.7 | **692.5** | 3,005,443 | 1796 | 902 |
| 49 | INFO all live streams | 112 | 220.6 | 225.7 | **188.3** | 214.7 | 112 | 1046 | 1042 |
| 50 | INFO P1 single | 1 | 51.7 | 54.4 | **12.7** | 14.8 | 1 | 66 | 73 |

Fix-3 matches v3 on the windowed reads (zonemaps on `ts` are tight in both) and beats it on the big scans, but does not get v3's point reads: `latest 1 desc` 29 ms vs 13 ms, `INFO P1 single` 15 vs 13, `90 days limit 100000` 54 vs 45. Those need the `ref_id` zonemap to prune, which only the globally sorted layout provides.

---

## 3. Phase B: continue the same store with the remaining 4,193 files

The store from phase A was reopened with `recreate = false` (migration 2 is a no-op on a table created without the constraint) and the driver pointed at `~/data/dpr_rest` (hard links to the 4,193 files not in `dpr1000`, 19.1 GB), so nothing was re-ingested.

| Metric | Value |
|---|---|
| Files | 4,193 (store total 5,193) |
| Rows added / total | 2,859,514,587 / **3,554,166,660** (exactly the baseline full-set count) |
| Wall (first → last file) | **1 h 42 min 17 s** (6,137 s), 466 k rows/s, 41 files/min |
| Total for all 5,193 files (phase A + B) | **2 h 07 min** vs baseline **4 h 33 min** (2.15× faster) |
| File size | 3.82 GB → **20,372,008,960 B (20.4 GB)**; baseline full set 188.9 GB (**9.3× smaller**) |
| vs raw 23.6 GB | **0.86×** (baseline 8.0×) |
| Used / free blocks | 77,586 (20.3 GB) / 127 (33 MB); WAL 0 after shutdown |
| `timeseries` column data (distinct blocks) | 19.6 GB = 5.5 B/row (`ts` 15.9 GB segment-attributed, `numeric_value` 7.5, `ref_id` 7.3, `text_value` 6.9, `last_revision` 0.05) |
| Indexes + metadata | 0.7 GB |
| Compact copy (data + `last_revision` index) | 19.08 GB (119 s) — the file has no slack |
| Row groups | 31,903 (111.4 k rows each) |
| Server RSS peak | 22.5 GB (buffer pool), min MemAvailable 91 GB |
| Errors | 0 |

Rate per 20-minute window (files/min, M rows/min, bytes added per row, server RSS): 38.3 / 29.7 / 4.7 / 8.9 GB → 41.9 / 28.6 / 5.7 / 12.2 → 41.9 / 28.0 / 6.1 / 15.4 → 38.8 / 27.7 / 6.4 / 19.3 → 44.1 / 25.8 / 5.9 / 22.2. **The index-free `DELETE … USING staging` does not get slower as the table grows from 0.7 B to 3.5 B rows**: DuckDB pushes the staging batch's `(ref_id, ts)` min/max into the probe-side scan (join filter pushdown), and a batch covers a few minutes of one file, so the delete touches a handful of row groups. The rows/min drift from 29.7 to 25.8 tracks the later files having more observations per file, not the table size.

Data note: 500,030 rows (one batch, 5 row groups) carry timestamps in year 25 (`0025-02-19`), i.e. one source file writes two-digit years and the driver's `%Y` parses them literally. The store's time range is therefore 0025 → 2026-11-07. It does not affect the measurements here (those 5 row groups are prunable like any other), but it is a data-quality bug worth a `date_format` check.

---

## 4. The read regression on the full store, and what causes it

Replaying the same 50 queries on the full store (phase B) gives:

| | Phase A: 1,000 files, 6,147 row groups | Phase B: 5,193 files, 31,903 row groups, as shipped | Phase C: same store, `partitioned_execution` disabled |
|---|---|---|---|
| Sum of p50, 50 queries | 9.5 s | 81.0 s | 42.6 s |
| Geomean p50 vs phase A, 39 queries whose result did not grow | 1.00 | **20.6×** | 3.1× |
| Geomean p50 vs phase A, 9 full-stream scans (rows grew 5.3×) | 1.00 | 8.0× | 6.1× |
| `P1 1 hour` (361 rows) | 10.6 ms | **860 ms** | 45 ms |
| `P1 1 min` (7 rows) | 10.6 ms | **849 ms** | 41 ms |
| `P1 latest 1 desc` | 28.8 ms | 197 ms | 194 ms |
| `P1 full range` (15.6 M rows) | 694 ms (3.0 M rows) | 5724 ms | 4155 ms |
| `INFO P1 single` | 14.8 ms | 66 ms | 73 ms |

Every windowed read costs ~0.8 s whether it returns 7 rows or 700 k. That is not a scan: `EXPLAIN ANALYZE` reports `Row Groups Scanned: 4 / 31903` for the 1-hour window, and the same query **without** `ORDER BY ts` runs in 35–42 ms.

**Cause: DuckDB 2.0's `partitioned_execution` optimizer.** For `… WHERE ref_id = ? AND ts BETWEEN … ORDER BY ts` the optimizer estimates 188 M output rows (a poor estimate: it only knows `ref_id = ?` selects 1/246 of 3.5 B rows), decides the sort is worth partitioning, and rewrites the plan into a `UNION` of **20** `ORDER BY` branches, each a full table scan with an extra range predicate (`ts < '2022-06-17'`, `ts BETWEEN '2022-06-17' AND '2022-09-10'`, …). Each branch re-evaluates the zonemaps of all 31,903 row groups before pruning to its 0–4 real ones, so the fixed cost is 20 × 31,903 row-group checks ≈ 0.7 s, and 7.4 s with `threads = 1`. On the 1,000-file store (6,147 row groups) the same mechanism costs ~10 ms, which is why none of the variants showed it. Measured on the host (`~/bench/diag_prune5.py`, 1-hour window, 361 rows):

| Variant of the read | p50 |
|---|---|
| as acquirium issues it (`ORDER BY ts`) | 795 ms |
| `SET disabled_optimizers = 'partitioned_execution'` | **29 ms** |
| `WITH t AS MATERIALIZED (filtered rows) SELECT * FROM t ORDER BY ts` | 36 ms |
| no `ORDER BY` | 34 ms |
| disabling any other candidate optimizer (`join_order`, `limit_pushdown`, `top_n`, `reorder_filter`, `statistics_propagation`, …) | 720–850 ms |

**Phase C** re-ran the full 50-query pass on the same store through a copy of the branch whose `_connect()` runs `SET disabled_optimizers='partitioned_execution'` (worktree `~/acq_fix3x`, branch `bench/fix3x`, a 6-line patch). The 39 queries whose result set did not grow drop from 20.6× to 3.1× of the 1,000-file latencies; `P1 1 hour` goes 860 → 45 ms, `P1 1 min` 849 → 41 ms, 30-day windows ~1,000 → ~100 ms. The residual 3–4× over phase A is the honest cost of 5.2× more row groups to zonemap-check once (≈ 1.3 µs per row group). The full-stream scans (15.6 M rows) go 5.7 → 4.2 s, which is Arrow serialisation at ~3.7 M rows/s. LIMIT reads (`latest 1 desc` 195 ms, `limit 1000` 95 ms) do not benefit: they stop early but still have to walk row groups in order until they find the stream's rows, because `ref_id` cannot prune in this layout; the globally sorted layout (v3) is what fixes those.

**Recommendation for the branch:** disable `partitioned_execution` on the store's read connections (or materialise the filtered rows before sorting). It is one line, it is DuckDB-version-specific, and without it any store beyond ~10 k row groups (≈ 1 B rows) pays ~1 s per windowed read. Point and LIMIT reads on large stores will still need either the sorted main table from the variants report or a per-stream partitioning.

---

## 5. Where this leaves issue 140

| | baseline `main` (measured) | `ums-duckdb-fix-3` (measured) |
|---|---|---|
| Full set on disk | 188.9 GB (8.0× raw) | **20.4 GB (0.86× raw)** |
| Full set ingest | 4 h 33 min, decaying to 17 files/min | **2 h 07 min, flat ~40 files/min** |
| Server RSS during ingest | 26.7 GB | 22.5 GB |
| Upsert semantics | yes | yes |
| Windowed reads at 3.5 B rows | not measured on baseline (its ART index is never used for reads, so it would show the same optimizer cost) | 0.8 s as shipped; **45 ms** with `partitioned_execution` disabled |
| Point / LIMIT reads at 3.5 B rows | — | 100–200 ms (row-group walk); v3-style global sort gets these to ~15 ms |

The branch fixes the bloat and the ingest decay. Ship it with the read-connection setting. The globally sorted layout (v3 in the variants report, 1.2 GB per 1,000 files and 13 ms point reads) remains the next step if read latency on big stores matters, with the merge-policy caveats documented there.

---

## 6. Reproduction

| Path on host | Content |
|---|---|
| `~/acq_fix3` (venv `~/venv_fix3`) | worktree of `origin/ums-duckdb-fix-3` @ 8cef704 |
| `~/acq_fix3x` (venv `~/venv_fix3x`) | same + `SET disabled_optimizers='partitioned_execution'` in `_connect()` |
| `~/bench/run_fix3.sh`, `~/bench/run_fix3c.sh` | phases A/B, phase C |
| `~/bench/diag_prune*.py` | the pruning diagnosis (segment stats, EXPLAIN ANALYZE, optimizer toggles) |
| `~/var_runs/fix3/` | `phase{A,B,C}_*`: server logs, progress logs (30 s), `ingest.txt`, `storage.txt`, `blocks.txt`, `compact.txt`, `queries.json`, `queries.stderr`; the 20.4 GB store in `.acquirium/timeseries.duckdb` |
| `~/var_runs/run_fix3.log` | master timeline: phase A 23:55–00:24, B 00:24–02:28, C 02:44–02:54 UTC |
| `~/data/dpr1000`, `~/data/dpr_rest` | the two file sets (hard links into `~/data/dpr_all`) |

---

## 7. Rerun at `64e7dc0` (PR revisions: require DuckDB 2.0, read Arrow via `conn.sql()`, dedupe inside `_insert_frame`)

Same protocol, same files, same query set; previous results kept in `~/var_runs/fix3_8cef704`, this run in `~/var_runs/fix3` (03:08 → 05:35 UTC).

### Ingest and storage

| | 8cef704 | **64e7dc0** |
|---|---|---|
| Phase A, 1,000 files: wall | 24 min 46 s | **24 min 46 s** |
| Phase A: file | 3.82 GB | 3.78 GB |
| Phase B, remaining 4,193 files: wall | 1 h 42 min 17 s | **1 h 38 min 30 s** (5,910 s, 484 k rows/s, 38–44 files/min, flat) |
| All 5,193 files, total | 2 h 07 min | **2 h 03 min** (baseline `main`: 4 h 33 min) |
| Full store: rows | 3,554,166,660 | 3,554,166,660 |
| Full store: file | 20.37 GB | **20.27 GB** (0.86× raw; `main`: 188.9 GB) |
| Full store: column data / indexes+metadata / free | 19.6 / 0.7 / 0.03 GB | 19.5 / 0.7 / 0.03 GB |
| Full store: compact copy with `last_revision` index | 19.08 GB | 18.98 GB |
| Row groups | 31,903 | 31,904 |
| Server RSS peak (phase B) | 22.5 GB | 22.4 GB |
| Errors | 0 | 0 |

Ingest and storage are unchanged within noise; the dedupe moved from `_prepare_frame` into `_insert_frame` costs nothing measurable.

### Queries

| | 8cef704 phase A | **64e7dc0 phase A** | 8cef704 phase B (full store) | **64e7dc0 phase B** | 8cef704 phase C (full, optimizer off) |
|---|---|---|---|---|---|
| Sum of p50, 50 queries | 9.48 s | 9.69 s | 81.0 s | **69.2 s** | 42.6 s |
| Geomean vs `main` (1,000 files) | 0.86 | 0.94 | — | — | — |
| 39 unchanged-result queries, geomean vs own phase A | — | — | 18.8× | 20.6× | 2.9× |
| 9 full-stream scans (rows ×5.3), geomean vs own phase A | — | — | 7.7× | **5.4×** | 5.9× |
| `P1 full range` (15.6 M rows) | 694 ms (3.0 M) | 766 ms (3.0 M) | 5,724 ms | **3,890 ms** | 4,155 ms |
| `P4 sparse full range` (10.3 M rows) | 268 ms | 298 ms | 3,964 ms | **2,808 ms** | 2,949 ms |
| `P1 1 hour` (361 rows) | 10.6 ms | 15.0 ms | 860 ms | 1,077 ms | **45 ms** |
| `P1 1 min` (7 rows) | 10.6 ms | 14.4 ms | 849 ms | 936 ms | **41 ms** |
| `P1 30 days` (209 k rows) | 67.6 ms | 74.4 ms | 1,022 ms | 1,003 ms | **103 ms** |
| `P1 latest 1 desc` | 28.8 ms | 32.8 ms | 197 ms | 229 ms | 194 ms |
| `INFO P1 single` | 14.8 ms | 18.5 ms | 66 ms | 70 ms | 73 ms |

Two effects, both expected from the diff:

- **`conn.sql()` removes the extra row copy on Arrow reads**: the 10–16 M-row scans are 25–30 % faster than at 8cef704 (5.7 → 3.9 s for P1 full range), which is why the phase B total drops from 81 s to 69 s.
- **Small reads pay a few ms more**: the new read path calls `conn.complete()` before the main query, so on the 1,000-file store the sub-10 k-row reads are 1.19× slower than at 8cef704 (10.6 → 15.0 ms for a 1-hour window). Against `main` the branch is still faster overall (0.94 geomean, was 0.86).

**The `partitioned_execution` cost from section 4 is unchanged** at this head: windowed reads on the full store still cost 0.9–1.1 s regardless of window size (phase C at 8cef704 showed 41–45 ms with the optimizer disabled on read connections). The read SQL did not change in these commits, so the recommendation stands.

---

## 8. Read-only rerun at `d0cc2c4` (lookups drained with `fetchall()` instead of `conn.complete()`)

One commit on top of 64e7dc0, read path only: the `ref_ids` and `streams` lookups before the streamed read use `… LIMIT 1` + `fetchall()` so no open result is left on the connection, and the `conn.complete()` call is gone. No ingest; the 50-query set was replayed against the full 5,193-file store from section 7 (`~/var_runs/fix3`, 3.554 B rows, 20.27 GB, 31,904 row groups), server restarted with `recreate = false`, 10 reps + 2 warmup. Files: `phaseD_queries.json` / `phaseD_queries.stderr` (the master-log line for this pass is mislabelled "phase C"; it ran `venv_fix3` at d0cc2c4).

| | 64e7dc0 (section 7) | **d0cc2c4** | 8cef704 + `partitioned_execution` off (section 4) |
|---|---|---|---|
| Sum of p50, 50 queries | 69.2 s | **58.3 s** | 42.6 s |
| Geomean vs 64e7dc0, all 50 | 1.00 | **0.78** | — |
| Geomean vs 64e7dc0, small reads (≤ 10 k rows) | 1.00 | **0.72** | — |
| Geomean vs 64e7dc0, full-stream scans | 1.00 | 0.95 | — |
| 39 unchanged-result queries vs the 1,000-file store | 20.6× | 15.1× | 2.9× |
| `P1 1 hour` (361 rows) | 1,077 ms | **680 ms** | 45 ms |
| `P1 1 min` (7 rows) | 936 ms | **695 ms** | 41 ms |
| `P1 30 days` (209 k rows) | 1,003 ms | **700 ms** | 103 ms |
| `P1 latest 1 desc` | 229 ms | **146 ms** | 194 ms |
| `P1 limit 1000 asc` | 112 ms | **70 ms** | 95 ms |
| `P1 90 days limit 100000` | 2,013 ms | 1,670 ms | 132 ms |
| `P1 full range` (15.6 M rows) | 3,890 ms | **3,438 ms** | 4,155 ms |
| `INFO P1 single` / `INFO all live streams` | 70 / 1,040 ms | 72 / 1,047 ms | 73 / 1,042 ms |
| Errors / row-count differences | 0 / — | 0 / none | — |

The change gives back what section 7 lost and more: small reads are 28 % faster than at 64e7dc0 and LIMIT reads are now the fastest measured on this store (`latest 1` 146 ms, `limit 1000` 70 ms). The windowed reads still carry the `partitioned_execution` cost (0.7 s for a 7-row window on 31,904 row groups; 41–45 ms with the optimizer disabled), so that recommendation is unchanged. `timeseries_info` is unaffected by any of the read-path commits, as expected: it is an aggregate over the stream, not the streamed read.
