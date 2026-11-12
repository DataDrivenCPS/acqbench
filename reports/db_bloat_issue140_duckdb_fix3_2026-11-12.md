# Acquirium issue 140: branch `ums-duckdb-fix-3` on 1,000 files, then the full set

**Date:** 2026-11-12
**Issue:** https://github.com/DataDrivenCPS/acquirium/issues/140
**Branch:** `ums-duckdb-fix-3` @ `8cef704` (sections 2–6) @ `64e7dc0` after the PR revisions (section 7), @ `d0cc2c4` read-path fix, queries only (section 8), @ `2215185` materialized-CTE read with a 300-query set (section 9), and @ `0a8eb80` CTE only for windowed non-LIMIT reads (section 10) (two commits on top of v0.4.0a9: `01ec7aa` drop the `UNIQUE (ref_id, ts)` constraint, `8cef704` stage each write in a temp table and insert it sorted by `(ref_id, ts)`; plus a migration that rebuilds existing tables once to drop the constraint)
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

---

## 9. 300-query comparison: `d0cc2c4` vs `2215185` (filter in a `MATERIALIZED` CTE before sorting)

The read path at 2215185 wraps the filtered scan in `WITH filtered AS MATERIALIZED (…) SELECT … FROM filtered ORDER BY ts [LIMIT n]`, to sidestep the `partitioned_execution` rewrite from section 4. To test it on more than the original 50 shapes, 250 queries were generated once with a fixed seed over the 112 live streams (each window placed inside the stream's own time range; `~/bench/queries300.py`, set `~/var_runs/queries_300.json`): 100 windows (1 min … 90 d), 50 LIMIT reads (1 … 10,000, asc/desc, a third inside a 30-day window), 40 value-mode variants, 10 full-stream reads, 20 one-year windows, 30 `timeseries_info` batches (1 … 112 streams). Both passes ran read-only on the same full store (3.554 B rows, 31,904 row groups), 10 reps + 2 warmup, server restarted between them; no errors, identical row counts on all 300.

| Group | n | median p50 at d0cc2c4 | median p50 at 2215185 | geomean 2215185 / d0cc2c4 |
|---|---|---|---|---|
| original 50 | 50 | 635 ms | 118 ms | 0.41 |
| W windows (1 min – 90 d) | 100 | 589 ms | **51 ms** | **0.11** |
| M value modes (1 h – 7 d windows) | 40 | 615 ms | **58 ms** | **0.11** |
| L LIMIT reads | 50 | 103 ms | **2,625 ms** | **7.56** |
| F full-stream reads (10–16 M rows) | 10 | 3,650 ms | 7,057 ms | 1.89 |
| Y one-year windows (3 M rows) | 20 | 1,243 ms | 1,265 ms | 0.91 |
| I `timeseries_info` batches | 30 | 317 ms | 323 ms | 1.01 |
| all 300 | 300 | sum 221.5 s | sum 295.5 s | 0.44 |

By read shape rather than group: windowed reads returning ≤ 10 k rows without a LIMIT (86 queries) go from a median of 584 ms to **49 ms** (geomean 0.083); LIMIT reads (57) go from 103 ms to **2,603 ms** (geomean 7.3); scans of ≥ 1 M rows (43) are 1.33× slower.

**Why.** The CTE fixes exactly what it was meant to fix, but materialising the filter result has two costs: (1) a LIMIT can no longer stop early — `… WHERE ref_id = ? ORDER BY ts DESC LIMIT 1` now materialises the stream's 15 M rows before taking one, so `latest 1 desc` goes 167 → 2,603 ms and `limit 1000 asc` 77 → 2,679 ms; (2) a full-stream read is copied once more, 3.5 → 7.5 s for 15.6 M rows. Only LIMIT reads that also carry a window (the 17 of the 50 L queries with a 30-day range, and the original `30 days limit 10000`) improve, because the window bounds the materialisation.

Direct measurement of the three plan shapes in DuckDB on the same file (`~/bench/diag_cte.py`, best of 3):

| Read | plain (d0cc2c4) | `MATERIALIZED` CTE (2215185) | plain + `SET disabled_optimizers='partitioned_execution'` |
|---|---|---|---|
| 1-hour window, 361 rows | 616 ms | 28.5 ms | **27.2 ms** |
| `ORDER BY ts DESC LIMIT 1` | 153 ms | 2,461 ms | **165 ms** |
| `ORDER BY ts LIMIT 1000` | 65 ms | 2,525 ms | **73 ms** |
| 30-day window `LIMIT 10000` | 93 ms | **50 ms** | 92 ms |
| full stream, 13.5 M rows | 6,233 ms | 9,435 ms | **5,638 ms** |

**Recommendation.** Keep the plain query and disable the optimizer on the read connection (`SET disabled_optimizers = 'partitioned_execution'` in `_connect()`, or per statement): it gets the CTE's windowed-read gain (27 ms) while keeping LIMIT reads streaming (65–165 ms) and full scans at their best (5.6 s). If a SQL-only fix is preferred, apply the CTE only when the read has a time window and no LIMIT; a LIMIT read must stay a plain Top-N over the scan. Files: `~/var_runs/fix3/phaseE_*` (d0cc2c4), `phaseF_*` (2215185).

---

## 10. 300 queries at `0a8eb80` (CTE only for windowed reads without LIMIT)

Same store, same 300 queries, same runner (`phaseG_*`, 16:15–16:47 UTC); no errors, row counts identical to both previous passes.

| Group | n | median p50 d0cc2c4 | median p50 2215185 | **median p50 0a8eb80** | 0a8eb80 / d0cc2c4 | 0a8eb80 / 2215185 |
|---|---|---|---|---|---|---|
| original 50 | 50 | 635 ms | 118 ms | **115 ms** | 0.30 | 0.73 |
| W windows (1 min – 90 d) | 100 | 589 ms | 51 ms | **41 ms** | 0.09 | 0.83 |
| M value modes | 40 | 615 ms | 58 ms | **42 ms** | 0.09 | 0.79 |
| L LIMIT reads | 50 | 103 ms | 2,625 ms | **111 ms** | 1.06 | 0.14 |
| F full-stream reads | 10 | 3,650 ms | 7,057 ms | **3,861 ms** | 1.06 | 0.56 |
| Y one-year windows (3 M rows) | 20 | 1,243 ms | 1,265 ms | 1,382 ms | 1.04 | 1.15 |
| I `timeseries_info` batches | 30 | 317 ms | 323 ms | 316 ms | 0.98 | 0.96 |
| all 300, sum of p50 | 300 | 221.5 s | 295.5 s | **142.6 s** | 0.27 | 0.61 |

By read shape: windowed reads ≤ 10 k rows without LIMIT (86) median 584 → **40 ms** (geomean 0.068 vs d0cc2c4); LIMIT reads without a window (37) 101 → 111 ms and with a window (20) 105 → 109 ms, i.e. back to the plain top-N; scans ≥ 1 M rows (43) 1.10× d0cc2c4.

This head keeps the CTE's gain on the reads it was meant for and removes the LIMIT and full-scan regressions of 2215185: `P1 1 hour` 650 → 39 ms, `latest 1 desc` 167 → 160 ms, `limit 1000` 77 → 82 ms, `P1 full range` 3.5 → 3.95 s. The one residual is large windowed reads that take the CTE path: the 20 one-year windows (3 M rows each) are 1.15× slower than at d0cc2c4 (worst 1.7 → 2.9 s), which is the extra copy of a multi-million-row materialisation. The `SET disabled_optimizers='partitioned_execution'` route from section 9 does not pay that (same 27 ms on the small window, 5.6 s vs 6.2 s on a full stream), so if those large-window reads matter it remains the better option; otherwise 0a8eb80 is the best measured read path on this branch.

Summary of the read path across the heads, full store (3.554 B rows), medians over the 300-query set unless noted:

| | windowed ≤ 10 k rows | LIMIT reads | full-stream 15.6 M rows | one-year 3 M rows |
|---|---|---|---|---|
| 8cef704 (50-query set) | ~850 ms | 100–200 ms | 5.7 s | 1.6 s |
| 64e7dc0 (50-query set) | ~1,000 ms | 110–230 ms | 3.9 s | 1.5 s |
| d0cc2c4 | 584 ms | 103 ms | 3.5 s | 1.24 s |
| 2215185 | 49 ms | 2,625 ms | 7.5 s | 1.27 s |
| **0a8eb80** | **40 ms** | **111 ms** | **3.95 s** | 1.38 s |
| d0cc2c4 + `partitioned_execution` off (DuckDB-level, section 9) | 27 ms | 73–165 ms | 5.6 s* | — |

\* measured in DuckDB directly without the Arrow/HTTP path, so not comparable to the other rows' full-stream column; the relative gain (6.2 → 5.6 s) is what matters.

---

## 11. 100 new query shapes: `d0cc2c4` vs `0a8eb80`

A second, disjoint query set (`backend_bench/issue140/queries100b.py`, seed 1400, ids 301–400, host file `~/var_runs/queries_100b.json`) with shapes none of the 300 earlier queries used. Same store (3.554 B rows, 31,903 row groups), same runner (1 cold + 2 warm-up + 10 timed, p50), run back to back on the same worktree: `phaseH_*` at d0cc2c4 (17:33–17:59 UTC, 1,557 s) and `phaseI_*` at 0a8eb80 (18:00–18:30 UTC, 1,829 s). No errors; row counts identical on both heads for all 100; the 10 deliberate-empty queries returned 0 rows on both.

| Group | n | Shape | median rows | median p50 d0cc2c4 | **median p50 0a8eb80** | geomean I/H | min – max |
|---|---|---|---|---|---|---|---|
| S | 15 | `start` only, open end: last 1 h … 365 d | 60 k | 806 ms | **61 ms** | 0.14 | 0.05 – 1.10 |
| E | 15 | `end` only, open start: first 1 h … 365 d | 2.7 k | 759 ms | **42 ms** | 0.08 | 0.05 – 0.57 |
| X | 10 | explicit `start`=earliest, `end`=latest (whole stream, windowed path) | 15.6 M | **3,627 ms** | 7,590 ms | 2.10 | 1.88 – 2.29 |
| B | 10 | 2- and 3-year windows, descending | 8.2 M | **2,317 ms** | 3,905 ms | 1.65 | 1.44 – 1.89 |
| P | 10 | windows straddling a stream's first / last timestamp | 4.9 k | 753 ms | **46 ms** | 0.06 | 0.04 – 0.07 |
| Z | 10 | deliberate empties (after latest, before earliest, unregistered uri, LIMIT 1 past the end) | 0 | 33 ms | **23 ms** | 0.43 | 0.05 – 1.15 |
| Q | 10 | 5 sparsest streams, full and 30-day | 4.3 M | 1,658 ms | **1,227 ms** | 0.41 | 0.15 – 0.99 |
| K | 10 | reads by storage key (`urn:acquirium#…`), 7-day and latest-10 | 30 k | 488 ms | **108 ms** | 0.25 | 0.07 – 0.91 |
| N | 10 | odd `timeseries_info` batches (unregistered, mixed, repeated, storage keys) | 2 | 87 ms | 91 ms | 1.02 | 0.86 – 1.10 |
| all | 100 | | | 786 ms | **82 ms** | 0.32 | 0.04 – 2.29 |
| sum of p50 | 100 | | | **118.0 s** | 137.6 s | | |
| sum of p50, X and B only | 20 | | | **58.6 s** | 113.9 s | | |
| sum of p50, the other 80 | 80 | | | 59.3 s | **23.7 s** | | |

The split is entirely by result size. Taking the 70 windowed reads without LIMIT (every group except Z, N and the LIMIT halves of K and Q) and bucketing by rows returned:

| rows returned | n | median p50 d0cc2c4 | median p50 0a8eb80 | geomean 0a8eb80 / d0cc2c4 | min – max |
|---|---|---|---|---|---|
| ≤ 10 k | 26 | 749 ms | **42 ms** | 0.06 | 0.04 – 0.07 |
| 10 k – 100 k | 11 | 806 ms | **64 ms** | 0.08 | 0.07 – 0.10 |
| 100 k – 1 M | 10 | 897 ms | **167 ms** | 0.23 | 0.15 – 0.57 |
| 1 – 3 M | 3 | 1,501 ms | 1,593 ms | 1.06 | 1.02 – 1.10 |
| 3 – 8 M | 5 | **1,807 ms** | 2,733 ms | 1.47 | 1.44 – 1.53 |
| > 8 M | 15 | **3,572 ms** | 7,332 ms | 2.01 | 1.81 – 2.29 |

What the new shapes add to sections 9–10:

- **Open-ended windows behave like closed ones.** `start`-only and `end`-only reads pay the full ~750 ms `partitioned_execution` floor at d0cc2c4 regardless of size (E8 returns 12 rows in 732 ms) and drop to 35–75 ms at 0a8eb80. Windows that straddle a stream's first or last timestamp, including the four that return 0 rows (P1/P3/P7/P9), are the same: 676–900 ms → 37–57 ms.
- **The CTE path loses above ~2 M rows and the loss grows with size.** The whole-range X reads (10–15.6 M rows) go from 3.6 s to 7.3–8.3 s, 3-year B windows (10.8 M) from 2.9 s to 5.2–5.4 s, 2-year (5.5 M) from 1.8 s to 2.5–2.8 s; the 365-day S windows (2.9 M) are the break-even at 1.02–1.10. Throughput on the > 8 M reads halves, 4.2 → 2.0 M rows/s, while server CPU per request drops from 35 to 14 CPU-s: the materialised CTE sort runs with less parallelism than the partitioned sort it replaces, so it costs less CPU and twice the wall time.
- **Explicit whole-range windows are the worst case for 0a8eb80 and a plausible client pattern** (a client that always passes `start`/`end` from `timeseries_info`). The same stream read with no window (Q1 full, 8.4 M rows) takes the plain path at both heads and is unchanged (2.38 → 2.28 s).
- **Everything else is neutral**, as designed: LIMIT reads by storage key 170–191 → 152–164 ms, `LIMIT 1` past the end 59–70 → 55–64 ms, full-stream reads 0.95–0.99, `timeseries_info` batches 0.86–1.10. Two empties are interesting on their own: a window entirely after a stream's latest timestamp returns in 6 ms at both heads (short-circuited before SQL), but a window entirely before its earliest goes through the sort and costs 690–770 ms at d0cc2c4 vs 39–45 ms at 0a8eb80.
- **Storage-key reads** (`urn:acquirium#uuid` instead of the point uri) resolve through the same path and show the same 10× gain on 7-day windows (786–919 → 60–65 ms); they are not a separate code path.

Net: over these 100 queries 0a8eb80 is 10–20× faster on the 70 reads that return under 1 M rows and 1.5–2.3× slower on the 20 that return 5–15 M rows, which is enough to make its total 17% higher (137.6 vs 118.0 s) despite winning 80 of 100. Whether that trade is right depends on how often clients read multi-million-row windows; the `SET disabled_optimizers='partitioned_execution'` route (section 9) avoids the choice because it removes the fixed cost without materialising anything.

Direct measurement of the three plan shapes on the large windows (`backend_bench/issue140/diag_big.py`, DuckDB 2.0 on the same file with the server stopped, stream 167 = 15.68 M rows, best of 2; absolute times include `fetchall()` into Python tuples and are therefore higher than the server's Arrow path, the ratios are what matters):

| Read on stream 167 | plain (d0cc2c4) | `MATERIALIZED` CTE (0a8eb80) | plain + `SET disabled_optimizers='partitioned_execution'` |
|---|---|---|---|
| explicit whole range, 15.68 M rows | 7,712 ms | 10,376 ms (1.35×) | **6,802 ms (0.88×)** |
| 3-year window `DESC`, 10.97 M rows | 4,730 ms | 7,169 ms (1.52×) | **4,547 ms (0.96×)** |
| 1-year window, 2.88 M rows | 1,926 ms | 2,076 ms (1.08×) | **1,380 ms (0.72×)** |
| 30-day window, 247 k rows | 796 ms | 196 ms (0.25×) | **139 ms (0.17×)** |

Disabling the optimizer on the read connection is the best of the three at every size: it removes the fixed ~0.7 s cost on small and medium windows like the CTE does, and on multi-million-row windows it is faster than the plain query rather than slower. The CTE's loss grows with result size exactly as the server measurements above show (1.08× at 2.9 M, 1.35–1.52× at 11–16 M in DuckDB; 1.06× / 1.8–2.3× through the server, where the extra copy also delays the Arrow conversion).

Updated cross-head summary (medians; the new 100-query columns from this section):

| | windowed ≤ 10 k rows | LIMIT reads | full-stream, no window, 15.6 M | one-year window 3 M | whole-range window 15.6 M | 3-year window 10.9 M |
|---|---|---|---|---|---|---|
| d0cc2c4 | 584 ms | 103 ms | 3.5 s | 1.24 s | **3.6 s** | **2.9 s** |
| 2215185 | 49 ms | 2,625 ms | 7.5 s | 1.27 s | — | — |
| 0a8eb80 | **40 ms** | **111 ms** | 3.95 s | 1.38 s | 7.6 s | 5.3 s |
| plain + `partitioned_execution` off (DuckDB-level) | 27 ms | 73–165 ms | 5.6 s* | 1.38 s* | 6.8 s* | 4.5 s* |

\* DuckDB-direct with `fetchall()`, compare only within the row (0.72–0.96× of the plain query at d0cc2c4 in the same harness).

Files: `~/var_runs/fix3/phaseH_*` (d0cc2c4), `phaseI_*` (0a8eb80), `diag_big.out`; local copies under the scratchpad `var_results/fix3b/`.
