# Acquirium issue 140: the Timescale store on the same data (partial run, 1,302 files)

**Date:** 2026-11-08 / 2026-11-09
**Issue:** https://github.com/DataDrivenCPS/acquirium/issues/140 — "Database gets very bloated compared to raw data"
**Companion report:** `db_bloat_issue140_duckdb_2026-11-08.md` (full 5,193-file set on DuckDB, 188.9 GB)

**Result in one line:** after 6 h 54 min the Timescale store held **1,302 of 5,193 files** (930.8 M rows from 6.04 GB of CSV) in **96.4 GB, fully uncompressed: 16.0× the raw bytes**, of which 60.9 GB (63 %) is heap at 65.4 B/row, 29.5 GB (31 %) is the `UNIQUE (ref_id, ts)` btree at 31.7 B/row and 6.0 GB (6 %) is the `last_revision` btree at 6.4 B/row. Compressing every chunk by hand took **15 min 52 s** and left **2.46 GB (2.6 B/row, 0.41× raw, 39× smaller)**. Nothing had been compressed by acquirium's own policy because its first run is scheduled 12 h after store creation. Ingest ran at a flat 37 k rows/s (5.8× slower than DuckDB), which is why the run was stopped early at the user's request; the per-row figures were constant from the first hour, so the stopped run measures the same thing a full run would.

---

## 1. Environment

Identical host, code, driver and data to the DuckDB report; only the server backend differs.

| Item | Value |
|---|---|
| Host | `exouser@149.165.173.175` (Jetstream VM), Ubuntu 24.04, 32 vCPU / 122 GB RAM / no swap, 484 GB disk (443 GB free before the run) |
| Acquirium | public repo `DataDrivenCPS/acquirium`, `main` @ `1b12d64`, editable install in `~/venv_main`, version `0.4.0a8`; Ray 2.60.0 |
| Timescale | container `acq_bench_tsdb` from `timescale/timescaledb:latest-pg17` (digest `b56a3f51…`): PostgreSQL 17.11, TimescaleDB 2.31.0, fresh volume, image defaults (`shared_buffers` 2 GB, `max_wal_size` 1 GB, `checkpoint_timeout` 300 s, `wal_level` replica, autovacuum on) |
| Server config | `timeseries_backend = "timescale"`, `pg_dsn = "postgresql://acquirium:acquirium@127.0.0.1:5433/acquirium"`, `recreate = true`, `data_dir = ".acquirium"`, `127.0.0.1:8080` |
| Driver | `backend_bench/dpr_driver.py:DPRDriver`, `interval = 30.0`, `insert_batch_rows = 50000`, `format = "wide"`, `date_col = "Date"`, `clock_col = "Time"`, `date_format = "%m/%d/%Y %I:%M:%S %p"`, `header_contains = ["Date","Time"]`, `null_values = ["NaN"]`, `watch_dir = ~/data/dpr_all`, `glob = "*.csv"` |
| Data | `~/data/dpr_all`: the full 5,193-file DPR trailer set (23.6 GB), same copy as the DuckDB run |

### Schema as created by `TimescaleStore` (`src/acquirium/Storage/timescale_store.py`)

| Object | Definition |
|---|---|
| `timeseries` | `ref_id integer, ts timestamptz, numeric_value double precision, text_value text, deleted boolean, last_revision bigint`; hypertable on `ts`, **7-day chunks**, `create_default_indexes => FALSE` |
| `idx_timeseries_ref_ts_unique` | `CREATE UNIQUE INDEX … ON timeseries USING btree (ref_id, ts)` (commit `42671758`, 2026-09-04) — backs the `ON CONFLICT` upsert |
| `idx_timeseries_last_revision` | `CREATE INDEX … USING btree (last_revision)` (commit `306c085`, 2026-08-26) — read by `Materialization/revision_store.py` (`WHERE t.last_revision > ?`) |
| compression | `segmentby = ref_id`, `orderby = ts DESC`; `add_compression_policy('timeseries', INTERVAL '7 days')` → job 1000, `schedule_interval = 12:00:00` |
| other | `ref_ids`, `streams`, `system_state`, `schema_migrations`, `experiment_*`, `logs` — 600 kB in total |

### Procedure

1. Host wiped after the DuckDB run (run dirs, 189 GB file, Ray temp, old container). New container, new volume. `recreate = true`.
2. `acquirium server --config server.toml` in tmux `acqsrv` (`~/run4`); healthy in 43 s; empty store 9.8 MB with both indexes present.
3. Per-minute logger `progress.sh` in tmux `acqlog` → `progress.log`: epoch, files inserted (count of `… .csv — inserted N observation(s)` lines in `server.log`), cumulative logged rows, `pg_database_size`, chunk count, compressed-chunk count, server / Ray / postgres RSS, MemAvailable, disk used, traceback count.
4. `acquirium driver start driver.toml` → driver `started_at` **2026-11-08 22:03:35 UTC**, first file logged 22:03:44.
5. **Stopped early** on the user's instruction at 20 % of the files (the rate made the full set a 26 h job and the per-row figures were already stable). `POST /drivers/stop {"name":"DPRDriver"}` at 04:58:07; the actor did not exit within 10 s and was killed; its in-flight Arrow batch (783,224 rows of file 1,303) still landed at 04:58:17. Row count was verified stable over the next 90 s, then the server and logger were stopped and `ray stop --force` run.
6. Uncompressed breakdown (`size_breakdown_fixed.sql` + `size_extra.sql`, 5 min), then `compress_all.sh` compressed the 56 chunks one at a time with per-chunk timing, then `size_compressed.sql`.

---

## 2. Ingest

| Metric | Value |
|---|---|
| Files fully ingested | **1,302 / 5,193** (exactly the first 1,302 in sorted filename order, 2023-03-06 → 2024-03-11) |
| Raw bytes of those files | 6,039,839,343 B = **6.04 GB** (25.6 % of the set) |
| Rows logged as inserted by the driver | 930,008,652 |
| Rows in `timeseries` after the run | **930,769,928** (= logged + 783,224 from the in-flight batch of file 1,303 − 21,948 upserted duplicates) |
| Duplicate (ref_id, ts) pairs across files (`n_tup_upd`) | 21,948 = **0.0024 %** |
| Streams (`ref_ids` = `streams`) | 246 |
| Time span of data | 2023-03-06 11:47:56 → 2024-03-26 16:02:15 (56 weekly chunks, 2023-03-02 → 2024-03-28) |
| Wall time (first → last logged file) | **6 h 54 min 12 s** (24,852 s) |
| Mean rate | **37.4 k rows/s**, 3.14 files/min |
| Projected full set at this rate | 3.556 B rows → **26.4 h** (DuckDB: 4 h 33 min) |
| Errors / tracebacks in `server.log` | 0 |
| Warnings | 4,570 × `stored N unparseable numeric value(s) as text` (expected: the DPR logs contain status columns) |
| Server RSS | 2.1–2.9 GB throughout (no buffer pool in-process) |
| Ray processes RSS | 3.1–3.7 GB |
| postgres RSS | 4.5–4.8 GB during ingest, 9.3 GB peak at the stop |
| Min MemAvailable | 104.3 GB |
| Disk used (whole VM) | 42 GB → 133 GB |

### Rate over the run (from `progress.log`)

| Hour | files/min | M rows/min | GB added to DB | B added per row |
|---|---|---|---|---|
| 1 | 3.0 | 2.2 | 13.7 | 104.5 |
| 2 | 2.8 | 2.2 | 13.5 | 103.5 |
| 3 | 2.8 | 2.2 | 13.3 | 103.6 |
| 4 | 3.8 | 2.3 | 14.1 | 103.4 |
| 5 | 3.3 | 2.3 | 14.4 | 103.6 |
| 6 | 3.2 | 2.3 | 14.4 | 104.1 |

Unlike DuckDB (rows/min fell 35 % between hour 1 and hour 4), Timescale's rate is **flat**: the btree is maintained per 7-day chunk, so the index being updated never grows beyond one chunk's worth. The cost is paid up front instead — 37 k rows/s versus DuckDB's 217 k rows/s average — in the `INSERT … ON CONFLICT (ref_id, ts) DO UPDATE` merge, which the earlier ingest trace (`backend_bench/ingest_trace`) showed to be ~83 % of Timescale ingest time, CPU-bound in one backend.

### Growth checkpoints

| t (min) | files | rows (M) | DB (GB) | server RSS (GB) | postgres RSS (GB) |
|---|---|---|---|---|---|
| 30 | 96 | 66 | 6.9 | 2.6 | 4.5 |
| 60 | 183 | 131 | 13.8 | 2.2 | 4.6 |
| 120 | 352 | 262 | 27.2 | 2.3 | 4.6 |
| 180 | 521 | 390 | 40.5 | 2.3 | 4.6 |
| 240 | 751 | 526 | 54.6 | 2.4 | 4.6 |
| 300 | 950 | 666 | 69.0 | 2.6 | 4.8 |
| 360 | 1,140 | 804 | 83.4 | 2.6 | 4.8 |
| 414 | 1,302 | 930 | 96.5 | 2.6 | 6.5 |

The store grows linearly at **≈ 104 bytes per logged row** from the first minute. Chunk count went 0 → 56; compressed chunks stayed at 0 for the whole run.

---

## 3. Storage breakdown, uncompressed (as acquirium left it)

### Totals

| Item | Bytes | Share | Per stored row |
|---|---|---|---|
| `pg_database_size` | 96,445,593,267 (**96.4 GB**) | 100 % | **103.6 B** |
| `timeseries` hypertable total (`hypertable_detailed_size`) | 96,432,226,304 | 99.99 % | 103.6 B |
| — heap (`table_bytes`) | 60,903,620,608 (**60.9 GB**) | 63.2 % | **65.4 B** |
| — `idx_timeseries_ref_ts_unique` | 29,526,581,248 (**29.5 GB**) | 30.6 % | **31.7 B** |
| — `idx_timeseries_last_revision` | 6,001,557,504 (**6.0 GB**) | 6.2 % | **6.4 B** |
| — TOAST | 466,944 | 0.0005 % | — |
| All other tables (`public` schema) | ~0.6 MB | — | — |
| WAL (`pg_ls_waldir`) | 1,024 MB (64 segments; pinned at `max_wal_size`, not counted in `pg_database_size`) | — | — |
| Raw CSV of the ingested files | 6,039,839,343 | — | 6.5 B |
| **Inflation vs raw** | **16.0×** | | |

The per-row figures match the 100-file run (65.7 / 31.9 / 6.5 B) to within 1 %, so they are a property of the schema, not of data volume.

### Where the heap bytes come from

A PostgreSQL heap row is a 23-byte tuple header + 1 padding byte, then `ref_id` 4 B, `ts` 8 B, `numeric_value` 8 B, `text_value` varlena (1 B header + text; NULL for numeric rows), `deleted` 1 B, `last_revision` 8 B, plus a 4-byte line pointer and 8-byte alignment of the tuple, plus the 24-byte page header and fill slack per 8 KB page. That is 58–62 B of fixed overhead per observation for ~17 B of payload; 65.4 B/row measured. Row-oriented storage of narrow rows is the whole story here; there are no dead rows worth mentioning (21,959 dead tuples, autovacuum had run on all 56 chunks).

### Value kinds

| Kind | Rows | Share |
|---|---|---|
| numeric (`numeric_value IS NOT NULL`) | 572,708,262 | 61.5 % |
| text (`text_value IS NOT NULL`) | 358,061,016 | 38.5 % |
| both NULL | 650 | — |
| `deleted = true` | 0 | — |
| distinct `last_revision` values | 1,303 (one per driver batch) | — |

Text values are status words — in the first chunk the top values are `OFF` (3.34 M), `OK`, `ON`, `Run`, `NO`, `Manual Control`, `YES`, `NO ERROR`, `OPEN`, `AC`, `Standby`, `BATTERY`; average length 3.4 chars. They are the "unparseable numeric value(s) stored as text" of the 4,570 warnings and cost ~5 B of varlena each in the heap.

### Chunks

| Item | Value |
|---|---|
| Chunks | 56 × 7 days, **0 compressed** |
| Typical chunk | 1.35–1.48 GB (≈ 0.89 GB heap + 0.52 GB indexes), 13.3 M rows |
| Largest | `_hyper_1_55_chunk` 12.5 GB and `_hyper_1_56_chunk` 11.2 GB: the week of 2024-03-11, where the subset ends with ~90 densely sampled files (`… 2024.03.11-1319-NN.csv`, 783 k observations each) |
| Smallest | 265 MB (partial first/last weeks) |
| Compression policy job 1000 | `compress_after = 7 days`, `schedule_interval = 12 h`, `total_runs = 1` (the no-op run at creation, 22:02:48, when there were no chunks), `next_start = 2026-11-09 10:02:48 UTC` — 5 h after the run was stopped |

Every one of the 56 chunks already qualified for compression (all data is older than 7 days); none was compressed because the policy had not fired yet.

---

## 4. After `compress_chunk` on every chunk

`compress_all.sh` compressed chunks oldest-first, one `SELECT compress_chunk(…)` at a time, 05:09:00 → 05:24:52 UTC.

| Item | Before | After | Ratio |
|---|---|---|---|
| `pg_database_size` | 96,445,593,267 | **2,464,929,459 (2.46 GB)** | **39.1×** |
| hypertable total | 96,432,201,728 | 2,460,254,208 | 39.2× |
| — table (uncompressed heap → compressed chunk heap) | 60,903,620,608 | 332,144,640 | 183× |
| — indexes | 35,528,122,368 | 38,928,384 | 913× |
| — TOAST (compressed column arrays live here) | 458,752 | 2,089,181,184 | — |
| `idx_timeseries_ref_ts_unique` | 29.5 GB | 456 kB | — |
| `idx_timeseries_last_revision` | 6.0 GB | 456 kB | — |
| Per stored row | 103.6 B | **2.64 B** | |
| vs raw CSV (6.04 GB) | 16.0× | **0.41×** | |
| Rows readable (`count(*)`) | 930,769,928 | 930,769,928 | |
| Wall time | | **15 min 52 s** (952 s) — 11–17 s per 1.4 GB chunk, 132 s and 113 s for the two 11–12 GB chunks; ≈ 100 MB/s | |

Per-chunk ratios ranged 38× to 67× (the chunks with fewer text rows compress best). Both btrees fall to one empty page per chunk because compressed chunks are not indexed by the hypertable's indexes; the unique constraint is then enforced via the segmentby/orderby metadata on decompression (`compress_segmentby = ref_id, compress_orderby = ts DESC` keeps each stream's values contiguous and sorted).

Not measured here, but relevant for a long-lived store: once chunks are compressed, an upsert that collides with a compressed row (the 0.0024 % cross-file duplicates, or re-processed files) forces a partial decompression of the segment, and inserts into a compressed chunk accumulate in an uncompressed staging area until the policy recompresses it. The 100-file run also showed full-range reads 4–6× slower on compressed chunks (116 ms → 500–700 ms for 280 k rows) with LIMIT / window reads unchanged.

---

## 5. Projection to the full set and comparison with DuckDB

Rates and bytes per row were constant across the run, so the full-set numbers scale linearly (3.554 B stored rows, 23.6 GB raw):

| | DuckDB (measured, full set) | Timescale uncompressed (projected) | Timescale compressed (projected) |
|---|---|---|---|
| Store size | **188.9 GB** | **≈ 368 GB** | **≈ 9.4 GB** |
| Inflation vs raw | 8.0× | 15.6× | 0.40× |
| Bytes per row | 53.1 | 103.6 | 2.6 |
| — data / heap | 6.6 | 65.4 | 2.5 (incl. TOAST) |
| — `UNIQUE (ref_id, ts)` index | **46.4** | **31.7** | ~0 |
| — `last_revision` index | 0.2 | 6.4 | ~0 |
| Ingest wall | **4 h 33 min** (rate decays 35 %) | **≈ 26.4 h** (flat) | + ≈ 1 h of `compress_chunk` |
| Mean ingest rate | 217 k rows/s | 37 k rows/s | |
| Server process RSS | 26.7 GB peak | 2.9 GB (+ 9 GB postgres) | |

Measured, not projected: 100-file subset DuckDB 3.2 GB vs Timescale 6.8 GB → 273 MB compressed; 1,302-file subset Timescale 96.4 GB → 2.46 GB compressed.

**What this says about issue 140.** Both backends pay heavily for the unique `(ref_id, ts)` constraint that backs the upsert: 46 B/row as a DuckDB ART index (87 % of the file, never compacted), 32 B/row as a PostgreSQL btree (31 % of the store). Timescale's heap is 10× DuckDB's column data per row, so uncompressed Timescale is twice as bloated as DuckDB — but Timescale has a compression path that removes all of it (indexes included) and ends up smaller than the CSVs, whereas DuckDB has nothing comparable for the index. The `last_revision` index is minor in both (0.4 % / 6 %).

**Operational finding (Timescale).** The store is compressed only by the policy job, and the policy's first run is **12 h after store creation**, every 12 h thereafter. A freshly created store that ingests a backlog therefore sits at 16× raw until that first run; on this host that would have been 368 GB of disk for the full set before the job fired. Either call `compress_chunk` (or `run_job(1000)`) at the end of a bulk load, or create the policy with `initial_start => now()` / a shorter `schedule_interval`.

---

## 6. Reproduction

On the host, under `~/bench` (copies of the scripts are in the session scratchpad):

| File | Purpose |
|---|---|
| `dpr_driver.py`, `union-dpr-model.ttl`, `property_to_column.json` | the DPR driver (from this suite's `backend_bench/`) |
| `size_breakdown_fixed.sql` | database size, row counts, `hypertable_detailed_size`, per-index `hypertable_index_size`, chunks, compression settings and jobs, per-chunk heap/index, value kinds, dead tuples, other tables (the original `size_breakdown.sql` had a `round(double, int)` that PostgreSQL rejects; fixed with a `::numeric` cast) |
| `size_extra.sql` | distinct refs, raw byte columns, chunk size distribution, WAL, settings, autovacuum coverage, `job_stats`, time range |
| `compress_all.sh` | `compress_chunk` on every uncompressed chunk oldest-first, logging before/after bytes and seconds per chunk |
| `size_compressed.sql` | `hypertable_compression_stats`, per-index sizes, per-chunk ratios, row count, size by schema |

Run artefacts in `~/run4`: `server.toml`, `driver.toml`, `server.log`, `server_clean.log` (ANSI stripped), `progress.log` (415 lines), `ingested_files.txt`, `size_uncompressed.txt`, `compress.log`, `size_compressed.txt`. The container `acq_bench_tsdb` is still up with the compressed 2.46 GB store.

Host traps hit in this run: `pkill -f` on a pattern that appears in the ssh command line kills the ssh session; `nohup … &` inside a single ssh command hangs the call (use `tmux new-session -d`); single quotes inside a heredoc inside an ssh `'…'` command are stripped (scp the script instead); `/drivers/stop` takes `{"name": …}`, not `{"id": …}` (`/drivers/list` returns `name`); CSV names contain spaces, so extract them with `sed 's/.*DPRDriver: \(.*\.csv\) — inserted.*/\1/'`, not a `\S+` match.
