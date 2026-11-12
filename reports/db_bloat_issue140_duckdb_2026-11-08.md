# Acquirium issue 140: why the DuckDB store is 8x the raw data

**Date:** 2026-11-07 / 2026-11-08
**Issue:** https://github.com/DataDrivenCPS/acquirium/issues/140 — "Database gets very bloated compared to raw data" (24.6 GB raw → 261.9 GB DuckDB file, reported by fletchapin on 2026-11-04)
**Companion run:** the same full-set measurement on Timescale started 2026-11-08 22:03 UTC in `~/run4`; it is reported separately when it completes.

**Result in one line:** a clean single-pass ingest of the full DPR trailer set produces a **188.9 GB** DuckDB file from **23.6 GB** of CSV. **165 GB (87%) is the ART index behind the `UNIQUE (ref_id, ts)` constraint.** Column data is 23.4 GB (12%). The `last_revision` index is 0.7 GB (0.4%). Dead rows, WAL and free blocks are zero.

---

## 1. Environment

| Item | Value |
|---|---|
| Host | `exouser@149.165.173.175` (Jetstream VM, hostname `client-loadgenerator-1`) |
| OS / kernel | Ubuntu 24.04, Linux 7.0.0-42-generic |
| CPU / RAM / swap | 32 vCPU / 122 GB / none |
| Disk | 484 GB `/dev/sda1`, 443 GB free before the run |
| Python / uv | 3.12.3 / uv 0.13.7 |
| Acquirium | public repo `DataDrivenCPS/acquirium`, branch `main`, commit `1b12d64` (merge of PR 139 `ums-ref-filter-lazy-stats`), installed editable into `~/venv_main`, reports version `0.4.0a8` |
| DuckDB | 2.0.0 (Python package pulled by acquirium) |
| Ray | 2.60.0 |
| Driver | `backend_bench/dpr_driver.py:DPRDriver` from this suite (subclass of the built-in `CSVIngestDriver`; adds the union DPR model `union-dpr-model.ttl` and the `property_to_column.json` point bindings) |
| Driver config | `interval = 30.0`, `insert_batch_rows = 50000`, `format = "wide"`, `date_col = "Date"`, `clock_col = "Time"`, `date_format = "%m/%d/%Y %I:%M:%S %p"`, `header_contains = ["Date","Time"]`, `null_values = ["NaN"]` |
| Server config | `timeseries_backend = "duckdb"`, `duckdb_path = ".acquirium/timeseries.duckdb"`, `recreate = true`, `host = 127.0.0.1`, `port = 8080` |

### Data

| Item | Value |
|---|---|
| Source | `~/Resilio Sync/DPR Pilot Logs New/*.csv` on the laptop |
| Files | 5,193 CSVs (all top-level `*.csv`; `.txt` siblings excluded) |
| Raw bytes | 23,596,563,556 B = **23.6 GB** |
| Copied to | `~/data/dpr_all` on the host, every file verified size-for-size against the source |
| Transfer | uncompressed `tar | ssh` stream, 41 min wall, 9.6 MB/s ≈ 77 Mbit/s (a 100-file / 462 MB burst earlier measured 12.0 MB/s ≈ 96 Mbit/s) |
| Gotcha | macOS `tar` adds `._*` AppleDouble sidecars which the CSV driver rejects ("no header row containing ['Date','Time']"); the full copy used `COPYFILE_DISABLE=1 tar --no-xattrs --no-mac-metadata` |

The 100-file subset used for the preliminary runs (section 6) is the first 100 lines of `backend_bench/dpr_800.list`, i.e. the same random sample the earlier backend benchmark used.

### Procedure

1. Host wiped: previous run dirs deleted, Timescale container removed, Ray stopped. `recreate = true` in `server.toml`.
2. `acquirium server --config server.toml` started detached in `~/run3`; healthy (HTTP 200 on `/health`) in under 60 s.
3. A per-minute logger (`progress.sh`) recorded: epoch, files inserted (count of `… .csv — inserted N observation(s)` lines in `server.log`), cumulative logged rows, DuckDB file bytes, WAL bytes, server RSS, Ray RSS, MemAvailable, traceback count.
4. `acquirium driver start driver.toml` at **2026-11-07 20:42:10 UTC**. Ray started with it (gcs_server, raylet, dashboard agent, runtime-env agent, one `DriverRunner` actor).
5. Last file logged at **2026-11-08 01:15:22 UTC**. Driver stopped via `POST /drivers/stop`, server sent SIGTERM, exited in 5 s, WAL fully checkpointed (no `.wal` file left).
6. The closed file was analysed read-only with DuckDB (`pragma_database_size`, `pragma_storage_info`, `duckdb_tables`, `duckdb_indexes`), then two rebuild experiments were run (section 4).

---

## 2. Ingest

| Metric | Value |
|---|---|
| Files | 5,193 / 5,193 |
| Rows logged as inserted by the driver | 3,556,122,871 |
| Rows in `timeseries` after the run | 3,554,166,660 |
| Difference (cross-file duplicate (ref_id, ts) pairs, deleted-then-reinserted) | 1,956,211 = **0.055 %** |
| Streams (`ref_ids` = `streams`) | 290 |
| Wall time (first → last inserted line) | **4 h 32 min 56 s** |
| Mean rate | 217 k rows/s, 19.0 files/min |
| Errors / tracebacks in `server.log` | 0 |
| Server peak RSS | 26.7 GB (DuckDB buffer pool; default memory limit is 80 % of RAM) |
| Ray processes RSS | 3.3–3.6 GB throughout |
| Min MemAvailable | 84.1 GB |

### Rate decay over the run (from `progress.log`)

| Hour | files/min | M rows/min | GB added to file |
|---|---|---|---|
| 1 | 22.2 | 15.9 | 52.1 |
| 2 | 20.2 | 14.4 | 47.6 |
| 3 | 18.7 | 12.7 | 40.0 |
| 4 | 16.9 | 10.3 | 31.6 |

Rows/min fall 35 % from hour 1 to hour 4 while file growth per row stays at ~53 B/row. The decay is the cost of maintaining a growing index on every batch.

### File growth checkpoints

| t (min) | files | rows (M) | file (GB) | server RSS (GB) |
|---|---|---|---|---|
| 30 | 703 | 491 | 25.6 | 6.0 |
| 60 | 1,332 | 953 | 52.1 | 8.8 |
| 120 | 2,542 | 1,817 | 99.7 | 14.0 |
| 180 | 3,665 | 2,580 | 139.7 | 19.6 |
| 240 | 4,682 | 3,200 | 171.3 | 24.1 |
| 274 | 5,193 | 3,556 | 188.9 | 25.7 |

The file grows linearly at ≈ 53 bytes per logged row from the first minute. There is no late-run blow-up; the ratio is the same at 700 files as at 5,193.

---

## 3. Storage breakdown of the final file

### Totals

| Item | Value |
|---|---|
| File size | 188,870,832,128 B = **188.9 GB** |
| `pragma_database_size`: block size | 262,144 B (256 KiB) |
| total / used / free blocks | 720,485 / 720,318 / 167 |
| WAL | 0 B |
| Ratio to raw CSV | **8.0×** |
| Bytes per stored row | 53.1 |

### Attribution by block ownership

`pragma_storage_info('<table>')` lists the block id of every column segment, so counting distinct block ids per table gives the blocks that hold column data. Everything else in the file is index (ART) storage plus a few metadata blocks.

| Component | Blocks | Size | Share | B/row |
|---|---|---|---|---|
| `timeseries` column data (7 columns) | 89,401 | **23.4 GB** | 12.4 % | 6.6 |
| All other tables (`ref_ids`, `streams`, migrations, experiment/materialization tables) | 4 | 1.0 MB | 0.0 % | – |
| Remainder = indexes + metadata | 630,913 | **165.4 GB** | 87.6 % | 46.5 |

### Which index

Two indexes exist on `timeseries` (from `src/acquirium/Storage/duckdb_store.py`):

| Index | Origin | Size (from rebuild experiments, section 4) |
|---|---|---|
| ART behind `UNIQUE (ref_id, ts)` table constraint | table DDL, commit `42671758` (2026-09-04) | **≈ 165 GB, 46.4 B/row** |
| `idx_timeseries_last_revision ON timeseries(last_revision)` | commit `306c085` (2026-08-26, revision-frontier materialization); read by `Materialization/revision_store.py` (`WHERE last_revision > ?`) | **0.68 GB, 0.2 B/row** |

### Column data detail (`pragma_storage_info('timeseries')`)

| Column | Type | Compression seen | Note |
|---|---|---|---|
| `ref_id` | INTEGER | Constant, RLE | long runs per batch |
| `ts` | TIMESTAMP | BitPacking, Constant | largest column |
| `numeric_value` | DOUBLE | ALP, RLE, Roaring (validity), Constant | NULL on text rows |
| `text_value` | VARCHAR | DICT_FSST, Uncompressed, Roaring, Constant, Empty Validity | avg length 3.9 chars |
| `deleted` | BOOLEAN | Constant | all FALSE — costs nothing |
| `last_stream_version` | BIGINT | Constant | all 0 — costs nothing |
| `last_revision` | BIGINT | RLE, Constant | one value per batch, 5,193 distinct |

Row groups: 31,449; mean 113,014 rows per row group (DuckDB max is 122,880, so the table is not fragmented). The per-column distinct-block sum (148,294) exceeds the table's distinct-block count (89,401) because a 256 KiB block holds segments from several columns; the table-level figure is the correct one.

Value kinds (measured on the 100-file subset, section 6; the full table was not scanned for this): 62 % numeric rows, 38 % text rows, no rows with both NULL, no deleted rows.

### Other tables

| Table | Rows | Note |
|---|---|---|
| `ref_ids` | 290 | ref_uri → INTEGER ref_id |
| `streams` | 290 | registry; 2 indexes |
| `schema_migrations` | 4 | |
| `experiment_*`, `materialization_*`, `logs`, `stream_resets`, `binding_progress` | 0–1 | |

### Raw-data comparison

Raw CSV is **6.6 B per observation** only because the wide format writes one timestamp per row for ~220 columns. DuckDB's compressed column data lands at the same **6.6 B/row** (23.4 GB) — the data itself is not bloated at all.

---

## 4. Rebuild experiments (what each index costs)

### 4a. Full table, fresh file

`CREATE TABLE timeseries AS SELECT * FROM src.timeseries` (insertion order preserved, no indexes), then add indexes one at a time, `CHECKPOINT` and measure the file.

| Step | Time | File | Δ |
|---|---|---|---|
| CTAS, no index | 59 s | **18.94 GB** | |
| `+ CREATE INDEX ON timeseries(last_revision)` | 86 s | 19.62 GB | **+0.68 GB** |
| `+ CREATE UNIQUE INDEX ON timeseries(ref_id, ts)` | — | **failed: `Out of Memory Error: could not allocate block of size 256.0 KiB (74.5 GiB/74.5 GiB used)`** with `memory_limit='80GB'`, 24 threads | |

Bulk-building the unique ART index on 3.55 B keys needs more than 75 GB of RAM. Note the rebuilt data-only copy (18.9 GB) is smaller than the ingested table's column data (23.4 GB): the ingest wrote row groups batch by batch, the CTAS repacks them.

### 4b. One-tenth subset (streams with `ref_id % 10 = 0`)

Taken from the 4a copy, so it fits in memory and gives a direct bytes-per-key figure.

| Step | Time | File | Δ | B/row |
|---|---|---|---|---|
| CTAS subset: 334,033,383 rows, 34 streams | 16 s | 1.573 GB | | 4.7 |
| `+ UNIQUE (ref_id, ts)` | 56 s | 17.069 GB | **+15.50 GB** | **46.4** |
| `+ INDEX (last_revision)` | 3 s | 17.141 GB | +0.07 GB | 0.2 |

46.4 B/row × 3,554,166,660 rows = **165 GB**, which equals the 165.4 GB remainder measured on the real file. A bulk-built ART index is no smaller than the one grown incrementally during ingest, so 46 B/key is the structure's inherent cost for this key (4-byte `ref_id` + 8-byte `ts`), not fragmentation or delete debris.

---

## 5. What the bloat is and is not

**It is** the `UNIQUE (ref_id, ts)` constraint. DuckDB implements it as an ART index, persists the ART uncompressed in 256 KiB blocks, and never compacts it. At 46 B/row it is 7× larger than the compressed column data it protects. The constraint exists to back the upsert in `DuckDBStore._insert_frame`, which deletes colliding `(ref_id, ts)` rows and re-inserts the incoming frame (`DELETE … USING incoming WHERE ref_id = … AND ts = …` then `INSERT … SELECT`).

**It is not:**

- dead rows / upsert churn — stored rows are 0.055 % below logged inserts;
- WAL or free space — both 0 after shutdown;
- the `last_revision` index — 0.68 GB, 0.4 %;
- column encoding — 6.6 B/row, equal to the raw CSV;
- row-group fragmentation — 113 k rows per row group, near the maximum.

**Why the reported case is worse (262 GB vs our 189 GB):** our run was a single clean pass. A long-lived store that has reprocessed files, re-run drivers, or received overlapping uploads performs many more DELETE+INSERT cycles; DuckDB does not reclaim ART nodes freed by deletes, so the index only grows.

---

## 6. Preliminary 100-file runs (same host, same code, same driver)

Run before the full set to validate the setup. Data: the first 100 files of `backend_bench/dpr_800.list` (461.9 MB raw, 68,651,765 logged observations, 68,650,997 stored rows, 290 streams; 42.8 M numeric rows, 25.8 M text rows, avg text length 3.9 chars, 159,098 distinct text values).

| | DuckDB | Timescale (pg17, `timescale/timescaledb:latest-pg17`, digest `b56a3f51…`) |
|---|---|---|
| Server healthy from cold start | 36 s | 39 s |
| Ingest wall | **4 min 05 s** (≈ 280 k rows/s) | **30 min 53 s** (≈ 37 k rows/s) |
| Store size as left by acquirium | **3.2 GB** (6.9×) | **6.8 GB** (14.8×) |
| Of which indexes | not split in this run | 2.51 GB (36.9 %): `idx_timeseries_ref_ts_unique` 2.09 GB (31.9 B/row), `idx_timeseries_last_revision` 423 MB (6.5 B/row) |
| Heap / column data | — | 4.31 GB (65.7 B/row), 768 dead tuples, TOAST 632 kB |
| Chunks | — | 78 × 7 days, **0 compressed** |
| After `compress_chunk` on all chunks (57 s) | — | **273 MB** (26× smaller; both indexes shrink to 632 kB) |
| Server + Ray RSS after ingest | 6.3 GB | — |

Timescale finding: `add_compression_policy(…, INTERVAL '7 days')` creates a job on a **12-hour schedule whose first run is 12 h after store creation** (here `next_start = 2026-11-08 07:38 UTC` for a store created 19:38 UTC). A fresh store is therefore measured fully uncompressed even though all its data qualifies. Compressed-chunk reads cost more for full scans (P1 full range 280 k rows: 116 ms → 500–700 ms) but LIMIT / window reads are unchanged (13–37 ms).

Query sanity (DuckDB, 100 files): SPARQL triple count 56 ms; `POST /timeseries_info` 29 ms; `GET /timeseries` P1 full range 280,142 rows 116 ms, desc LIMIT 1000 14 ms, 4-month window 19,100 rows 19 ms; values and time bounds (2024-08-20 → 2026-08-21) matched the CSVs.

---

## 7. Reproduction

On the host, everything lives under `~/bench`:

| File | Purpose |
|---|---|
| `dpr_driver.py`, `union-dpr-model.ttl`, `property_to_column.json` | the DPR driver (copied from this suite's `backend_bench/`) |
| `duck_storage.py <db>` | read-only breakdown: `pragma_database_size`, tables, indexes, per-column `storage_info`, value kinds |
| `duck_blocks.py <db>` | exact data-vs-index split by distinct block ids |
| `duck_copy_test.py <src> <out>` | CTAS rebuild + index-at-a-time sizing (4a) |
| `size_breakdown.sql` | the Timescale equivalent (`hypertable_detailed_size`, per-index `hypertable_index_size`, chunks, compression stats, dead tuples) |

Copies of the three Python scripts and the SQL are in the session scratchpad; the DuckDB and subset experiment in 4b was run inline (see the SQL in section 4).

Run order: `acquirium server --config server.toml` → wait for `/health` → `acquirium driver start driver.toml` → wait for 5,193 `… .csv — inserted` lines → `POST /drivers/stop` → SIGTERM server → analyse the closed file.

Operational traps hit on this host: `pkill -f "<pattern>"` inside an ssh command matches the ssh session's own shell and kills it (exit 255); `nohup … &` inside a single ssh command keeps the channel open until the client times out — start long-lived processes in `tmux new-session -d`; `curl` to a server that has bound but not yet accepted needs `--max-time`.
