## Benchmark: DuckDB vs TimescaleDB backend on this branch (1000 DPR files)

**Setup.** Both backends on this branch @ `d6b6e1a`, same VM (4 vCPU / 16 GiB cgroup), fresh clone + venv + `timescale/timescaledb:latest-pg17` container per run. Dataset: 1000 DPR trailer SCADA CSVs (2024-08 → 2026-08), **682.6M rows pushed, 682,522,976 stored on both**, 292 streams. Ingest via the CSV driver (`insert_batch_rows = 50000`). Query set: 50 `GET /timeseries` / `POST /timeseries_info` shapes generated on the DuckDB run and replayed unchanged on Timescale (1 cold + 2 warm-up + 10 timed reps, p50 reported).

### Ingest and footprint

| | DuckDB | Timescale (this branch) |
|---|---|---|
| Ingest wall | **19.9 min** | 77.4 min |
| Rows/s | **572k** | 147k |
| CPU during ingest | 1.5 cores (acquirium) | 0.93 cores postgres + 0.17 acquirium |
| DB on disk | **36.5 GB** (54 B/row) | 57.8 GB (85 B/row): 35.9 GB table + 21.8 GB unique `(ref_id, ts)` index |
| Peak RSS | 10.3 GB (server) | 4.5 GB server + 9.0 GB postgres |
| Chunks compressed | – | 0 / 101 |

Both rates are flat vs the 200-file dataset (586k / 148k rows/s). Timescale is bound by the single-threaded staging → hypertable merge: 65% of sampled Postgres statements were the `INSERT … SELECT … ON CONFLICT`, 8% the `COPY`.

### Queries (p50, Timescale ÷ DuckDB)

| Regime | Queries | Result | Example |
|---|---|---|---|
| Scans ≥ 170k rows | 16 | **DuckDB 1.3–2.0× faster** | 2.76M-row full stream: 1.7 s vs 2.7 s |
| Windows ≤ 6k rows | 14 | **Timescale 2–5× faster** | 1-day: 5.5 vs 14 ms; 1-hour: 1.9 vs 8 ms |
| LIMIT / latest-N | 5 | **Timescale 2–11× faster** | `latest 1`: 5 vs 61 ms; `90 d limit 100k`: 91 vs 335 ms |
| Sparse stream (29k rows) | 2 | **Timescale 7.5× faster** | full range: 29 vs 219 ms |
| `timeseries_info` batch, 112 streams | 1 | **Timescale 30× slower** | 24.6 s vs 0.82 s (full hypertable scan for `COUNT/MIN/MAX … GROUP BY ref_id`) |
| `timeseries_info` single stream | 1 | ~equal | 247 vs 327 ms |

### Bug found: NUL byte in a CSV wedges Timescale ingest

One file (`DPR Pilot R1 SCADA v14.10 - 2026.01.31-2156-46.csv`) ends in 1.16 MB of `0x00` from a truncated write. DuckDB ingested it. On Timescale the COPY path sends text values verbatim, Postgres rejects it (`invalid byte sequence for encoding "UTF8": 0x00`), `/insert_timeseries_arrow` returns 400, and the driver retries the same file every tick forever — ingest froze at 652/1000 files. The numbers above are from a re-run with the NUL tail stripped from that one file (the only file of 1000 with NUL / invalid UTF-8). Suggest stripping `\x00` from text values before `COPY` (or rejecting the row) instead of failing the batch.

### Caveats

- Three queries (two "1 year back" windows, sparse stream last 30 days) return 0 rows on both backends because of gaps in the data.
- Query #33 returns 2,763,643 rows on Timescale vs 2,763,642 on DuckDB: the truncated last line of the sanitised file parses as one numeric row.
- Absolute latencies drift ±15–20% with host load between runs; ratios within this campaign (runs back to back) are the reliable numbers.

Full report with per-query table, timelines and provenance: `reports/backend_1000files_duckdb_vs_timescale.html` in the acqbench repo.
