## Benchmark: `ums-duckdb-fix` vs `main` vs `ums-duckdb-fix + idx_ref`

Full-stack run — real `acquirium server` + real `DPRTrailerCSVDriver` pushed over
HTTP, DuckDB backend, fresh store per configuration (`recreate = true`).

| | |
|---|---|
| Data | 200 DPR SCADA CSVs, 893 MB → 139.8M rows across 261 streams |
| Environment | `cloud-vm` container: 4 CPUs, 16 GiB cap, Ubuntu 24.04, Python 3.12 |
| Commits | `ums-duckdb-fix` @ `e16f05fa` · `main` @ `068e0bc6` |
| Third config | `ums-duckdb-fix` @ `e16f05fa` + one added line in `DuckDBStore.ensure_table`:<br>`f"CREATE INDEX IF NOT EXISTS idx_ref ON {TIMESERIES_TABLE} (ref_id)"` |
| Method | ingest timed from the driver's own per-file `inserted N row(s)` lines; queries over HTTP with `Accept: arrow.stream`, iterator fully drained, 20 reps after 3 discarded warmups |
| Verified | index audit after shutdown — fix: `[]`, +idx_ref: `['idx_ref']`; all three `COUNT(*)` identical |

### Ingest

| metric | `ums-duckdb-fix` | `main` | `fix + idx_ref` |
|---|---:|---:|---:|
| rows ingested | 139,824,486 | 139,824,486 | 139,824,486 |
| `COUNT(*)` cross-check | 139,777,767 | 139,777,767 | 139,777,767 |
| wall time | **7.7 min** | 51.1 min | 52.9 min |
| throughput | **302,337 rows/s** | 45,642 rows/s | 44,061 rows/s |
| DB size | **7.43 GB** | 23.95 GB | 10.65 GB |
| bytes/row | **53.2** | 171.3 | 76.2 |
| indexes on `timeseries` | none | 6 | `idx_ref` |

Adding `idx_ref` costs the fix branch **6.9× its ingest throughput** (302k → 44k
rows/s) and **+43% storage** (7.43 → 10.65 GB), landing it marginally slower than
`main`. Throughput decayed monotonically as the table grew — 181k rows/s at the
start, 75k at 81M rows, 60k at 106M, 48k at 128M — the signature of ART
maintenance on a low-cardinality key (261 distinct `ref_id`s over 139.8M rows:
few keys, very large subtrees, rewritten on every batch).

### Query latency (p50 ms, 20 reps)

Each stream holds ~535k rows, so "full range" reads ~0.4% of the table.

| query | rows returned | `ums-duckdb-fix` | `main` | `fix + idx_ref` |
|---|---:|---:|---:|---:|
| `timeseries_info` batch (112 uris) | — | 154.4 | 287.6 | **152.4** |
| `timeseries_info` single | — | **94.9** | 119.0 | 110.2 |
| full range | 535,547 | 398.9 | **183.3** | 428.7 |
| 30 days | 211,095 | 160.3 | **74.0** | 158.8 |
| 7 days | 59,675 | 50.2 | **25.3** | 50.1 |
| 1 day | 8,553 | 10.2 | **6.9** | 10.1 |
| 1 hour | 348 | 4.1 | **3.5** | 4.2 |
| 10 min | 50 | 3.3 | **3.3** | 3.5 |
| 1 min | 4 | 3.1 | **2.9** | 3.2 |
| limit 1000 | 1,000 | **7.3** | 35.2 | 7.4 |
| latest 100 desc | 100 | 4.4 | **3.6** | 4.3 |
| coalesce, 1 day | 8,553 | 10.8 | **7.7** | 10.6 |
| sparse stream, full range | 535,547 | 396.0 | **178.5** | 427.1 |
| numeric stream, 1 day | 8,553 | 10.4 | **6.8** | 10.4 |
| numeric stream, full range | 535,547 | 397.4 | **179.3** | 508.2 |
| empty range | 0 | 2.6 | **2.4** | 2.5 |

### Reading these numbers

- **`idx_ref` buys nothing on queries.** Every shape lands within noise of the
  unindexed fix branch, and the widest scans are *slower* (full range 428.7 vs
  398.9 ms; numeric full range 508.2 vs 397.4 ms). It does not recover any of
  `main`'s advantage on wide reads. So the 6.9× ingest cost and +43% storage buy
  no measurable query benefit — on this workload the added index is pure
  overhead and should not be merged.
- **Why:** DuckDB only uses an ART index for a sufficiently selective equality
  predicate. Here `WHERE ref_id = ?` matches ~535k of 139.8M rows (0.4%), and the
  planner correctly prefers a sequential scan with zone-map pruning. With just
  261 distinct `ref_id`s the index can never be selective enough to be chosen,
  while still paying full maintenance cost on every insert.
- **`main` vs `ums-duckdb-fix` is the real trade-off:** the fix branch ingests
  6.6× faster into a 3.2× smaller store, and gives back ~2× on unbounded and
  wide-window reads. Bounded queries barely move — at 1 hour or less the branches
  are within 0.6 ms of each other; the gap only opens past ~10k rows returned.
- **Two shapes favour the fix branch:** `timeseries_info` (154 vs 288 ms) and
  `limit 1000` (**7.3 vs 35.2 ms, 4.8×**). On `main` a small `LIMIT` over an
  unbounded range does not exploit its indexes — worth a look, since "sample this
  stream" is a common UI call.
- **Not fixed by any configuration:** all 112 `point_uri`s from
  `property_to_column.json` register with `value_kind="text"`, so
  `value_mode="numeric"` returns nothing for any semantically-addressed point
  (measured 0 of 20 on **all three** configs). Root cause is driver-side —
  `DPRTrailerCSVDriver._register_streams_from_lookup()` re-registers the lookup
  bindings without a `value_kind`, clobbering the kind inferred from the data.
  Unmapped streams infer correctly (114 numeric / 35 text). The numeric query
  shapes above therefore address an unmapped stream by computed ref URI.

<details>
<summary>Caveats</summary>

- Single run per configuration, single client thread; no concurrency in the query phase.
- Query windows are anchored on a real row (newest timestamp via
  `limit=1&order=desc`), not on the midpoint of the reported range — DPR exports
  contain occasional decades-off timestamps that otherwise put windows in a void.
- The `system_metrics` driver was omitted so it could not pollute row counts.
- `main`'s store reached 24 GB against a 16 GiB RAM cap without being OOM-killed.
- The `idx_ref` config is `ums-duckdb-fix` with a one-line source patch applied to
  a fresh clone at the same commit; the patch asserts a unique anchor and the
  resulting index set was verified from `duckdb_indexes()` after shutdown.

</details>
