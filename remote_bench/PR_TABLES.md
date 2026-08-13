## Benchmark: `ums-duckdb-fix` vs `main`

Full-stack run — real `acquirium server` + real `DPRTrailerCSVDriver` pushed over
HTTP, DuckDB backend, fresh store per branch (`recreate = true`).

| | |
|---|---|
| Data | 200 DPR SCADA CSVs, 893 MB → 139.8M rows across 261 streams |
| Environment | `cloud-vm` container: 4 CPUs, 16 GiB cap, Ubuntu 24.04, Python 3.12, DuckDB via `pip install -e .` |
| Commits | `ums-duckdb-fix` @ `e16f05fa` · `main` @ `068e0bc6` |
| Method | ingest timed from the driver's own per-file `inserted N row(s)` lines; queries over HTTP with `Accept: arrow.stream`, iterator fully drained, 20 reps after 3 discarded warmups |

### Ingest

| metric | `ums-duckdb-fix` | `main` | delta |
|---|---:|---:|---|
| rows ingested | 139,824,486 | 139,824,486 | identical |
| `COUNT(*)` cross-check | 139,777,767 | 139,777,767 | identical |
| wall time | **7.7 min** | 51.1 min | **6.6× faster** |
| throughput | **302,337 rows/s** | 45,642 rows/s | **6.6× faster** |
| DB size | **7.43 GB** | 23.95 GB | **3.2× smaller** |
| bytes/row | **53.2** | 171.3 | **3.2× smaller** |
| server boot | 240 s | 240 s | same |

### Query latency (p50 ms, 20 reps)

Each stream holds ~535k rows, so "full range" reads ~0.4% of the table.

| query | rows returned | `ums-duckdb-fix` | `main` | faster |
|---|---:|---:|---:|---|
| `timeseries_info` batch (112 uris) | — | **154.4** | 287.6 | fix, 1.9× |
| `timeseries_info` single | — | **94.9** | 119.0 | fix, 1.3× |
| full range | 535,547 | 398.9 | **183.3** | main, 2.2× |
| 30 days | 211,095 | 160.3 | **74.0** | main, 2.2× |
| 7 days | 59,675 | 50.2 | **25.3** | main, 2.0× |
| 1 day | 8,553 | 10.2 | **6.9** | main, 1.5× |
| 1 hour | 348 | 4.1 | **3.5** | main, 1.2× |
| 10 min | 50 | 3.3 | **3.3** | tie |
| 1 min | 4 | 3.1 | **2.9** | main, 1.1× |
| limit 1000 | 1,000 | **7.3** | 35.2 | **fix, 4.8×** |
| latest 100 desc | 100 | 4.4 | **3.6** | main, 1.2× |
| coalesce, 1 day | 8,553 | 10.8 | **7.7** | main, 1.4× |
| sparse stream, full range | 535,547 | 396.0 | **178.5** | main, 2.2× |
| numeric stream, 1 day | 8,553 | 10.4 | **6.8** | main, 1.5× |
| numeric stream, full range | 535,547 | 397.4 | **179.3** | main, 2.2× |
| empty range | 0 | 2.6 | **2.4** | main, 1.1× |

### Reading these numbers

- **The trade is ingest and storage for scan latency.** `ums-duckdb-fix` ingests
  6.6× faster into a 3.2× smaller store, and gives back ~2× on unbounded and
  wide-window reads — consistent with dropping the five secondary indexes and
  interning `ref_uri` into an integer `ref_id`.
- **Bounded queries barely move.** At 1 hour or less the two branches are within
  0.6 ms of each other; the gap only opens past ~10k rows returned.
- **Two shapes go the other way.** `timeseries_info` is 1.9× faster on the fix
  branch, and `limit 1000` is **4.8× faster** (7.3 vs 35.2 ms) — on `main` a
  small `LIMIT` over an unbounded range does not exploit its indexes, which is
  worth a look since "sample this stream" is a common UI call.
- **Not fixed by this branch:** all 112 `point_uri`s from
  `property_to_column.json` still register with `value_kind="text"`, so
  `value_mode="numeric"` returns nothing for any semantically-addressed point
  (measured: 0 of 20 sampled points answer a numeric-mode query, on **both**
  branches). Root cause is driver-side —
  `DPRTrailerCSVDriver._register_streams_from_lookup()` re-registers the lookup
  bindings without a `value_kind`, clobbering the kind inferred from the data.
  Unmapped streams infer correctly (114 numeric / 35 text). The numeric query
  shapes above therefore address an unmapped stream by computed ref URI.

<details>
<summary>Caveats</summary>

- Single run per branch, single client thread; no concurrency in the query phase.
- Query windows are anchored on a real row (newest timestamp via
  `limit=1&order=desc`), not on the midpoint of the reported range — DPR exports
  contain occasional decades-off timestamps that otherwise put windows in a void.
- The `system_metrics` driver was omitted so it could not pollute row counts.
- `main`'s store reached 24 GB against a 16 GiB RAM cap without being OOM-killed.

</details>
