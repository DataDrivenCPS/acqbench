# Issue 140 measurement scripts

Scripts behind `reports/db_bloat_issue140_*.md`. They were run on the Jetstream host
(`exouser@149.165.173.175`) from `~/bench/`; paths inside assume that layout.

| File | Used in |
|---|---|
| `duck_storage.py`, `duck_blocks.py`, `duck_copy_test.py`, `duck_compact.py` | DuckDB file breakdown (data vs index blocks, compact copy) |
| `size_breakdown.sql`, `size_compressed.sql`, `compress_all.sh` | Timescale breakdown before/after `compress_chunk` |
| `run_all.sh`, `v2/duckdb_store.py`, `v3/duckdb_store.py`, `test_store.py`, `final_merge.py` | the three DuckDB write-path variants on 1,000 files |
| `run_fix3.sh`, `run_fix3c.sh`, `patch_fix3x.py`, `diag_prune5.py` | branch `ums-duckdb-fix-3`: 1,000 files then the full set, the `partitioned_execution` diagnosis |
