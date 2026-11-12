"""Storage breakdown of a closed acquirium DuckDB file. Usage: python duck_storage.py <db> [--copy-test]"""
import sys, time, json, os, duckdb
db = sys.argv[1]; copy_test = "--copy-test" in sys.argv
def q(c, sql): return c.execute(sql).fetchall()
def t(label, f):
    t0 = time.time(); r = f(); print(f"[{time.time()-t0:7.1f}s] {label}", flush=True); return r
c = duckdb.connect(db, read_only=True)
c.execute("SET threads=16; SET memory_limit='60GB'")
print("file bytes:", os.path.getsize(db))
print("database_size:", q(c, "PRAGMA database_size"))
print("tables:", q(c, "SELECT table_name, estimated_size, column_count, index_count FROM duckdb_tables() ORDER BY estimated_size DESC"))
print("indexes:", q(c, "SELECT table_name, index_name, is_unique, sql FROM duckdb_indexes()"))
rows = t("count", lambda: q(c, "SELECT count(*) FROM timeseries"))[0][0]; print("timeseries rows:", rows)
# per-column storage of timeseries via storage_info (sums over all row groups / segments)
si = t("storage_info", lambda: q(c, """
  SELECT column_name, column_id, count(*) AS segments, sum(count) AS values,
         string_agg(DISTINCT compression, ',') AS compressions,
         count(DISTINCT row_group_id) AS row_groups,
         sum(CASE WHEN segment_type IS NOT NULL THEN 1 ELSE 0 END) AS typed_segments
  FROM pragma_storage_info('timeseries') GROUP BY 1,2 ORDER BY 2"""))
for r in si: print("  col", r)
blk = q(c, "SELECT block_size FROM pragma_database_size()")
print("block_size:", blk)
# distinct blocks touched per column: storage_info has block_id; count distinct (block_id) per column -> approx bytes = blocks*block_size
bc = t("blocks per column", lambda: q(c, """
  SELECT column_name, count(DISTINCT block_id) AS blocks, count(DISTINCT block_id)*262144 AS approx_bytes
  FROM pragma_storage_info('timeseries') WHERE block_id IS NOT NULL AND block_id >= 0 GROUP BY 1 ORDER BY 2 DESC"""))
tot = 0
for r in bc: print("  blocks", r); tot += r[1]
print("timeseries data blocks total:", tot, "≈", tot*262144/1e9, "GB")
print("other tables storage:", t("others", lambda: q(c, """
  SELECT t.table_name, count(DISTINCT s.block_id) AS blocks FROM duckdb_tables() t,
  LATERAL (SELECT block_id FROM pragma_storage_info(t.table_name)) s WHERE t.table_name <> 'timeseries' GROUP BY 1 ORDER BY 2 DESC""")))
print("value kinds / deleted / revisions:", t("kinds", lambda: q(c, """
  SELECT count(*) FILTER (WHERE numeric_value IS NOT NULL), count(*) FILTER (WHERE text_value IS NOT NULL),
         count(*) FILTER (WHERE deleted), count(DISTINCT last_revision), count(DISTINCT ref_id), min(ts), max(ts) FROM timeseries""")))
print("text stats:", t("text", lambda: q(c, "SELECT avg(length(text_value)), count(DISTINCT text_value) FROM timeseries WHERE text_value IS NOT NULL")))
print("ref_ids / streams:", q(c, "SELECT (SELECT count(*) FROM ref_ids), (SELECT count(*) FROM streams)"))
c.close()
if copy_test:
    out = db + ".noindex_copy"; 
    if os.path.exists(out): os.remove(out)
    d = duckdb.connect(out); d.execute("SET threads=16; SET memory_limit='60GB'")
    d.execute(f"ATTACH '{db}' AS src (READ_ONLY)")
    t("CTAS timeseries (no index)", lambda: d.execute("CREATE TABLE timeseries AS SELECT * FROM src.timeseries ORDER BY ref_id, ts"))
    d.execute("CHECKPOINT"); d.close()
    print("no-index copy bytes:", os.path.getsize(out))
    d = duckdb.connect(out); d.execute("SET threads=16; SET memory_limit='60GB'")
    t("CREATE UNIQUE INDEX (ref_id, ts)", lambda: d.execute("CREATE UNIQUE INDEX u ON timeseries(ref_id, ts)"))
    d.execute("CHECKPOINT"); d.close(); print("with unique index bytes:", os.path.getsize(out))
    d = duckdb.connect(out); d.execute("SET threads=16; SET memory_limit='60GB'")
    t("CREATE INDEX (last_revision)", lambda: d.execute("CREATE INDEX r ON timeseries(last_revision)"))
    d.execute("CHECKPOINT"); d.close(); print("with both indexes bytes:", os.path.getsize(out))
