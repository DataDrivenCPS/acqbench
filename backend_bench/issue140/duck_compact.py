"""Compact copy of the timeseries table (order preserved) into a fresh file; prints sizes."""
import duckdb, os, sys, time
src, out = sys.argv[1], sys.argv[2]
if os.path.exists(out): os.remove(out)
con = duckdb.connect(out)
con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
t = time.time()
con.execute("CREATE TABLE timeseries AS SELECT * FROM src.timeseries")
n = con.execute("SELECT count(*) FROM timeseries").fetchone()[0]
con.execute("CHECKPOINT")
s1 = os.path.getsize(out)
con.execute("CREATE INDEX idx_timeseries_last_revision ON timeseries (last_revision)")
con.execute("CHECKPOINT"); con.close()
s2 = os.path.getsize(out)
print(f"COMPACT rows={n} data_only_bytes={s1} with_last_revision_index_bytes={s2} seconds={time.time()-t:.1f}")
