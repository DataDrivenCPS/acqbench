"""Rebuild timeseries in a new file without indexes, then add each index and record the file size after each step."""
import sys, time, os, duckdb
src, out = sys.argv[1], sys.argv[2]
def step(label, f):
    t0 = time.time(); f(); print(f"[{time.time()-t0:8.1f}s] {label}", flush=True)
def size(label):
    print(f"SIZE {label}: {os.path.getsize(out)} bytes = {os.path.getsize(out)/1e9:.2f} GB", flush=True)
def connect():
    d = duckdb.connect(out); d.execute("SET threads=24; SET memory_limit='80GB'; SET temp_directory='/home/exouser/run3/tmp'"); return d
if os.path.exists(out): os.remove(out)
d = connect(); d.execute(f"ATTACH '{src}' AS s (READ_ONLY)")
step("CTAS timeseries, insertion order, no index", lambda: d.execute("CREATE TABLE timeseries AS SELECT * FROM s.timeseries"))
d.execute("DETACH s"); d.execute("CHECKPOINT"); d.close(); size("data only")
d = connect(); step("CREATE INDEX idx_last_revision", lambda: d.execute("CREATE INDEX idx_last_revision ON timeseries(last_revision)")); d.execute("CHECKPOINT"); d.close(); size("data + last_revision index")
d = connect(); step("CREATE UNIQUE INDEX (ref_id, ts)", lambda: d.execute("CREATE UNIQUE INDEX idx_ref_ts ON timeseries(ref_id, ts)")); d.execute("CHECKPOINT"); d.close(); size("data + both indexes")
print("DONE", flush=True)
