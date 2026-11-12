"""Smoke test for a DuckDBStore variant: overlapping inserts, merges, reads."""
import os, sys, tempfile, datetime as dt
os.environ["ACQUIRIUM_DUCKDB_STAGING_ROWS"] = "5"
import polars as pl
from acquirium.Storage.duckdb_store import DuckDBStore
import acquirium.Storage.duckdb_store as m
variant = sys.argv[1]
p = tempfile.mkdtemp() + "/t.duckdb"
s = DuckDBStore(p, recreate=True)
T0 = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
def frame(uri, start, n, off=0.0):
    return pl.DataFrame({"ref_uri": [uri]*n,
                         "ts": pl.Series([T0 + dt.timedelta(seconds=start+i) for i in range(n)]).dt.replace_time_zone("UTC"),
                         "numeric_value": [float(start+i)+off for i in range(n)],
                         "text_value": [None]*n}).with_columns(pl.col("text_value").cast(pl.Utf8))
print("A", s.bulk_insert_polars(frame("u1", 0, 10)))
print("B", s.bulk_insert_polars(frame("u1", 5, 10, 100.0)))
print("C", s.bulk_insert_polars(frame("u2", 0, 3)))
def read(uri, **kw):
    import pyarrow as pa
    bs = list(s.timeseries(uri, **kw)); return pa.Table.from_batches(bs).to_pydict() if bs else {"ts": [], "value": []}
r = read("u1"); print("u1 rows", len(r["ts"]), "vals@5..9", r["value"][5:10] if len(r["ts"])>=10 else r["value"])
print("u2 rows", len(read("u2")["ts"]), "desc limit 2", read("u1", order="desc", limit=2)["value"])
print("info", {k: v.row_count for k, v in s.timeseries_info_batch(["u1","u2","u3"]).items()})
if hasattr(s, "merge_staging"):
    print("merge", s.merge_staging(force=True))
    con = s._connect()
    print("staging", con.execute("SELECT count(*) FROM timeseries_staging").fetchone(), "main", con.execute("SELECT count(*) FROM timeseries").fetchone())
    rows = con.execute("SELECT ref_id, ts FROM timeseries").fetchall(); print("main sorted:", rows == sorted(rows))
    print("tables", [t[0] for t in con.execute("SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall()])
    print("views", [t[0] for t in con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1").fetchall()])
    con.close()
    r = read("u1"); print("after merge u1 rows", len(r["ts"]), "vals@5..9", r["value"][5:10])
    print("u1 window", len(read("u1", start=T0+dt.timedelta(seconds=3), end=T0+dt.timedelta(seconds=6))["ts"]))
con = s._connect(); print("indexes", [t[0] for t in con.execute("SELECT index_name FROM duckdb_indexes() ORDER BY 1").fetchall()]); con.close()
s.close(); print("OK", variant)
