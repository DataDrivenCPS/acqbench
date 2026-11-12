import duckdb, sys, time
con = duckdb.connect(sys.argv[1], read_only=True)
t = time.perf_counter()
rid, nrows = con.execute("SELECT ref_id, count(*) c FROM timeseries WHERE NOT deleted GROUP BY ref_id ORDER BY c DESC LIMIT 1").fetchone()
tmin, tmax = con.execute(f"SELECT min(ts), max(ts) FROM timeseries WHERE ref_id={rid} AND NOT deleted").fetchone()
print(f"ref_id {rid}: {nrows} rows, {tmin} .. {tmax}  (lookup {time.perf_counter()-t:.1f}s)", flush=True)
def timed(label, sql, n):
    best = 1e9; rows = 0
    for _ in range(n):
        t = time.perf_counter(); r = con.execute(sql).fetchall(); best = min(best, (time.perf_counter()-t)*1000); rows = len(r)
    print(f"{label:<66} {rows:>9} rows {best:9.1f} ms", flush=True)
shapes = {
 "whole range explicit (X)": (f"ts >= '{tmin}' AND ts <= '{tmax}'", "ORDER BY ts", 2),
 "3-year window desc (B)":   (f"ts >= TIMESTAMP '{tmax}' - INTERVAL 3 YEAR AND ts <= '{tmax}'", "ORDER BY ts DESC", 2),
 "1-year window (S/Y)":      (f"ts >= TIMESTAMP '{tmax}' - INTERVAL 365 DAY AND ts <= '{tmax}'", "ORDER BY ts", 2),
 "30-day window":            (f"ts >= TIMESTAMP '{tmax}' - INTERVAL 30 DAY AND ts <= '{tmax}'", "ORDER BY ts", 3),
}
for name, (win, order, n) in shapes.items():
    where = f"ref_id={rid} AND NOT deleted AND {win}"
    plain = f"SELECT ts, numeric_value, text_value FROM timeseries WHERE {where} {order}"
    cte = f"WITH t AS MATERIALIZED (SELECT ts, numeric_value, text_value FROM timeseries WHERE {where}) SELECT * FROM t {order}"
    con.execute("SET disabled_optimizers=''"); timed(f"{name}: plain (d0cc2c4)", plain, n)
    timed(f"{name}: MATERIALIZED CTE (0a8eb80)", cte, n)
    con.execute("SET disabled_optimizers='partitioned_execution'"); timed(f"{name}: plain + partitioned_execution off", plain, n)
    con.execute("SET disabled_optimizers=''")
print("DIAG_BIG_END", flush=True)
