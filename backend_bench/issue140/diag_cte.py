import duckdb, sys, time
con=duckdb.connect(sys.argv[1], read_only=True)
rid=214; W0, W1 = "2024-03-11 09:47:16", "2024-03-11 10:47:16"
def timed(label, sql, n=3):
    best=1e9
    for _ in range(n):
        t=time.perf_counter(); r=con.execute(sql).fetchall(); best=min(best,(time.perf_counter()-t)*1000)
    print(f"{label:<64} {len(r):>9} rows {best:9.1f} ms", flush=True)
shapes = {
 "1h window":        (f"ref_id={rid} AND NOT deleted AND ts >= '{W0}' AND ts <= '{W1}'", "ORDER BY ts", ""),
 "latest 1 desc":    (f"ref_id={rid} AND NOT deleted", "ORDER BY ts DESC", " LIMIT 1"),
 "limit 1000 asc":   (f"ref_id={rid} AND NOT deleted", "ORDER BY ts", " LIMIT 1000"),
 "30d limit 10000":  (f"ref_id={rid} AND NOT deleted AND ts >= '2024-02-10' AND ts <= '2024-03-11 10:47:16'", "ORDER BY ts", " LIMIT 10000"),
 "full stream":      (f"ref_id={rid} AND NOT deleted", "ORDER BY ts", ""),
}
for name,(where,order,lim) in shapes.items():
    plain=f"SELECT ts, numeric_value, text_value FROM timeseries WHERE {where} {order}{lim}"
    cte=f"WITH t AS MATERIALIZED (SELECT ts, numeric_value, text_value FROM timeseries WHERE {where}) SELECT * FROM t {order}{lim}"
    con.execute("SET disabled_optimizers=''"); timed(f"{name}: plain (d0cc2c4)", plain, 1 if name=="full stream" else 3)
    timed(f"{name}: MATERIALIZED CTE (2215185)", cte, 1 if name=="full stream" else 3)
    con.execute("SET disabled_optimizers='partitioned_execution'"); timed(f"{name}: plain + partitioned_execution off", plain, 1 if name=="full stream" else 3)
    con.execute("SET disabled_optimizers=''")
