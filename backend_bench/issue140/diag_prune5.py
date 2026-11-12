import duckdb, sys, time
con=duckdb.connect(sys.argv[1], read_only=True)
rid=214; W0, W1 = "2024-03-11 09:47:16", "2024-03-11 10:47:16"
Q=f"SELECT ts, numeric_value, text_value FROM timeseries WHERE ref_id={rid} AND ts >= '{W0}' AND ts <= '{W1}' ORDER BY ts"
QF=f"SELECT ts, numeric_value, text_value FROM timeseries WHERE ref_id={rid} ORDER BY ts"
def timed(label, sql, n=2):
    best=1e9
    for _ in range(n):
        t=time.perf_counter(); r=con.execute(sql).fetchall(); best=min(best,(time.perf_counter()-t)*1000)
    print(f"{label:<62} {len(r):>9} rows {best:9.1f} ms", flush=True); return best
opts=[r[0] for r in con.execute("SELECT name FROM duckdb_optimizers() ORDER BY 1").fetchall()]
print("optimizers:", ", ".join(opts))
ex=con.execute("EXPLAIN "+Q).fetchall()[0][1]
print("ORDER BY branches in plan:", ex.count("ORDER_BY")+ex.count("Order By:"), "| union nodes:", ex.count("UNION")+ex.count("Union"))
base=timed("baseline ORDER BY ts", Q)
for o in opts:
    if any(k in o.lower() for k in ("sort","order","partition","union","top","limit","statistics","cardinal")):
        con.execute(f"SET disabled_optimizers='{o}'")
        timed(f"disabled {o}", Q)
con.execute("SET disabled_optimizers=''")
timed("MATERIALIZED CTE then ORDER BY", f"WITH t AS MATERIALIZED (SELECT ts, numeric_value, text_value FROM timeseries WHERE ref_id={rid} AND ts >= '{W0}' AND ts <= '{W1}') SELECT * FROM t ORDER BY ts")
timed("no ORDER BY", Q.replace(" ORDER BY ts",""))
timed("full stream (15.6M) ORDER BY ts", QF, 1)
timed("full stream no ORDER BY", QF.replace(" ORDER BY ts",""), 1)
