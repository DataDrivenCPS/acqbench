import sys, re
p=sys.argv[1]; s=open(p).read()
m=re.search(r"    def _connect\(self\):\n(.*?\n)(        return self\._duckdb\.connect\(str\(self\.db_path\)\)\n)", s, re.S)
assert m, "connect body not found"
new = m.group(0).replace(m.group(2), """        conn = self._duckdb.connect(str(self.db_path))
        # BENCH: DuckDB 2.0's partitioned_execution rewrites ORDER BY into a union of
        # range-partitioned scans; each branch re-evaluates zonemaps over every row
        # group, so small window reads cost ~0.8 s on 32k row groups.
        conn.execute("SET disabled_optimizers='partitioned_execution'")
        return conn
""")
s=s.replace(m.group(0), new); open(p,"w").write(s); print("patched _connect")
