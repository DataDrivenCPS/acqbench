import duckdb, sys
c = duckdb.connect(sys.argv[1], read_only=True); c.execute("SET threads=8")
B = 262144
blocks = c.execute("SELECT count(DISTINCT block_id) FROM pragma_storage_info('timeseries') WHERE block_id IS NOT NULL AND block_id >= 0").fetchone()[0]
rg = c.execute("SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('timeseries')").fetchone()[0]
used = c.execute("SELECT used_blocks, total_blocks, free_blocks FROM pragma_database_size()").fetchone()
others = 0
for (t,) in c.execute("SELECT table_name FROM duckdb_tables() WHERE table_name <> 'timeseries'").fetchall():
    others += c.execute(f"SELECT count(DISTINCT block_id) FROM pragma_storage_info('{t}') WHERE block_id IS NOT NULL AND block_id >= 0").fetchone()[0]
print(f"used blocks {used[0]} = {used[0]*B/1e9:.1f} GB")
print(f"timeseries data blocks {blocks} = {blocks*B/1e9:.1f} GB")
print(f"other tables {others} blocks = {others*B/1e6:.1f} MB")
print(f"remainder = indexes + metadata: {(used[0]-blocks-others)*B/1e9:.1f} GB")
print(f"row groups {rg}, avg rows per row group {3554166660/rg:.0f}")
