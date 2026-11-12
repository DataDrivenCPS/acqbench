import sys, time, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
from acquirium.Storage.duckdb_store import DuckDBStore
s = DuckDBStore(sys.argv[1])
t = time.time(); info = s.merge_staging(force=True); s.close()
print("FINAL_MERGE", info, "wall_s", round(time.time() - t, 1), flush=True)
