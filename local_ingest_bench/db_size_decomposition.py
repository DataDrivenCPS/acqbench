"""Where do 181 bytes/row go? Insert identical melted DPR data into
(a) main's full schema (UNIQUE + 5 indexes) and (b) a bare table, and
compare file sizes. Uses fast polars melt — pipeline speed is not under
test here, only storage footprint."""

import sys
import time
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from ingest_path_bench import (
    BenchDuckDBStore, compute_ref_uri, _safe_name, detect_header_line,
    SOURCE_ID, DATE_COL, CLOCK_COL, DATE_FMT, TIMESERIES_TABLE,
)

DATA_DIR = Path.home() / "Resilio Sync" / "DPR Pilot Logs New"
TMP = Path(__file__).parent / "tmp_db"
TMP.mkdir(exist_ok=True)

paths = sorted(DATA_DIR.rglob("*.csv"))[::500]  # ~11 files spread across time
frames = []
for p in paths:
    try:
        hdr = detect_header_line(p)
    except ValueError:
        continue
    df = pl.read_csv(p, skip_rows=hdr - 1, encoding="utf8-lossy",
                     truncate_ragged_lines=True, infer_schema_length=200,
                     schema_overrides={DATE_COL: pl.Utf8, CLOCK_COL: pl.Utf8})
    if df.is_empty() or DATE_COL not in df.columns:
        continue
    ts = (pl.concat_str([pl.col(DATE_COL).str.strip_chars(), pl.lit(" "),
                         pl.col(CLOCK_COL).str.strip_chars()])
          .str.to_datetime(DATE_FMT, strict=False))
    stream_cols = [c for c in df.columns if c not in (DATE_COL, CLOCK_COL)]
    long = (df.with_columns(ts.alias("ts"))
              .select(["ts"] + [pl.col(c).cast(pl.Utf8, strict=False) for c in stream_cols])
              .unpivot(on=stream_cols, index="ts", variable_name="ref_name", value_name="value")
              .drop_nulls(subset=["ts", "value"])
              .filter(pl.col("value") != "NaN"))
    frames.append(long)

long = pl.concat(frames)
ref_map = {n: compute_ref_uri(SOURCE_ID, _safe_name(n)) for n in long["ref_name"].unique().to_list()}
data = (long.with_columns(pl.col("ref_name").replace(ref_map).alias("ref_uri"),
                          pl.col("value").cast(pl.Float64, strict=False).alias("numeric_value"))
            .with_columns(pl.when(pl.col("numeric_value").is_null())
                            .then(pl.col("value")).otherwise(None).alias("text_value"))
            .select(["ref_uri", "ts", "numeric_value", "text_value"])
            .unique(subset=["ref_uri", "ts"], keep="last"))
n = len(data)
print(f"{n:,} melted rows from {len(paths)} files "
      f"({sum(p.stat().st_size for p in paths)/1e6:.1f} MB csv)", flush=True)

results = {}
for label in ("full_schema", "bare_table"):
    db = TMP / f"decomp_{label}.duckdb"
    db.unlink(missing_ok=True)
    t0 = time.perf_counter()
    if label == "full_schema":
        store = BenchDuckDBStore(db, recreate=True)
        conn = store._conn
    else:
        conn = duckdb.connect(str(db))
        conn.execute(f"""CREATE TABLE {TIMESERIES_TABLE} (
            ref_uri VARCHAR NOT NULL, ts TIMESTAMP NOT NULL,
            numeric_value DOUBLE, text_value VARCHAR)""")
    conn.register("incoming", data)
    conn.execute(f"INSERT INTO {TIMESERIES_TABLE} SELECT * FROM incoming")
    conn.unregister("incoming")
    conn.close()
    size = db.stat().st_size
    results[label] = size
    print(f"{label:12s}: {size/1e6:8.1f} MB  ({size/n:6.1f} B/row)  "
          f"insert+checkpoint {time.perf_counter()-t0:.1f}s", flush=True)
    db.unlink()

print(f"index overhead: {(results['full_schema']-results['bare_table'])/1e6:.1f} MB "
      f"= {results['full_schema']/results['bare_table']:.1f}x the bare table", flush=True)
