#!/usr/bin/env python
"""Query-latency benchmark: main's timeseries() / timeseries_info() vs index choice.

Phase 1 — build three DuckDB stores from DPR CSVs, ~100M rows each, one per
index configuration (none / idx(ref_uri) / idx(ref_uri, ts)). The CSV is
parsed, melted, assembled, and split ONCE per file (the Python pipeline is
index-independent, as established in the ingest benchmark); only the per-store
SQL (DELETE+INSERT+COMMIT) runs three times. Ingest cost per config =
shared pipeline time + that store's SQL time.

Phase 2 — timeseries_info(ref_uri) (COUNT/MIN/MAX, on the store's write
connection under its lock, replicated verbatim) for EVERY registered ref_uri,
100 repetitions each, per store.

Phase 3 — timeseries() (fresh connection per call, streamed Arrow batches,
value-mode handling — replicated verbatim from main) over 20 generated query
specs of varying selectivity: 19 verified non-empty, 1 deliberately empty.
Each spec runs 100 times per store; the iterator is fully consumed.

Raw latencies land in results/query-bench/<run>/results.json.
The three DBs are kept in tmp_db/ for follow-up queries (delete manually).
"""

from __future__ import annotations

import json
import signal
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, str(Path(__file__).parent))
from ingest_path_bench import (
    SOURCE_ID, TIMESERIES_TABLE,
    BenchDuckDBStore, BenchManager,
    assign_stream_value_kind, build_ts,
    melt_wide_to_long, prepare_value_columns,
    read_csv_pandas,
)

# Storage/values.py @ main
VALUE_MODES = {"default", "coalesce", "numeric", "text"}


def normalize_value_mode(value_mode: Any = None) -> str:
    if value_mode is None:
        return "default"
    value = str(value_mode).strip().lower()
    if value in VALUE_MODES:
        return value
    raise ValueError(
        f"value_mode must be 'default', 'coalesce', 'numeric', or 'text', got {value_mode!r}"
    )

TARGET_ROWS = 100_000_000
# timeseries_info is a full COUNT/MIN/MAX per call — hundreds of ms on a 100M-row
# store — and its per-call variance is tiny (p95 ~1.6x mean), so 10 reps pin the
# mean as well as 100 did at a tenth of the cost. timeseries() is milliseconds
# and is where run-to-run spread actually matters, so it keeps 100.
INFO_REPS = 10
TS_REPS = 100
CONFIGS = {
    "noidx": [],
    "ref":   [f"CREATE INDEX IF NOT EXISTS idx_ref ON {TIMESERIES_TABLE} (ref_uri)"],
    "tsref": [f"CREATE INDEX IF NOT EXISTS idx_ts_ref ON {TIMESERIES_TABLE} (ref_uri, ts)"],
}

HERE = Path(__file__).parent
DATA_DIR = Path.home() / "Resilio Sync" / "DPR Pilot Logs New"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---- verbatim query replicas (duckdb_store.py @ main) ----------------------

def _to_utc_naive(ts: datetime) -> datetime:
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts


def _stream_value_kind(conn, ref_uri: str) -> str | None:
    row = conn.execute(
        "SELECT value_kind FROM streams WHERE ref_uri = ?", [ref_uri]
    ).fetchone()
    return None if row is None else row[0]


def timeseries_replica(
    store: BenchDuckDBStore,
    ref_uri: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int | None = None,
    order: str = "asc",
    batch_size: int = 50_000,
    value_mode: str = "default",
):
    """Generator identical to DuckDBStore.timeseries on main (logging removed)."""
    mode = normalize_value_mode(value_mode)
    clauses = ["ref_uri = ?"]
    params: list[Any] = [ref_uri]
    if start:
        clauses.append("ts >= ?")
        params.append(_to_utc_naive(start))
    if end:
        clauses.append("ts <= ?")
        params.append(_to_utc_naive(end))
    if mode == "numeric":
        clauses.append("numeric_value IS NOT NULL")
    elif mode == "text":
        clauses.append("text_value IS NOT NULL")

    where = " AND ".join(clauses)
    order_sql = "ASC" if order == "asc" else "DESC"
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT ts, numeric_value, text_value
        FROM {TIMESERIES_TABLE}
        WHERE {where}
        ORDER BY ts {order_sql}{limit_sql}
    """
    conn = duckdb.connect(str(store.db_path))
    try:
        value_kind = _stream_value_kind(conn, ref_uri)
        reader = conn.execute(query, params).to_arrow_reader(batch_size)
        for batch in reader:
            numeric_col = batch.column("numeric_value")
            text_col = batch.column("text_value")
            if mode == "coalesce":
                numeric_values = numeric_col.to_pylist()
                text_values = text_col.to_pylist()
                values = [
                    text if text is not None else (str(numeric) if numeric is not None else None)
                    for numeric, text in zip(numeric_values, text_values)
                ]
                val_col = pa.array(values, type=pa.string())
            elif mode == "text" or value_kind == "text":
                val_col = text_col.cast(pa.string())
            elif mode == "numeric" or value_kind == "numeric":
                val_col = numeric_col.cast(pa.float64())
            elif numeric_col.null_count < len(numeric_col):
                val_col = numeric_col.cast(pa.float64())
            else:
                val_col = text_col.cast(pa.string())
            ts_col = pc.assume_timezone(batch.column("ts"), timezone="UTC")
            uri_col = pa.array([ref_uri] * len(batch), type=pa.string())
            yield pa.record_batch([ts_col, val_col, uri_col], names=["ts", "value", "uri"])
    finally:
        conn.close()


def timeseries_info_replica(store: BenchDuckDBStore, ref_uri: str):
    """Identical to DuckDBStore.timeseries_info on main (model wrapper removed)."""
    with store._lock:
        row = store._conn.execute(
            f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM {TIMESERIES_TABLE} WHERE ref_uri = ?",
            [ref_uri],
        ).fetchone()
    return (row[0] or 0, row[1], row[2]) if row else (0, None, None)


# ---- phase 1: build the three stores ---------------------------------------

def db_path_for(name: str) -> Path:
    return HERE / "tmp_db" / f"query_bench_{name}.duckdb"


def open_existing_stores() -> tuple[dict[str, BenchDuckDBStore], dict[str, Any]]:
    """Reuse the DBs a previous build left behind (identical row counts)."""
    stores: dict[str, BenchDuckDBStore] = {}
    counts: dict[str, int] = {}
    sizes: dict[str, int] = {}
    for name in CONFIGS:
        db = db_path_for(name)
        if not db.exists():
            raise SystemExit(f"--reuse-db: {db} not found; run a build first")
        store = BenchDuckDBStore(db, recreate=False, no_indexes=True)
        with store._lock:
            counts[name] = store._conn.execute(
                f"SELECT count(*) FROM {TIMESERIES_TABLE}").fetchone()[0]
            idx = [r[0] for r in store._conn.execute(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'timeseries'").fetchall()]
        expected = [d.split(" ON ")[0].split()[-1] for d in CONFIGS[name]]
        if sorted(idx) != sorted(expected):
            raise SystemExit(f"--reuse-db: {name} has indexes {idx}, expected {expected}")
        sizes[name] = db.stat().st_size
        stores[name] = store
    if len(set(counts.values())) != 1:
        raise SystemExit(f"--reuse-db: row counts differ across stores: {counts}")
    info = {"reused": True, "rows_per_store": next(iter(counts.values())), "db_bytes": sizes}
    log(f"reusing existing stores: {json.dumps(info)}")
    return stores, info


def build_stores() -> tuple[dict[str, BenchDuckDBStore], dict[str, Any]]:
    stores: dict[str, BenchDuckDBStore] = {}
    mgrs: dict[str, BenchManager] = {}
    for name, extra_ddl in CONFIGS.items():
        db = db_path_for(name)
        db.unlink(missing_ok=True)
        db.with_suffix(db.suffix + ".wal").unlink(missing_ok=True)
        store = BenchDuckDBStore(db, recreate=True, no_indexes=True)
        for ddl in extra_ddl:
            store._conn.execute(ddl)
        stores[name] = store
        mgrs[name] = BenchManager(store)

    paths = sorted(
        {p for pat in ("*.csv", "*.tsv") for p in DATA_DIR.rglob(pat)
         if not any(part.startswith(".") for part in p.relative_to(DATA_DIR).parts)}
    )
    registered: set[str] = set()
    inserted = 0
    files_used = 0
    shared_s = 0.0
    sql_s = {name: 0.0 for name in CONFIGS}
    t_build0 = time.perf_counter()

    for path in paths:
        if inserted >= TARGET_ROWS:
            break
        try:
            t0 = time.perf_counter()
            df, _ragged = read_csv_pandas(path)
            if len(df) == 0:
                continue
            ts, _bad = build_ts(df)
            long, _counts = melt_wide_to_long(df, ts)
            del df
            if len(long) == 0:
                continue
            kinds = {rn: assign_stream_value_kind(v.tolist())
                     for rn, v in long.groupby("ref_name", sort=False)["value"]}
            new = [n for n in kinds if n not in registered]
            if new:
                refs = [(None, SOURCE_ID, n, None, kinds[n]) for n in new]
                for store in stores.values():
                    store.ensure_stream_refs(refs)
                registered.update(new)
            # endpoint + manager assembly, once (identical across stores)
            streams: dict[str, list] = {}
            for rn, grp in long.groupby("ref_name", sort=False):
                streams[rn] = list(zip(grp["ts"].tolist(), grp["value"].tolist()))
            del long
            batch_df = mgrs["noidx"].insert_timeseries_batch_assemble(SOURCE_ID, streams)
            del streams
            if batch_df is None:
                continue
            # bulk_insert_polars' store-independent half, once
            split = prepare_value_columns(batch_df).with_columns(
                pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            ).unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
            del batch_df
            shared_s += time.perf_counter() - t0

            # store-specific half: DELETE+INSERT+COMMIT per config
            n = 0
            for name, store in stores.items():
                t1 = time.perf_counter()
                with store._lock:
                    store._conn.register("_acquirium_incoming_timeseries", split)
                    try:
                        store._conn.execute("BEGIN TRANSACTION")
                        store._conn.execute(
                            f"""DELETE FROM {TIMESERIES_TABLE}
                                USING _acquirium_incoming_timeseries AS incoming
                                WHERE {TIMESERIES_TABLE}.ref_uri = incoming.ref_uri
                                  AND {TIMESERIES_TABLE}.ts = incoming.ts"""
                        )
                        store._conn.execute(
                            f"""INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value)
                                SELECT ref_uri, ts, numeric_value, text_value
                                FROM _acquirium_incoming_timeseries"""
                        )
                        store._conn.execute("COMMIT")
                    except Exception:
                        store._conn.execute("ROLLBACK")
                        raise
                    finally:
                        store._conn.unregister("_acquirium_incoming_timeseries")
                sql_s[name] += time.perf_counter() - t1
                n = len(split)
            del split
            inserted += n
            files_used += 1
            if files_used % 25 == 0:
                el = time.perf_counter() - t_build0
                log(f"build: {files_used} files, {inserted/1e6:.1f}M rows, "
                    f"{inserted/el:,.0f} rows/s wall, sql_s={ {k: round(v) for k, v in sql_s.items()} }")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log(f"build FAIL {path.name}: {type(e).__name__}: {e}")

    ckpt_s = {}
    sizes = {}
    for name, store in stores.items():
        t1 = time.perf_counter()
        with store._lock:
            store._conn.execute("CHECKPOINT")
        ckpt_s[name] = round(time.perf_counter() - t1, 2)
        sizes[name] = store.db_path.stat().st_size
    build_info = {
        "files_used": files_used, "rows_per_store": inserted,
        "shared_pipeline_s": round(shared_s, 1),
        "store_sql_s": {k: round(v, 1) for k, v in sql_s.items()},
        "checkpoint_s": ckpt_s,
        "db_bytes": sizes,
        "build_wall_s": round(time.perf_counter() - t_build0, 1),
        "streams_registered": len(registered),
    }
    log(f"build done: {json.dumps(build_info)}")
    return stores, build_info


# ---- phase 3 helpers: query spec generation --------------------------------

def make_query_specs(store: BenchDuckDBStore) -> list[dict[str, Any]]:
    """20 timeseries() specs of varying selectivity; 19 non-empty, 1 empty."""
    with store._lock:
        rows = store._conn.execute(
            f"""SELECT t.ref_uri, s.ref_name, s.value_kind, count(*) n, min(ts) mn, max(ts) mx
                FROM {TIMESERIES_TABLE} t JOIN streams s USING (ref_uri)
                GROUP BY 1,2,3 ORDER BY n DESC"""
        ).fetchall()
    numeric_rows = [r for r in rows if r[2] == "numeric"]
    dense = numeric_rows[0]
    dense_text = [r for r in rows if r[2] == "text"][0]
    # mid comes from the numeric streams so the value_mode="numeric" spec
    # cannot be accidentally empty on a text stream
    mid = numeric_rows[len(numeric_rows) // 2]
    sparse = [r for r in rows if r[3] > 0][-1]

    def utc(dt): return dt.replace(tzinfo=timezone.utc)

    def anchor(row, frac: float) -> datetime:
        """A timestamp that provably has data: the row at OFFSET n*frac.

        Windows [anchor, anchor+delta] always contain at least the anchor row,
        so gaps in the SCADA record can't produce accidentally-empty specs.
        """
        with store._lock:
            (ts,) = store._conn.execute(
                f"SELECT ts FROM {TIMESERIES_TABLE} WHERE ref_uri = ? "
                f"ORDER BY ts LIMIT 1 OFFSET {int(row[3] * frac)}",
                [row[0]],
            ).fetchone()
        return utc(ts)

    specs = []

    def add(label, row, *, dt0=None, dt1=None, limit=None, order="asc", mode="default"):
        specs.append(dict(
            label=label, ref_uri=row[0], ref_name=row[1],
            start=None if dt0 is None else dt0.isoformat(),
            end=None if dt1 is None else dt1.isoformat(),
            limit=limit, order=order, value_mode=mode,
        ))

    d_mid = anchor(dense, 0.5)
    for label, delta in [
        ("dense full range", None), ("dense 1 month", timedelta(days=30)),
        ("dense 1 week", timedelta(days=7)), ("dense 1 day", timedelta(days=1)),
        ("dense 6 h", timedelta(hours=6)), ("dense 1 h", timedelta(hours=1)),
        ("dense 10 min", timedelta(minutes=10)), ("dense 1 min", timedelta(minutes=1)),
    ]:
        if delta is None:
            add(label, dense)
        else:
            add(label, dense, dt0=d_mid, dt1=d_mid + delta)
    add("dense 1 day limit 1000", dense, dt0=d_mid, dt1=d_mid + timedelta(days=1), limit=1000)
    add("dense full limit 1000", dense, limit=1000)
    add("dense latest 100 desc", dense, limit=100, order="desc")
    d_q = anchor(dense, 0.25)
    add("dense 2 days coalesce", dense, dt0=d_q, dt1=d_q + timedelta(days=2), mode="coalesce")

    m_mid = anchor(mid, 0.5)
    add("mid full range", mid)
    add("mid 1 day", mid, dt0=m_mid, dt1=m_mid + timedelta(days=1))
    add("mid 1 h", mid, dt0=m_mid, dt1=m_mid + timedelta(hours=1))
    m_th = anchor(mid, 1 / 3)
    add("mid 12 h numeric mode", mid, dt0=m_th, dt1=m_th + timedelta(hours=12), mode="numeric")

    add("sparse full range", sparse)
    t_mid = anchor(dense_text, 0.5)
    add("text full range", dense_text)
    add("text 1 day", dense_text, dt0=t_mid, dt1=t_mid + timedelta(days=1))
    # the deliberately-empty one: a range that ends before the data starts
    d_mn = utc(dense[4])
    add("EMPTY before data", dense, dt0=d_mn - timedelta(days=60), dt1=d_mn - timedelta(days=30))

    assert len(specs) == 20, len(specs)
    return specs


def run_spec(store: BenchDuckDBStore, spec: dict[str, Any]) -> int:
    rows = 0
    for batch in timeseries_replica(
        store, spec["ref_uri"],
        start=None if spec["start"] is None else datetime.fromisoformat(spec["start"]),
        end=None if spec["end"] is None else datetime.fromisoformat(spec["end"]),
        limit=spec["limit"], order=spec["order"], value_mode=spec["value_mode"],
    ):
        rows += batch.num_rows
    return rows


# ---- main ------------------------------------------------------------------

def main() -> int:
    def _sigterm(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _sigterm)

    out_dir = HERE / "results" / "query-bench" / time.strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}

    if "--reuse-db" in sys.argv:
        stores, build_info = open_existing_stores()
    else:
        stores, build_info = build_stores()
    results["build"] = build_info

    try:
        # ---- phase 2: timeseries_info, every ref_uri, REPS times ----------
        with stores["noidx"]._lock:
            uris = [r[0] for r in stores["noidx"]._conn.execute(
                "SELECT ref_uri FROM streams ORDER BY ref_uri").fetchall()]
        log(f"timeseries_info: {len(uris)} ref_uris x {INFO_REPS} reps x {len(stores)} stores")
        info_res: dict[str, Any] = {}
        for name, store in stores.items():
            t_cfg = time.perf_counter()
            per_uri: list[dict[str, Any]] = []
            all_lat: list[float] = []
            for uri in uris:
                lats = []
                rows = 0
                for _ in range(INFO_REPS):
                    t0 = time.perf_counter()
                    rows = timeseries_info_replica(store, uri)[0]
                    lats.append(time.perf_counter() - t0)
                all_lat.extend(lats)
                per_uri.append({"ref_uri": uri, "rows": rows,
                                "mean_ms": round(statistics.mean(lats) * 1e3, 3),
                                "min_ms": round(min(lats) * 1e3, 3),
                                "max_ms": round(max(lats) * 1e3, 3)})
            all_lat.sort()
            info_res[name] = {
                "calls": len(all_lat),
                "mean_ms": round(statistics.mean(all_lat) * 1e3, 3),
                "p50_ms": round(all_lat[len(all_lat) // 2] * 1e3, 3),
                "p95_ms": round(all_lat[int(len(all_lat) * 0.95)] * 1e3, 3),
                "max_ms": round(all_lat[-1] * 1e3, 3),
                "total_s": round(sum(all_lat), 1),
                "per_uri": per_uri,
            }
            log(f"timeseries_info[{name}]: mean {info_res[name]['mean_ms']}ms "
                f"p95 {info_res[name]['p95_ms']}ms total {info_res[name]['total_s']}s "
                f"({time.perf_counter() - t_cfg:.0f}s wall)")
            with open(out_dir / "results.json", "w") as f:
                json.dump({**results, "timeseries_info": info_res}, f, indent=1)
        results["timeseries_info"] = info_res

        # ---- phase 3: timeseries() over 20 specs --------------------------
        specs = make_query_specs(stores["tsref"])
        # validate: every spec but the EMPTY one must return rows
        for spec in specs:
            n = run_spec(stores["tsref"], spec)
            spec["expected_rows"] = n
            if spec["label"].startswith("EMPTY"):
                assert n == 0, f"{spec['label']} unexpectedly returned {n}"
            else:
                assert n > 0, f"{spec['label']} returned nothing"
        log("specs validated: " + ", ".join(f"{s['label']}={s['expected_rows']}" for s in specs))
        results["specs"] = specs

        ts_res: dict[str, Any] = {}
        for name, store in stores.items():
            t_cfg = time.perf_counter()
            per_q = []
            for spec in specs:
                lats = []
                rows = 0
                for _ in range(TS_REPS):
                    t0 = time.perf_counter()
                    rows = run_spec(store, spec)
                    lats.append(time.perf_counter() - t0)
                lats.sort()
                per_q.append({"label": spec["label"], "rows": rows,
                              "mean_ms": round(statistics.mean(lats) * 1e3, 2),
                              "p50_ms": round(lats[len(lats) // 2] * 1e3, 2),
                              "p95_ms": round(lats[int(len(lats) * 0.95)] * 1e3, 2),
                              "min_ms": round(lats[0] * 1e3, 2),
                              "max_ms": round(lats[-1] * 1e3, 2)})
                log(f"timeseries[{name}] {spec['label']}: p50 {per_q[-1]['p50_ms']}ms "
                    f"mean {per_q[-1]['mean_ms']}ms rows={rows}")
            ts_res[name] = {"per_query": per_q,
                            "wall_s": round(time.perf_counter() - t_cfg, 1)}
            with open(out_dir / "results.json", "w") as f:
                json.dump({**results, "timeseries": ts_res}, f, indent=1)
        results["timeseries"] = ts_res
    except KeyboardInterrupt:
        log("interrupted — partial results saved")
        results["aborted"] = True
    finally:
        for store in stores.values():
            store.close()
        with open(out_dir / "results.json", "w") as f:
            json.dump(results, f, indent=1)
        log(f"results: {out_dir / 'results.json'}")
        log("DBs kept in tmp_db/query_bench_*.duckdb")
    return 0


if __name__ == "__main__":
    sys.exit(main())
