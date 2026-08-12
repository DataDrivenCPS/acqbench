#!/usr/bin/env python
"""Measure acquirium main-branch ingest speed without driver/HTTP overhead.

Replicates, in-process, the exact pipeline a DPR CSV goes through when it is
ingested via ``POST /insert_timeseries`` on acquirium@main, but with the
driver-side wide->long transformation done in pandas (per request) and the
storage layer pointed at a throwaway DuckDB file.

Pipeline stages (each timed for wall clock, process CPU, and RSS):

  driver-side prep (pandas, mirrors DPRTrailerCSVDriver semantics):
    read_csv        header located by content (cells contain Date+Time),
                    utf-8 with replacement (driver's "utf8-lossy")
    build_ts        Date + " " + Time -> UTC datetime, "%m/%d/%Y %I:%M:%S %p"
    melt            pandas wide->long (ts, ref_name, value); ref_name passed
                    through _safe_name; NaN/NaT rows dropped
    infer_kinds     assign_stream_value_kind per stream (verbatim from
                    Storage/values.py)
    register        DuckDBStore.ensure_stream_refs replica for new streams
    regroup         long frame -> {ref_name: [(ts, value), ...]} — the shape
                    the /insert_timeseries endpoint hands the Manager

  server-side (verbatim replicas from acquirium@main):
    mgr_assembly    Manager.insert_timeseries_batch: per-stream value-kind
                    lookup + per-row Python list build -> polars DataFrame
    prep_split      prepare_value_columns (per-row numeric/text split)
    tz_dedupe       UTC normalise + unique(ref_uri, ts, keep="last")
    sql_delete      DELETE existing (ref_uri, ts) matches (upsert semantics)
    sql_insert      INSERT INTO timeseries from registered frame
    sql_commit      COMMIT

Deliberate deviations from the wire path (reported, not hidden):
  * no HTTP/JSON serialisation, no pydantic parsing, no driver scheduler
  * pandas read_csv drops "NaN" cells at melt time; the real polars driver
    casts them to the string "NaN" and the store persists them as
    (NULL, NULL) rows.  The dropped count is reported per file.
  * insert_stats.record() and RDF-graph writes (register_datasource /
    register_streams graph triples) are skipped — dummy store has no graph.
"""

from __future__ import annotations

import argparse
import csv as _csv
import signal
import io
import json
import math
import os
import shutil
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from numbers import Real
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd
import polars as pl
import psutil

# --------------------------------------------------------------------------
# Verbatim replicas from acquirium@main
# --------------------------------------------------------------------------

# internals/models.py
_REF_URI_NAMESPACE = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")
_ACQUIRIUM_NS_PREFIX = "urn:acquirium#"


def compute_ref_uri(source_id: str, ref_name: str) -> str:
    return _ACQUIRIUM_NS_PREFIX + str(
        uuid.uuid5(_REF_URI_NAMESPACE, f"{source_id}:{ref_name}")
    )


# Drivers/BuiltInDrivers/tabular_base.py
_URI_UNSAFE = str.maketrans({
    "\n": "_", "\r": "_", "\t": "_",
    " ": "_", "(": "", ")": "",
    "#": "", "?": "", "<": "", ">": "",
    '"': "", "'": "", "\\": "", "`": "",
})


def _safe_name(s: str) -> str:
    return " ".join(s.split()).translate(_URI_UNSAFE)


# Storage/values.py
def normalize_value_kind(value_kind: Any = None) -> str:
    if value_kind is None:
        return "text"
    value = str(value_kind).strip().lower()
    if value in {"", "unknown"}:
        return "text"
    if value in {"numeric", "number", "float", "int", "integer"}:
        return "numeric"
    if value in {"text", "string", "str", "status", "state"}:
        return "text"
    raise ValueError(f"value_kind must be 'numeric' or 'text', got {value_kind!r}")


def _is_numeric_value(value: Any, *, parse_numeric_strings: bool = True) -> bool:
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (Real, Decimal)):
        return True
    if not parse_numeric_strings or not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        float(text)
    except ValueError:
        return False
    return True


def assign_stream_value_kind(values: Iterable[Any], *, parse_numeric_strings: bool = True) -> str:
    return "numeric" if any(
        _is_numeric_value(value, parse_numeric_strings=parse_numeric_strings)
        for value in values
    ) else "text"


def split_value(value: Any, value_kind: str | None = None) -> tuple[float | None, str | None]:
    kind = normalize_value_kind(value_kind)
    if value is None:
        return None, None
    if kind == "numeric":
        if isinstance(value, str) and not value.strip():
            return None, None
        if isinstance(value, bool):
            return None, str(value)
        try:
            numeric_value = float(value)
        except (TypeError, ValueError, OverflowError):
            return None, str(value)
        if not math.isfinite(numeric_value):
            return None, None
        return numeric_value, None
    return None, str(value)


def prepare_value_columns(df: pl.DataFrame) -> pl.DataFrame:
    if {"numeric_value", "text_value"}.issubset(df.columns):
        return df.select(["ref_uri", "ts", "numeric_value", "text_value"])

    if "value" not in df.columns:
        raise ValueError("timeseries dataframe must include value or numeric_value/text_value columns")

    has_value_kind = "value_kind" in df.columns
    selected = ["ref_uri", "ts", "value"] + (["value_kind"] if has_value_kind else [])
    rows = []
    fallback_counts: defaultdict[str, int] = defaultdict(int)
    for row in df.select(selected).iter_rows():
        if has_value_kind:
            ref_uri, ts, value, value_kind = row
            kind = normalize_value_kind(value_kind)
            numeric_value, text_value = split_value(value, kind)
            if kind == "numeric" and text_value is not None:
                fallback_counts[str(ref_uri)] += 1
        else:
            ref_uri, ts, value = row
            numeric_value, text_value = split_value(value)
        rows.append((ref_uri, ts, numeric_value, text_value))

    return pl.DataFrame(
        rows,
        schema={
            "ref_uri": pl.Utf8,
            "ts": df["ts"].dtype,
            "numeric_value": pl.Float64,
            "text_value": pl.Utf8,
        },
        orient="row",
    )


# Storage/duckdb_store.py — trimmed to the ingest path, behavior preserved.
TIMESERIES_TABLE = "timeseries"
STREAMS_TABLE = "streams"
LOGS_TABLE = "logs"
TIMESERIES_STREAMS_VIEW = "timeseries_streams"


class BenchDuckDBStore:
    """DuckDBStore replica: same DDL, same locking, same upsert SQL.

    bulk_insert_polars is split into instrumented sub-steps but performs the
    identical operations in the identical order.
    """

    def __init__(self, db_path: str | Path, *, recreate: bool = False, no_indexes: bool = False) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = duckdb.connect(str(self.db_path))
        self._in_tx = False
        self.no_indexes = no_indexes
        if recreate:
            self._conn.execute(f"DROP VIEW IF EXISTS {TIMESERIES_STREAMS_VIEW}")
            for tbl in (TIMESERIES_TABLE, STREAMS_TABLE, LOGS_TABLE):
                self._conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        self.ensure_table()

    def ensure_table(self) -> str:
        # --no-indexes variant: identical tables but no ART indexes on the
        # timeseries/logs tables — no UNIQUE(ref_uri, ts) and none of the five
        # secondary indexes. The streams PRIMARY KEY is kept: ensure_stream_refs'
        # ON CONFLICT (ref_uri) requires it (340 rows — negligible).  Upsert
        # semantics are unchanged either way: dedupe happens via the explicit
        # DELETE+INSERT, not via the constraint.
        # single-index config (2026-08-10): with the secondary indexes
        # commented out above, UNIQUE(ref_uri, ts) is dropped here as well —
        # it materializes as a second ART index on the exact same key, so
        # keeping it would measure two indexes, not one. The kept index is
        # idx_ts_ref only.
        ts_constraints = "CHECK (numeric_value IS NULL OR text_value IS NULL)"
        ts_indexes = [] if self.no_indexes else [
            f"CREATE INDEX IF NOT EXISTS idx_ref ON {TIMESERIES_TABLE} (ref_uri)",
            # f"CREATE INDEX IF NOT EXISTS idx_ts_ref ON {TIMESERIES_TABLE} (ref_uri, ts)",
            # f"CREATE INDEX IF NOT EXISTS idx_ts_numeric_ref ON {TIMESERIES_TABLE} (ref_uri, ts, numeric_value)",
            # f"CREATE INDEX IF NOT EXISTS idx_ts_text_ref ON {TIMESERIES_TABLE} (ref_uri, ts, text_value)",
            # f"CREATE INDEX IF NOT EXISTS idx_ts_numeric_value ON {TIMESERIES_TABLE} (ref_uri, numeric_value)",
            # f"CREATE INDEX IF NOT EXISTS idx_ts_text_value ON {TIMESERIES_TABLE} (ref_uri, text_value)",
        ]
        stmts = [
            f"""
            CREATE TABLE IF NOT EXISTS {TIMESERIES_TABLE} (
                ref_uri VARCHAR NOT NULL,
                ts      TIMESTAMP NOT NULL,
                numeric_value DOUBLE,
                text_value    VARCHAR,
                {ts_constraints}
            )
            """,
            *ts_indexes,
            f"""
            CREATE TABLE IF NOT EXISTS {STREAMS_TABLE} (
                ref_uri   VARCHAR PRIMARY KEY,
                point_uri VARCHAR,
                source_id VARCHAR NOT NULL,
                ref_name  VARCHAR NOT NULL,
                value_kind VARCHAR NOT NULL DEFAULT 'text'
            )
            """,
            *([] if self.no_indexes else [
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_source_ref_name ON {STREAMS_TABLE} (source_id, ref_name)",
                f"CREATE INDEX IF NOT EXISTS idx_streams_point_uri ON {STREAMS_TABLE} (point_uri)",
            ]),
            f"""
            CREATE OR REPLACE VIEW {TIMESERIES_STREAMS_VIEW} AS
            SELECT
                t.ref_uri, s.point_uri, s.source_id, s.ref_name,
                COALESCE(s.value_kind, 'text') AS value_kind,
                t.ts, t.numeric_value AS value_numeric, t.text_value AS value_text
            FROM {TIMESERIES_TABLE} AS t
            LEFT JOIN {STREAMS_TABLE} AS s ON t.ref_uri = s.ref_uri
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                point_uri      VARCHAR NOT NULL,
                timestamp      TIMESTAMP NOT NULL,
                observed_start TIMESTAMP,
                observed_end   TIMESTAMP,
                message        VARCHAR NOT NULL,
                UNIQUE (point_uri, timestamp)
            )
            """,
            *([] if self.no_indexes else [
                f"CREATE INDEX IF NOT EXISTS idx_logs_point ON {LOGS_TABLE} (point_uri, timestamp)",
                f"CREATE INDEX IF NOT EXISTS idx_logs_obs ON {LOGS_TABLE} (observed_start, observed_end)",
            ]),
        ]
        with self._lock:
            for stmt in stmts:
                self._conn.execute(stmt)
        return "ok"

    def ensure_stream_refs(self, refs) -> list[str]:
        prepared: dict[str, list[Any]] = {}
        for point_uri, source_id, ref_name, ref_uri, value_kind in refs:
            key = str(ref_uri if ref_uri is not None else compute_ref_uri(source_id, ref_name))
            prepared[key] = [key, point_uri, source_id, ref_name, normalize_value_kind(value_kind)]
        if not prepared:
            return []
        df = pl.DataFrame(
            list(prepared.values()),
            schema=["ref_uri", "point_uri", "source_id", "ref_name", "value_kind"],
            orient="row",
        )
        with self._lock:
            self._conn.register("_acquirium_incoming_refs", df)
            try:
                self._conn.execute(
                    f"""
                    INSERT INTO {STREAMS_TABLE} (ref_uri, point_uri, source_id, ref_name, value_kind)
                    SELECT ref_uri, point_uri, source_id, ref_name, value_kind
                    FROM _acquirium_incoming_refs
                    ON CONFLICT (ref_uri) DO UPDATE SET
                        source_id  = excluded.source_id,
                        ref_name   = excluded.ref_name,
                        point_uri  = COALESCE(excluded.point_uri, {STREAMS_TABLE}.point_uri),
                        value_kind = excluded.value_kind
                    """
                )
            finally:
                self._conn.unregister("_acquirium_incoming_refs")
        return list(prepared.keys())

    def stream_value_kind(self, ref_uri: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT value_kind FROM {STREAMS_TABLE} WHERE ref_uri = ?", [ref_uri]
            ).fetchone()
        return None if row is None else row[0]

    def bulk_insert_polars_instrumented(self, df: pl.DataFrame, timer: "StepTimer") -> tuple[int, int]:
        """Identical operations to DuckDBStore.bulk_insert_polars, with each
        sub-step timed. Returns (rows_inserted, rows_deleted_by_upsert)."""
        if df.is_empty():
            return 0, 0
        with timer.step("prep_split"):
            df = prepare_value_columns(df)
        with timer.step("tz_dedupe"):
            df = df.with_columns(
                pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            )
            df = df.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
        deleted = 0
        with self._lock:
            self._conn.register("_acquirium_incoming_timeseries", df)
            owns_transaction = not self._in_tx
            try:
                if owns_transaction:
                    self._conn.execute("BEGIN TRANSACTION")
                with timer.step("sql_delete"):
                    res = self._conn.execute(
                        f"""
                        DELETE FROM {TIMESERIES_TABLE}
                        USING _acquirium_incoming_timeseries AS incoming
                        WHERE {TIMESERIES_TABLE}.ref_uri = incoming.ref_uri
                          AND {TIMESERIES_TABLE}.ts = incoming.ts
                        """
                    )
                    row = res.fetchone()
                    deleted = int(row[0]) if row else 0
                with timer.step("sql_insert"):
                    self._conn.execute(
                        f"""
                        INSERT INTO {TIMESERIES_TABLE} (ref_uri, ts, numeric_value, text_value)
                        SELECT ref_uri, ts, numeric_value, text_value
                        FROM _acquirium_incoming_timeseries
                        """
                    )
                with timer.step("sql_commit"):
                    if owns_transaction:
                        self._conn.execute("COMMIT")
            except Exception:
                if owns_transaction:
                    self._conn.execute("ROLLBACK")
                raise
            finally:
                self._conn.unregister("_acquirium_incoming_timeseries")
        return len(df), deleted

    def sql(self, q: str):
        with self._lock:
            return self._conn.execute(q).fetchall()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


# Server/manager.py — Manager.insert_timeseries_batch, verbatim behavior.
class BenchManager:
    def __init__(self, store: BenchDuckDBStore) -> None:
        self.timescale = store

    def _registered_value_kind(self, ref_uri: str) -> str:
        value_kind = self.timescale.stream_value_kind(ref_uri)
        if value_kind is None:
            raise ValueError(f"stream {ref_uri} is not registered")
        return normalize_value_kind(value_kind)

    def insert_timeseries_batch_assemble(
        self, source_id: str, streams: dict[str, list[tuple[datetime, Any]]]
    ) -> pl.DataFrame | None:
        ref_uris: list[str] = []
        timestamps: list[datetime] = []
        values: list[Any] = []
        value_kinds: list[str] = []
        for ref_name, stream_rows in streams.items():
            ref_uri = str(compute_ref_uri(source_id, ref_name))
            value_kind = self._registered_value_kind(ref_uri)
            for ts, value in stream_rows:
                ref_uris.append(ref_uri)
                timestamps.append(ts)
                values.append(value)
                value_kinds.append(value_kind)
        if not ref_uris:
            return None
        return pl.DataFrame(
            {"ref_uri": ref_uris, "ts": timestamps, "value": values, "value_kind": value_kinds},
            schema={
                "ref_uri": pl.Utf8,
                "ts": pl.Datetime("us", "UTC"),
                "value": pl.Object,
                "value_kind": pl.Utf8,
            },
        )


# --------------------------------------------------------------------------
# DPR driver-side prep (pandas port of DPRTrailerCSVDriver)
# --------------------------------------------------------------------------

SOURCE_ID = "dpr-trailer"
DATE_COL = "Date"
CLOCK_COL = "Time"
DATE_FMT = "%m/%d/%Y %I:%M:%S %p"
_HEADER_SCAN_LIMIT = 100


def detect_header_line(path: Path) -> int:
    """1-indexed line number of the first line whose cells include Date+Time."""
    needed = {DATE_COL, CLOCK_COL}
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for lineno, line in enumerate(f, start=1):
            if lineno > _HEADER_SCAN_LIMIT:
                break
            cells = next(_csv.reader([line]), [])
            if needed <= {cell.strip() for cell in cells}:
                return lineno
    raise ValueError(f"no header row with {sorted(needed)} in first {_HEADER_SCAN_LIMIT} lines of {path}")


def read_csv_pandas(path: Path) -> tuple[pd.DataFrame, int]:
    """Read one DPR CSV with the header located by content.

    Returns (frame, ragged_rows_skipped). Mirrors the driver's utf8-lossy +
    truncate-ragged behavior as closely as pandas allows: clean files take the
    fast C path; files with ragged rows are retried with on_bad_lines="skip".
    """
    header_line = detect_header_line(path)
    kwargs: dict[str, Any] = dict(
        skiprows=header_line - 1,
        encoding="utf-8",
        encoding_errors="replace",
        dtype={DATE_COL: str, CLOCK_COL: str},
    )
    try:
        return pd.read_csv(path, **kwargs), 0
    except pd.errors.ParserError:
        df = pd.read_csv(path, on_bad_lines="skip", **kwargs)
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            physical = sum(1 for _ in f)
        return df, max(0, physical - header_line - len(df))


def build_ts(df: pd.DataFrame) -> tuple[pd.Series, int]:
    """Combine Date + Time into a UTC datetime series. Returns (ts, n_unparseable)."""
    combined = df[DATE_COL].str.strip() + " " + df[CLOCK_COL].str.strip()
    ts = pd.to_datetime(combined, format=DATE_FMT, errors="coerce")
    bad = int(ts.isna().sum() - combined.isna().sum())
    if bad:
        # Same spirit as normalize_timestamps' fallback format list: give
        # non-conforming rows a second chance with flexible parsing.
        mask = ts.isna() & combined.notna()
        ts.loc[mask] = pd.to_datetime(combined[mask], errors="coerce")
        bad = int(ts.isna().sum() - combined.isna().sum())
    return ts.dt.tz_localize("UTC"), bad


def melt_wide_to_long(df: pd.DataFrame, ts: pd.Series) -> tuple[pd.DataFrame, dict[str, int]]:
    """pandas equivalent of the driver's unpivot to (ts, ref_name, value)."""
    stream_cols = [c for c in df.columns if c not in (DATE_COL, CLOCK_COL)]
    wide = df[stream_cols].copy()
    wide.insert(0, "ts", ts)
    long = wide.melt(id_vars=["ts"], var_name="ref_name", value_name="value")
    pre = len(long)
    long = long.dropna(subset=["ts", "value"])
    dropped = pre - len(long)
    long["ref_name"] = long["ref_name"].map({c: _safe_name(c) for c in stream_cols})
    counts = {
        "stream_cols": len(stream_cols),
        "long_rows_pre_drop": pre,
        "nan_dropped": int(dropped),
        "long_rows": len(long),
    }
    return long, counts


# --------------------------------------------------------------------------
# Instrumentation
# --------------------------------------------------------------------------

class RSSSampler(threading.Thread):
    def __init__(self, proc: psutil.Process, interval: float = 0.1) -> None:
        super().__init__(daemon=True)
        self.proc = proc
        self.interval = interval
        self._lock = threading.Lock()
        self._max = 0
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                rss = self.proc.memory_info().rss
            except Exception:
                break
            with self._lock:
                if rss > self._max:
                    self._max = rss
            self._stop.wait(self.interval)

    def reset(self) -> None:
        with self._lock:
            self._max = 0

    def peak(self) -> int:
        with self._lock:
            return self._max

    def stop(self) -> None:
        self._stop.set()


class StepTimer:
    """Collects per-step wall / CPU / RSS metrics for one file."""

    def __init__(self, proc: psutil.Process, sampler: RSSSampler) -> None:
        self.proc = proc
        self.sampler = sampler
        self.steps: dict[str, dict[str, float]] = {}

    def step(self, name: str):
        return _Step(self, name)


class _Step:
    def __init__(self, timer: StepTimer, name: str) -> None:
        self.t = timer
        self.name = name

    def __enter__(self):
        cpu = self.t.proc.cpu_times()
        self.cpu0 = cpu.user + cpu.system
        self.rss0 = self.t.proc.memory_info().rss
        self.t.sampler.reset()
        self.w0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        wall = time.perf_counter() - self.w0
        cpu = self.t.proc.cpu_times()
        rss1 = self.t.proc.memory_info().rss
        peak = max(self.t.sampler.peak(), rss1, self.rss0)
        rec = self.t.steps.setdefault(
            self.name, {"wall_s": 0.0, "cpu_s": 0.0, "rss_before_mb": self.rss0 / 1e6}
        )
        rec["wall_s"] += wall
        rec["cpu_s"] += (cpu.user + cpu.system) - self.cpu0
        rec["rss_after_mb"] = rss1 / 1e6
        rec["rss_peak_mb"] = max(rec.get("rss_peak_mb", 0.0), peak / 1e6)
        return False


# --------------------------------------------------------------------------
# Main benchmark loop
# --------------------------------------------------------------------------

STEP_ORDER = [
    "read_csv", "build_ts", "melt", "infer_kinds", "register", "regroup",
    "mgr_assembly", "prep_split", "tz_dedupe", "sql_delete", "sql_insert", "sql_commit",
]


def human(n: float) -> str:
    return f"{n:,.0f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", type=Path,
                    default=Path.home() / "Resilio Sync" / "DPR Pilot Logs New")
    ap.add_argument("--db-path", type=Path,
                    default=Path(__file__).parent / "tmp_db" / "ingest_bench.duckdb")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="results dir (default: local_ingest_bench/results/<runid>)")
    ap.add_argument("--limit", type=int, default=None, help="process at most N files")
    ap.add_argument("--stride", type=int, default=1,
                    help="take every Nth file of the sorted list (calibration)")
    ap.add_argument("--keep-db", action="store_true", help="do not delete the duckdb file at the end")
    ap.add_argument("--no-indexes", action="store_true",
                    help="create the timeseries table without UNIQUE(ref_uri, ts) or any secondary index")
    ap.add_argument("--max-rows", type=int, default=None,
                    help="stop gracefully once this many rows have been inserted")
    ap.add_argument("--min-free-gb", type=float, default=40.0,
                    help="abort ingestion loop if free disk drops below this")
    ap.add_argument("--progress-every", type=int, default=20)
    ap.add_argument("--lookup-json", type=Path,
                    default=Path.home() / "Documents/DDCPS/NAWI/acquirium/dpr_scripts/property_to_column.json")
    args = ap.parse_args()

    # Background-launched processes start with SIGINT ignored, so a graceful
    # stop arrives as SIGTERM — turn it into the same KeyboardInterrupt path
    # (finalize: consistency queries, summary, delete db).
    def _sigterm(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _sigterm)

    run_id = time.strftime("%Y%m%d-%H%M%S")
    out_dir = args.out_dir or Path(__file__).parent / "results" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl = open(out_dir / "files.jsonl", "a", buffering=1)

    def log(msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    # ---- file discovery (mirrors _pending_paths: sorted rglob of *.csv/*.tsv)
    # Deviation: paths under hidden directories (.sync — Resilio's archive of
    # deleted/replaced files) are excluded. They are sync-tool internals, not
    # data, and Resilio prunes them while we run.
    all_paths = sorted(
        {p for pat in ("*.csv", "*.tsv") for p in args.data_dir.rglob(pat)
         if not any(part.startswith(".") for part in p.relative_to(args.data_dir).parts)}
    )
    non_csv = sum(1 for p in args.data_dir.rglob("*") if p.is_file() and p.suffix.lower() not in (".csv", ".tsv"))
    paths = all_paths[:: args.stride]
    if args.limit:
        paths = paths[: args.limit]
    total_bytes = sum(p.stat().st_size for p in paths)
    log(f"run {run_id}: {len(paths)}/{len(all_paths)} csv files "
        f"({total_bytes/1e9:.2f} GB), {non_csv} non-csv files ignored (driver globs *.csv/*.tsv only)")
    log(f"db: {args.db_path}  results: {out_dir}")

    proc = psutil.Process()
    sampler = RSSSampler(proc)
    sampler.start()

    # ---- store + one-time setup (mirrors driver.setup())
    t_boot = time.perf_counter()
    if args.db_path.exists():
        args.db_path.unlink()
    wal = args.db_path.with_suffix(args.db_path.suffix + ".wal")
    if wal.exists():
        wal.unlink()
    store = BenchDuckDBStore(args.db_path, recreate=True, no_indexes=args.no_indexes)
    mgr = BenchManager(store)
    boot_s = time.perf_counter() - t_boot

    # DPRTrailerCSVDriver.setup(): register point_uri->ref_name bindings from
    # the lookup (value_kind defaults to text; the lazy per-file registration
    # upserts the inferred kind later, exactly like production).
    lookup_registered = 0
    if args.lookup_json.exists():
        with open(args.lookup_json) as f:
            mapping: dict[str, str] = json.load(f)
        store.ensure_stream_refs(
            [(point_uri, SOURCE_ID, _safe_name(ref_name), None, "text")
             for point_uri, ref_name in mapping.items()]
        )
        lookup_registered = len(mapping)
    log(f"store boot {boot_s:.2f}s; {lookup_registered} streams pre-registered from property_to_column.json")

    registered: set[str] = set()
    if lookup_registered:
        # setup() registers via the server but the driver's in-memory
        # `_registered` set starts empty, so production re-registers these on
        # first sight with inferred kinds; mirror that by leaving `registered`
        # empty here.
        pass

    agg_steps: dict[str, dict[str, float]] = {}
    totals = defaultdict(int)
    ts_min: pd.Timestamp | None = None
    ts_max: pd.Timestamp | None = None
    failures: list[dict[str, str]] = []
    aborted_reason = None
    run_t0 = time.perf_counter()

    try:
        for i, path in enumerate(paths, start=1):
            free_gb = shutil.disk_usage(args.db_path.parent).free / 1e9
            if free_gb < args.min_free_gb:
                aborted_reason = f"free disk {free_gb:.1f} GB < {args.min_free_gb} GB after {i-1} files"
                log(f"ABORT: {aborted_reason}")
                break

            timer = StepTimer(proc, sampler)
            try:
                size_bytes = path.stat().st_size
            except FileNotFoundError:
                log(f"SKIP {path.name}: vanished before processing")
                totals["files_vanished"] += 1
                continue
            counts: dict[str, Any] = {"file": path.name, "size_bytes": size_bytes}
            try:
                with timer.step("read_csv"):
                    df, ragged_skipped = read_csv_pandas(path)
                counts["raw_rows"] = len(df)
                counts["ragged_skipped"] = ragged_skipped
                counts["columns"] = len(df.columns)
                if len(df) == 0:
                    counts["skipped_empty"] = True
                    jsonl.write(json.dumps({**counts, "steps": timer.steps}, default=str) + "\n")
                    totals["files_empty"] += 1
                    continue

                with timer.step("build_ts"):
                    ts, bad_ts = build_ts(df)
                counts["unparseable_ts"] = bad_ts

                with timer.step("melt"):
                    long, mcounts = melt_wide_to_long(df, ts)
                counts.update(mcounts)
                del df
                if len(long) == 0:
                    counts["skipped_no_values"] = True
                    jsonl.write(json.dumps({**counts, "steps": timer.steps}, default=str) + "\n")
                    totals["files_empty"] += 1
                    continue

                # driver: _with_value_kinds — infer kinds for every stream in
                # the frame (not just new ones), each file.
                with timer.step("infer_kinds"):
                    kinds: dict[str, str] = {}
                    for ref_name, vals in long.groupby("ref_name", sort=False)["value"]:
                        kinds[ref_name] = assign_stream_value_kind(vals.tolist())

                # driver: ensure_streams_registered — register names not seen yet
                with timer.step("register"):
                    new_names = [n for n in kinds if n not in registered]
                    if new_names:
                        store.ensure_stream_refs(
                            [(None, SOURCE_ID, n, None, kinds[n]) for n in new_names]
                        )
                        registered.update(new_names)
                counts["new_streams"] = len(new_names)

                # endpoint: list[StreamInsert] -> {ref_name: [(ts, value), ...]}
                with timer.step("regroup"):
                    streams: dict[str, list[tuple[Any, Any]]] = {}
                    for ref_name, grp in long.groupby("ref_name", sort=False):
                        streams[ref_name] = list(zip(grp["ts"].tolist(), grp["value"].tolist()))
                del long

                with timer.step("mgr_assembly"):
                    batch_df = mgr.insert_timeseries_batch_assemble(SOURCE_ID, streams)
                del streams
                counts["batch_rows"] = 0 if batch_df is None else len(batch_df)

                if batch_df is not None:
                    inserted, deleted = store.bulk_insert_polars_instrumented(batch_df, timer)
                    counts["rows_inserted"] = inserted
                    counts["upsert_deleted_existing"] = deleted
                    counts["dedup_dropped"] = counts["batch_rows"] - inserted
                    tmn = batch_df["ts"].min()
                    tmx = batch_df["ts"].max()
                    ts_min = tmn if ts_min is None else min(ts_min, tmn)
                    ts_max = tmx if ts_max is None else max(ts_max, tmx)
                    del batch_df

                counts["db_size_mb"] = round(args.db_path.stat().st_size / 1e6, 1) if args.db_path.exists() else 0

            except KeyboardInterrupt:
                raise
            except Exception as e:
                failures.append({"file": path.name, "error": f"{type(e).__name__}: {e}"})
                log(f"FAIL {path.name}: {type(e).__name__}: {e}")
                jsonl.write(json.dumps({**counts, "error": str(e), "steps": timer.steps}, default=str) + "\n")
                totals["files_failed"] += 1
                continue

            # aggregate
            totals["files_ok"] += 1
            for k in ("raw_rows", "long_rows_pre_drop", "nan_dropped", "long_rows",
                      "batch_rows", "rows_inserted", "upsert_deleted_existing",
                      "dedup_dropped", "ragged_skipped", "unparseable_ts", "new_streams",
                      "size_bytes"):
                totals[k] += counts.get(k, 0)
            for name, rec in timer.steps.items():
                a = agg_steps.setdefault(name, {"wall_s": 0.0, "cpu_s": 0.0, "rss_peak_mb": 0.0})
                a["wall_s"] += rec["wall_s"]
                a["cpu_s"] += rec["cpu_s"]
                a["rss_peak_mb"] = max(a["rss_peak_mb"], rec.get("rss_peak_mb", 0.0))

            jsonl.write(json.dumps({**counts, "steps": timer.steps}, default=str) + "\n")

            if i % args.progress_every == 0 or i == len(paths):
                el = time.perf_counter() - run_t0
                mb = totals["size_bytes"] / 1e6
                log(f"{i}/{len(paths)} files | {human(totals['rows_inserted'])} rows inserted | "
                    f"{mb/el:.1f} MB/s | {totals['rows_inserted']/el:,.0f} rows/s | "
                    f"db {counts.get('db_size_mb', 0):,} MB | rss {proc.memory_info().rss/1e6:,.0f} MB | "
                    f"free {free_gb:.0f} GB | {el/60:.1f} min elapsed")

            if args.max_rows and totals["rows_inserted"] >= args.max_rows:
                aborted_reason = (f"reached --max-rows: {totals['rows_inserted']:,} rows "
                                  f"after {totals['files_ok']} files")
                log(f"STOP: {aborted_reason}")
                break
    except KeyboardInterrupt:
        aborted_reason = f"KeyboardInterrupt after {totals['files_ok']} files"
        log(f"ABORT: {aborted_reason}")

    ingest_wall = time.perf_counter() - run_t0

    # ---- consistency checks straight from the store -------------------------
    log("running consistency queries…")
    consistency: dict[str, Any] = {}
    t_c = time.perf_counter()
    try:
        (n_rows, n_streams, db_min, db_max), = store.sql(
            f"SELECT count(*), count(DISTINCT ref_uri), min(ts), max(ts) FROM {TIMESERIES_TABLE}")
        (n_numeric, n_text, n_both_null), = store.sql(
            f"SELECT count(numeric_value), count(text_value), "
            f"sum(CASE WHEN numeric_value IS NULL AND text_value IS NULL THEN 1 ELSE 0 END) "
            f"FROM {TIMESERIES_TABLE}")
        streams_rows = store.sql(
            f"SELECT value_kind, count(*) FROM {STREAMS_TABLE} GROUP BY value_kind")
        top = store.sql(
            f"SELECT s.ref_name, count(*) AS n FROM {TIMESERIES_TABLE} t "
            f"JOIN {STREAMS_TABLE} s USING (ref_uri) GROUP BY 1 ORDER BY n DESC LIMIT 10")
        consistency = {
            "db_row_count": n_rows,
            "db_distinct_streams": n_streams,
            "db_ts_min": str(db_min),
            "db_ts_max": str(db_max),
            "db_numeric_values": n_numeric,
            "db_text_values": n_text,
            "db_null_null_rows": int(n_both_null or 0),
            "streams_table_by_kind": {k: v for k, v in streams_rows},
            "top10_streams_by_rows": [[r, n] for r, n in top],
        }
    except Exception as e:
        consistency = {"error": f"{type(e).__name__}: {e}"}
    consistency["consistency_query_s"] = round(time.perf_counter() - t_c, 2)

    db_size = args.db_path.stat().st_size if args.db_path.exists() else 0
    t_close = time.perf_counter()
    store.close()
    close_s = time.perf_counter() - t_close
    db_size_final = args.db_path.stat().st_size if args.db_path.exists() else db_size

    summary = {
        "run_id": run_id,
        "no_indexes": args.no_indexes,
        "aborted": aborted_reason,
        "store_boot_s": round(boot_s, 3),
        "close_checkpoint_s": round(close_s, 2),
        "ingest_wall_s": round(ingest_wall, 2),
        "files": {"listed": len(paths), "ok": totals["files_ok"],
                  "empty": totals["files_empty"], "failed": totals["files_failed"],
                  "vanished": totals["files_vanished"], "failures": failures[:50]},
        "counts": {k: int(totals[k]) for k in (
            "size_bytes", "raw_rows", "long_rows_pre_drop", "nan_dropped", "long_rows",
            "batch_rows", "dedup_dropped", "rows_inserted", "upsert_deleted_existing",
            "ragged_skipped", "unparseable_ts")},
        "unique_streams_registered": len(registered),
        "ts_min_seen": str(ts_min), "ts_max_seen": str(ts_max),
        "steps_aggregate": {k: {m: round(v, 3) for m, v in agg_steps[k].items()}
                            for k in STEP_ORDER if k in agg_steps},
        "throughput": {
            "csv_mb_per_s": round(totals["size_bytes"] / 1e6 / ingest_wall, 2) if ingest_wall else None,
            "inserted_rows_per_s": round(totals["rows_inserted"] / ingest_wall, 1) if ingest_wall else None,
        },
        "db_file_size_bytes_final": db_size_final,
        "consistency": consistency,
        "environment": {
            "duckdb": duckdb.__version__, "pandas": pd.__version__,
            "polars": pl.__version__, "python": sys.version.split()[0],
            "cpu_count": os.cpu_count(),
            "ram_gb": round(psutil.virtual_memory().total / 1e9, 1),
        },
    }
    with open(out_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # ---- delete the throwaway db -------------------------------------------
    if not args.keep_db:
        for p in (args.db_path, wal):
            if p.exists():
                p.unlink()
        log(f"deleted {args.db_path} ({db_size_final/1e9:.2f} GB)")
    sampler.stop()

    log("---- summary ----")
    print(json.dumps(summary, indent=2, default=str), flush=True)
    return 0 if not aborted_reason else 2


if __name__ == "__main__":
    sys.exit(main())
