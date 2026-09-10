"""Replay acquirium's CSV -> polars -> melt -> Timescale ingest path stage by stage.

No acquirium server or HTTP: the exact same library functions the driver, client,
server route, manager and TimescaleStore call are invoked in the same order on the
same data, with a perf_counter around every stage.  The SQL that the store runs is
copied verbatim from timescale_store.py and asserted to be present in the source
of the checked-out branch, so the write path cannot silently diverge.

Evidence written under <out>/:
  files.jsonl        one record per CSV: every stage's wall time, rows, bytes, WAL
  activity.jsonl     pg_stat_activity wait-event samples (5 Hz) tagged with the stage
  sysstat.jsonl      postgres container cgroup cpu/memory + tracer rusage, every 5 s
  explain/NNNN.txt   EXPLAIN (ANALYZE, BUFFERS, WAL) of the merge on sampled files
  pgss_*.json        pg_stat_statements snapshots
  pgio_*.json        pg_stat_io / pg_stat_checkpointer / pg_stat_wal snapshots
  summary.json       totals, provenance, settings
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import resource
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import psycopg
import pyarrow as pa
import pyarrow.ipc as ipc

# ---- the code under test (imported from the clone of the public branch) ----
import acquirium
from acquirium.Drivers.BuiltInDrivers.csv_ingest import _header_line, _read_text
from acquirium.Drivers.Driver import IngestDriver, _cast_value_to_utf8
from acquirium.Drivers.tabular import to_observations, to_timestamp
from acquirium.internals.models import compute_ref_uri
from acquirium.Storage.timescale_store import (
    REF_IDS_TABLE, TIMESERIES_TABLE, TimescaleStore,
)
from acquirium.Storage.values import assign_stream_value_kind, normalize_value_kind

SOURCE_ID = "dpr-trailer"
DRIVER_CFG = dict(
    format="wide", date_col="Date", clock_col="Time",
    date_format="%m/%d/%Y %I:%M:%S %p", header_contains=["Date", "Time"],
    null_values=["NaN"], encoding="utf8-lossy", timezone="UTC",
)
STAGING = TimescaleStore.STAGING_TABLE

# ---- SQL copied verbatim from TimescaleStore._insert_frame (branch d6b6e1a) ----
SQL_DROP = f"DROP TABLE IF EXISTS {STAGING}"
SQL_CREATE = f"""
            CREATE TEMP TABLE {STAGING} (
                ref_uri TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                numeric_value DOUBLE PRECISION,
                text_value TEXT
            ) ON COMMIT DROP
            """
SQL_COPY = f"COPY {STAGING} (ref_uri, ts, numeric_value, text_value) FROM STDIN WITH (FORMAT csv)"
SQL_REF_IDS = f"""
            INSERT INTO {REF_IDS_TABLE} (ref_uri)
            SELECT DISTINCT i.ref_uri FROM {STAGING} AS i
            WHERE NOT EXISTS (
                SELECT 1 FROM {REF_IDS_TABLE} AS r WHERE r.ref_uri = i.ref_uri
            )
            ON CONFLICT (ref_uri) DO NOTHING
            """
SQL_MERGE = f"""
                INSERT INTO {TIMESERIES_TABLE} (ref_id, ts, numeric_value, text_value)
                SELECT r.ref_id, i.ts, i.numeric_value, i.text_value
                FROM {STAGING} AS i
                JOIN {REF_IDS_TABLE} AS r USING (ref_uri)
                ORDER BY r.ref_id, i.ts
                ON CONFLICT (ref_id, ts) DO UPDATE SET
                    numeric_value = EXCLUDED.numeric_value,
                    text_value = EXCLUDED.text_value
                """


# Variant "plain-insert": same statement without the ON CONFLICT clause, run against a
# NON-unique (ref_id, ts) index. Dedupe in Python stays, so the same rows are written.
SQL_MERGE_PLAIN = SQL_MERGE.split("ON CONFLICT")[0].rstrip() + "\n"
VARIANT_DDL = [
    "DROP INDEX idx_timeseries_ref_ts_unique",
    f"CREATE INDEX idx_timeseries_ref_ts ON {TIMESERIES_TABLE} (ref_id, ts)",
]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def fidelity_check() -> dict:
    """Refuse to run if the copied SQL is not literally what the branch's store runs."""
    src_dir = Path(acquirium.__file__).resolve().parent
    store_src = (src_dir / "Storage" / "timescale_store.py").read_text()
    mgr_src = (src_dir / "Server" / "manager.py").read_text()
    app_src = (src_dir / "Server" / "app.py").read_text()
    client_src = (src_dir / "Client" / "client.py").read_text()
    n_store = _norm(store_src)
    checks = {
        "store.drop": _norm(SQL_DROP.replace(STAGING, "{staging}")) in n_store,
        "store.create_temp": _norm(SQL_CREATE.replace(STAGING, "{staging}")) in n_store,
        "store.copy": _norm(SQL_COPY.replace(STAGING, "{staging}")) in n_store,
        "store.ref_ids": _norm(SQL_REF_IDS.replace(STAGING, "{staging}").replace(REF_IDS_TABLE, "{REF_IDS_TABLE}")) in n_store,
        "store.merge": _norm(SQL_MERGE.replace(STAGING, "{staging}").replace(REF_IDS_TABLE, "{REF_IDS_TABLE}").replace(TIMESERIES_TABLE, "{TIMESERIES_TABLE}", 1)) in n_store,
        "store.write_csv": 'write_csv(\n            include_header=False\n        )' in store_src or "write_csv(include_header=False)" in _norm(store_src),
        "store.dedupe": 'unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)' in store_src,
        "manager.value_kind_per_stream": "self._registered_value_kind(ref_uri)" in mgr_src and "for name in df[\"ref_name\"].unique().to_list():" in mgr_src,
        "manager.replace_maps": 'pl.col("ref_name").replace(ref_uri_map)' in mgr_src,
        "app.from_arrow_partition": "pl.from_arrow(reader.read_all())" in app_src and 'partition_by("source_id", as_dict=True)' in app_src,
        "client.ipc_stream": 'append_column("source_id", pa.repeat(source_id, len(table)))' in client_src,
        "driver.register_group_by": 'group_by(group).agg(pl.col("value"))' in (src_dir / "Drivers" / "Driver.py").read_text(),
    }
    repo = src_dir.parent.parent
    head = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    return {"checks": checks, "all_ok": all(checks.values()), "acquirium_path": str(src_dir), "clone_head": head}


class Timer:
    def __init__(self, rec: dict, stage_holder: dict):
        self.rec, self.holder = rec, stage_holder

    def __call__(self, name: str):
        rec, holder = self.rec, self.holder

        class _T:
            def __enter__(s):
                holder["stage"] = name
                s.t = time.perf_counter()

            def __exit__(s, *a):
                rec[name] = rec.get(name, 0.0) + (time.perf_counter() - s.t)
                holder["stage"] = "between"
        return _T()


class _NormSelf:
    """Stand-in for the driver instance in IngestDriver.normalize_observations."""
    to_timestamp = staticmethod(to_timestamp)


def pg_scalar(conn, q):
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchone()[0]


def snapshot_pg(conn, out: Path, label: str):
    with conn.cursor() as cur:
        cur.execute("""SELECT queryid, calls, total_exec_time, mean_exec_time, rows,
                              shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
                              temp_blks_read, temp_blks_written, shared_blk_read_time, shared_blk_write_time,
                              temp_blk_read_time, temp_blk_write_time, wal_records, wal_bytes, left(query, 300)
                       FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname='acquirium')
                       ORDER BY total_exec_time DESC LIMIT 40""")
        cols = [d[0] for d in cur.description]
        (out / f"pgss_{label}.json").write_text(json.dumps([dict(zip(cols, [str(v) if not isinstance(v, (int, float, type(None))) else v for v in r])) for r in cur.fetchall()], indent=1))
        io_stats = {}
        for name, q in {
            "pg_stat_io": "SELECT backend_type, object, context, reads, read_time, writes, write_time, writebacks, extends, extend_time, hits, evictions, reuses, fsyncs, fsync_time FROM pg_stat_io WHERE reads>0 OR writes>0 OR hits>0 OR extends>0",
            "pg_stat_checkpointer": "SELECT * FROM pg_stat_checkpointer",
            "pg_stat_bgwriter": "SELECT * FROM pg_stat_bgwriter",
            "pg_stat_wal": "SELECT * FROM pg_stat_wal",
            "pg_stat_user_tables": "SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_live_tup, seq_scan, idx_scan FROM pg_stat_user_tables ORDER BY n_tup_ins DESC LIMIT 20",
            "settings": "SELECT name, setting FROM pg_settings WHERE name IN ('shared_buffers','work_mem','maintenance_work_mem','wal_buffers','max_wal_size','checkpoint_timeout','checkpoint_completion_target','synchronous_commit','fsync','full_page_writes','wal_compression','effective_io_concurrency','random_page_cost','track_io_timing','shared_preload_libraries','max_parallel_workers','max_parallel_workers_per_gather','autovacuum','wal_level','jit','temp_buffers')",
        }.items():
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            io_stats[name] = [dict(zip(cols, [v if isinstance(v, (int, float, str, type(None))) else str(v) for v in r])) for r in cur.fetchall()]
        (out / f"pgio_{label}.json").write_text(json.dumps(io_stats, indent=1))


def sizes(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT table_bytes, index_bytes, total_bytes FROM hypertable_detailed_size('timeseries')")
        t, i, tot = cur.fetchone()
        cur.execute("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='timeseries'")
        chunks = cur.fetchone()[0]
    return {"table_bytes": t, "index_bytes": i, "total_bytes": tot, "chunks": chunks}


def activity_sampler(dsn, out: Path, holder: dict, stop: threading.Event, my_pid: int, hz: float = 5.0):
    conn = psycopg.connect(dsn, autocommit=True)
    f = open(out / "activity.jsonl", "a", buffering=1)
    q = """SELECT pid, backend_type, state, wait_event_type, wait_event, left(query, 80)
           FROM pg_stat_activity
           WHERE pid <> pg_backend_pid() AND (pid = %s OR backend_type IN ('checkpointer','background writer','walwriter','autovacuum worker'))"""
    while not stop.is_set():
        t = time.time()
        try:
            with conn.cursor() as cur:
                cur.execute(q, (my_pid,))
                rows = cur.fetchall()
            f.write(json.dumps({"t": t, "stage": holder.get("stage"), "file": holder.get("file"), "rows": rows}) + "\n")
        except Exception as e:  # noqa: BLE001
            f.write(json.dumps({"t": t, "error": str(e)}) + "\n")
        time.sleep(max(0.0, 1.0 / hz - (time.time() - t)))
    f.close()


def sys_sampler(out: Path, holder: dict, stop: threading.Event, tsdb: str, period=5.0):
    f = open(out / "sysstat.jsonl", "a", buffering=1)
    while not stop.is_set():
        t = time.time()
        rec = {"t": t, "stage": holder.get("stage"), "file": holder.get("file")}
        try:
            r = subprocess.run(["docker", "exec", tsdb, "sh", "-c", "cat /sys/fs/cgroup/cpu.stat; echo MEM; cat /sys/fs/cgroup/memory.current"],
                               capture_output=True, text=True, timeout=20)
            cpu, mem = r.stdout.split("MEM")
            rec["pg_cgroup"] = dict(l.split() for l in cpu.strip().splitlines() if len(l.split()) == 2)
            rec["pg_mem_bytes"] = int(mem.strip() or 0)
        except Exception as e:  # noqa: BLE001
            rec["pg_err"] = str(e)
        ru = resource.getrusage(resource.RUSAGE_SELF)
        rec["py"] = {"utime": ru.ru_utime, "stime": ru.ru_stime, "maxrss_kb": ru.ru_maxrss}
        with open("/sys/fs/cgroup/cpu.stat") as c:
            rec["vm_cgroup"] = dict(l.split() for l in c.read().splitlines() if len(l.split()) == 2)
        f.write(json.dumps(rec) + "\n")
        time.sleep(max(0.0, period - (time.time() - t)))
    f.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dsn", default="postgresql://acquirium:acquirium@127.0.0.1:5433/acquirium")
    ap.add_argument("--tsdb", default="acq_bench_tsdb")
    ap.add_argument("--max-files", type=int, default=1000)
    ap.add_argument("--deadline-s", type=float, default=2400)
    ap.add_argument("--explain-every", type=int, default=25)
    ap.add_argument("--variant", choices=["baseline", "plain-insert"], default="baseline")
    a = ap.parse_args()
    merge_sql = SQL_MERGE_PLAIN if a.variant == "plain-insert" else SQL_MERGE
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True); (out / "explain").mkdir(exist_ok=True)
    log = open(out / "trace.log", "a", buffering=1)

    def say(msg):
        line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
        print(line, flush=True); log.write(line + "\n")

    fid = fidelity_check()
    say(f"fidelity: {json.dumps(fid)}")
    if not fid["all_ok"]:
        raise SystemExit("fidelity check failed: the copied pipeline no longer matches the branch source")

    say("creating store (TimescaleStore recreate=True → ensure_table, the real schema)")
    t0 = time.perf_counter()
    store = TimescaleStore(dsn=a.dsn, recreate=True)
    conn = store.conn
    say(f"schema ready in {time.perf_counter() - t0:.2f}s")
    if a.variant == "plain-insert":
        with conn.cursor() as cur:
            for ddl in VARIANT_DDL:
                cur.execute(ddl)
        say(f"variant plain-insert: {VARIANT_DDL}; merge SQL:\n{merge_sql}")
    with conn.cursor() as cur:
        cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s ORDER BY 1", (TIMESERIES_TABLE,))
        indexes = cur.fetchall()
    say(f"indexes on {TIMESERIES_TABLE}: {indexes}")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        cur.execute("SELECT pg_stat_statements_reset()")
        cur.execute("SELECT pg_stat_reset_shared('io'); SELECT pg_stat_reset_shared('bgwriter'); SELECT pg_stat_reset_shared('checkpointer'); SELECT pg_stat_reset_shared('wal')")
        cur.execute("SELECT pg_backend_pid()"); my_pid = cur.fetchone()[0]
        cur.execute("SELECT version(), (SELECT extversion FROM pg_extension WHERE extname='timescaledb')")
        pgver = cur.fetchone()
    say(f"postgres backend pid={my_pid} {pgver}")
    snapshot_pg(conn, out, "start")

    holder = {"stage": "setup", "file": None}
    stop = threading.Event()
    threads = [
        threading.Thread(target=activity_sampler, args=(a.dsn, out, holder, stop, my_pid), daemon=True),
        threading.Thread(target=sys_sampler, args=(out, holder, stop, a.tsdb), daemon=True),
    ]
    for th in threads:
        th.start()

    files = sorted(p for p in Path(a.data).rglob("*.csv") if p.is_file())[: a.max_files]
    say(f"{len(files)} files, deadline {a.deadline_s:.0f}s, explain every {a.explain_every}")
    declared: dict[str, dict] = {}      # driver._declarations
    registered: set[str] = set()        # driver._registered
    kinded: set[str] = set()            # driver._kinded
    ff = open(out / "files.jsonl", "a", buffering=1)
    run_t0 = time.perf_counter(); run_wall0 = time.time()
    total_rows = 0; done = 0
    stopping = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stopping.__setitem__("flag", True))

    for idx, path in enumerate(files):
        if stopping["flag"] or time.perf_counter() - run_t0 > a.deadline_s:
            say(f"stopping before file {idx}: {'SIGTERM' if stopping['flag'] else 'deadline'}")
            break
        rec = {"idx": idx, "file": path.name, "bytes": path.stat().st_size, "t_start": time.time()}
        T = Timer(rec, holder); holder["file"] = idx
        file_t0 = time.perf_counter()
        wal0 = pg_scalar(conn, "SELECT pg_current_wal_lsn()")

        # ---------------- driver: CSVIngestDriver.read ----------------
        with T("d1.header_scan"):
            hdr = _header_line(path, tuple(DRIVER_CFG["header_contains"]), ",", DRIVER_CFG["encoding"])
            skip_rows = frozenset(range(1, hdr))
        with T("d2.read_text"):
            text = _read_text(path, DRIVER_CFG["encoding"])
        with T("d3.skip_lines"):
            if skip_rows:
                text = "".join(line for n, line in enumerate(text.splitlines(keepends=True), 1) if n not in skip_rows)
        with T("d4.read_csv"):
            df = pl.read_csv(io.StringIO(text), separator=",", try_parse_dates=False, skip_rows_after_header=0,
                             encoding=DRIVER_CFG["encoding"], truncate_ragged_lines=True,
                             null_values=DRIVER_CFG["null_values"], infer_schema=False)
        rec["wide_rows"], rec["wide_cols"] = df.height, df.width
        with T("d5.to_observations"):
            observations = to_observations(df, time_col=None, date_col=DRIVER_CFG["date_col"], clock_col=DRIVER_CFG["clock_col"],
                                           layout="wide", date_format=DRIVER_CFG["date_format"], timezone="UTC", day_first=False)
        with T("d6.declare_unique"):
            for name in observations["ref_name"].unique():
                declared.setdefault((SOURCE_ID, name), {})
        rec["obs_rows"] = observations.height
        del df, text

        # ---------------- driver: IngestDriver.insert_observations ----------------
        with T("d7.normalize_observations"):
            df = IngestDriver.normalize_observations(_NormSelf(), observations)
        del observations
        with T("d8.register_declared"):
            # exactly register_declared(): group every value into Python lists per stream, infer kinds for new streams
            by_stream = df.group_by(["ref_name"]).agg(pl.col("value"))
            observed = {(SOURCE_ID, str(r["ref_name"])): r["value"] for r in by_stream.iter_rows(named=True)}
            specs = {k: dict(m) for k, m in declared.items() if k not in registered}
            inferred = set()
            for key, values in observed.items():
                if key in kinded or "value_kind" in declared[key]:
                    continue
                meaningful = [v for v in values if v is not None and not (isinstance(v, str) and not v.strip())]
                if not meaningful:
                    continue
                specs.setdefault(key, dict(declared[key]))["value_kind"] = assign_stream_value_kind(meaningful)
                inferred.add(key)
            del by_stream, observed
        rec["new_streams"] = len(specs)
        if specs:
            # server side of register_streams: TimescaleStore.ensure_stream_refs (graph write not reproduced)
            with T("d9.register_streams_db"):
                store.ensure_stream_refs([(None, s, n, str(compute_ref_uri(s, n)), m.get("value_kind", "text")) for (s, n), m in specs.items()])
            registered.update(specs); kinded.update(inferred)

        # ---------------- client: insert_timeseries_arrow ----------------
        with T("c1.to_arrow"):
            table = df.to_arrow()
        del df
        with T("c2.ipc_serialize"):
            table_with_sid = table.append_column("source_id", pa.repeat(SOURCE_ID, len(table)))
            buf = io.BytesIO()
            with ipc.new_stream(buf, table_with_sid.schema) as writer:
                writer.write_table(table_with_sid)
            body = buf.getvalue()
        rec["ipc_bytes"] = len(body)
        del table, table_with_sid, buf

        # ---------------- server route: /insert_timeseries_arrow ----------------
        with T("s1.ipc_parse"):
            reader = ipc.RecordBatchStreamReader(pa.BufferReader(body))
            sdf = pl.from_arrow(reader.read_all())
        with T("s2.partition_by_source"):
            parts = sdf.partition_by("source_id", as_dict=True)
            (key, source_df), = parts.items()
            arrow_in = source_df.drop("source_id").to_arrow()
        del body, sdf, parts, source_df

        # ---------------- manager: insert_timeseries_arrow ----------------
        with T("m1.from_arrow"):
            mdf = pl.from_arrow(arrow_in)
            n_streams = mdf["ref_name"].n_unique()
        rec["streams"] = n_streams
        with T("m2.ref_uri_and_value_kind_lookup"):
            ref_uri_map, value_kind_map = {}, {}
            for name in mdf["ref_name"].unique().to_list():
                ref_uri = str(compute_ref_uri(SOURCE_ID, name))
                ref_uri_map[name] = ref_uri
                vk = store.stream_value_kind(ref_uri)          # one SELECT per stream, as the manager does
                if vk is None:
                    raise ValueError(f"stream {ref_uri} is not registered")
                value_kind_map[name] = normalize_value_kind(vk)
        with T("m3.replace_select"):
            mdf = (mdf.with_columns([pl.col("ref_name").replace(ref_uri_map).alias("ref_uri"),
                                     pl.col("ref_name").replace(value_kind_map).alias("value_kind")])
                   .drop("ref_name").select(["ref_uri", "ts", "value", "value_kind"]))
        del arrow_in

        # ---------------- store: bulk_insert_polars ----------------
        with T("t1.prepare_frame_split_dedupe"):
            pdf = TimescaleStore._prepare_frame(mdf)
        rec["rows_after_dedupe"] = pdf.height
        del mdf
        explain = a.explain_every and (idx % a.explain_every == 0)
        with T("t2.transaction_total"):
            with conn.transaction(), conn.cursor() as cur:
                with T("t3.drop_create_temp"):
                    cur.execute(SQL_DROP); cur.execute(SQL_CREATE)
                with T("t4.write_csv"):
                    payload = pdf.select(["ref_uri", "ts", "numeric_value", "text_value"]).write_csv(include_header=False)
                rec["csv_bytes"] = len(payload)
                with T("t5.copy_staging"):
                    with cur.copy(SQL_COPY) as copy:
                        copy.write(payload)
                del payload
                with T("t6.ref_ids_insert"):
                    cur.execute(SQL_REF_IDS)
                with T("t7.merge_on_conflict"):
                    if explain:
                        cur.execute("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT TEXT) " + merge_sql)
                        plan = "\n".join(r[0] for r in cur.fetchall())
                        (out / "explain" / f"{idx:04d}.txt").write_text(f"-- file {path.name}, rows {pdf.height}\n{plan}\n")
                        rec["explained"] = True
                        m = re.search(r"Execution Time: ([\d.]+) ms", plan)
                        rec["merge_explain_exec_ms"] = float(m.group(1)) if m else None
                    else:
                        cur.execute(merge_sql)
                        rec["merged"] = cur.rowcount
                with T("t8.commit"):
                    pass  # the transaction() context commits on exit; measured as transaction_total - sum(inner)
        del pdf
        rec["t8.commit"] = rec["t2.transaction_total"] - sum(rec[k] for k in ("t3.drop_create_temp", "t4.write_csv", "t5.copy_staging", "t6.ref_ids_insert", "t7.merge_on_conflict"))
        wal1 = pg_scalar(conn, "SELECT pg_current_wal_lsn()")
        rec["wal_bytes"] = int(pg_scalar(conn, f"SELECT pg_wal_lsn_diff('{wal1}','{wal0}')"))
        rec["file_total"] = time.perf_counter() - file_t0
        if idx % 10 == 0:
            rec["sizes"] = sizes(conn)
        ru = resource.getrusage(resource.RUSAGE_SELF)
        rec["py_maxrss_kb"] = ru.ru_maxrss; rec["py_cpu"] = ru.ru_utime + ru.ru_stime
        ff.write(json.dumps(rec) + "\n")
        total_rows += rec["rows_after_dedupe"]; done += 1
        elapsed = time.perf_counter() - run_t0
        say(f"file {idx + 1}/{len(files)} {path.name[:40]!r} rows={rec['rows_after_dedupe']} total={rec['file_total']:.2f}s "
            f"(csv {rec['d4.read_csv']:.2f} melt {rec['d5.to_observations']:.2f} reg {rec['d8.register_declared']:.2f} "
            f"prep {rec['t1.prepare_frame_split_dedupe']:.2f} copy {rec['t5.copy_staging']:.2f} merge {rec['t7.merge_on_conflict']:.2f}) "
            f"elapsed {elapsed:.0f}s avg {elapsed / done:.2f}s/file {total_rows / elapsed:,.0f} rows/s")

    holder["stage"] = "teardown"
    elapsed = time.perf_counter() - run_t0
    snapshot_pg(conn, out, "end")
    final = sizes(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM timeseries"); n_db = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM ref_ids"); n_ref = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM streams"); n_streams = cur.fetchone()[0]
    stop.set()
    for th in threads:
        th.join(timeout=30)
    summary = {
        "started": datetime.fromtimestamp(run_wall0, timezone.utc).isoformat(), "finished": datetime.now(timezone.utc).isoformat(),
        "files_done": done, "files_total": len(files), "rows_pushed": total_rows, "rows_in_db": n_db, "ref_ids": n_ref, "streams": n_streams,
        "ingest_wall_s": elapsed, "rows_per_s": total_rows / elapsed if elapsed else None, "sizes": final,
        "postgres": [str(x) for x in pgver], "fidelity": fid, "driver_cfg": DRIVER_CFG, "explain_every": a.explain_every,
        "versions": {"polars": pl.__version__, "pyarrow": pa.__version__, "psycopg": psycopg.__version__, "python": sys.version.split()[0]},
        "stopped_by": "SIGTERM" if stopping["flag"] else ("deadline" if done < len(files) else "complete"),
        "variant": a.variant, "merge_sql": merge_sql, "variant_ddl": VARIANT_DDL if a.variant == "plain-insert" else [],
        "indexes": [list(i) for i in indexes],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    say(f"done: {json.dumps({k: v for k, v in summary.items() if k not in ('fidelity',)})}")
    store.close()


if __name__ == "__main__":
    main()
