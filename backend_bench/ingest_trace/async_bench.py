"""End-to-end check of TimescaleStore async_merge: acquirium server + DPR CSV driver.

usage: async_bench.py --mode sync|async --files N --root DIR --venv VENV --data CSVDIR

Fresh Timescale container, fresh data_dir, server started from the venv (the working
tree under test), driver pushed with `acquirium driver start`. While the driver ingests
N files a probe thread hits GET /health and a 1-day GET /timeseries window every second
and records the latency, so "does anyone else wait for the merge" is measured, not
assumed. Ingest cadence comes from the server log ("inserted N observation(s)"), the
merge tail from the store's "merged ... rows" lines and from the row count in Postgres.
Writes <root>/<mode>/result.json.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

TSDB = "acq_bench_tsdb"
IMG = "timescale/timescaledb:latest-pg17"
PG_DSN = "postgresql://acquirium:acquirium@127.0.0.1:5433/acquirium"
PORT = 8123
HERE = Path(__file__).resolve().parent


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def psql(q):
    return sh(["docker", "exec", TSDB, "psql", "-U", "acquirium", "-d", "acquirium", "-tA", "-c", q]).stdout.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["sync", "async"], required=True)
    ap.add_argument("--files", type=int, default=100)
    ap.add_argument("--root", required=True)
    ap.add_argument("--venv", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--probe-col", default="UF-Feed Flow (GPM)")
    ap.add_argument("--probe-uri", default=None, help="uri passed to GET /timeseries (point_uri of a mapped stream); default: ref uri of --probe-col")
    ap.add_argument("--tag", default=None, help="result directory name (default: mode)")
    a = ap.parse_args()
    root = Path(a.root) / (a.tag or a.mode)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    logf = open(root / "bench.log", "a", buffering=1)

    def say(msg):
        line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
        print(line, flush=True); logf.write(line + "\n")

    venv = Path(a.venv)
    files = sorted(Path(a.data).glob("*.csv"))[: a.files]
    data = root / "data"; data.mkdir()
    for f in files:
        os.symlink(f, data / f.name)
    say(f"mode={a.mode} files={len(files)} root={root}")

    # ---- fresh timescale
    sh(["docker", "rm", "-f", "-v", TSDB])
    sh(["docker", "run", "-d", "--name", TSDB, "-p", "127.0.0.1:5433:5432", "-e", "POSTGRES_USER=acquirium",
        "-e", "POSTGRES_PASSWORD=acquirium", "-e", "POSTGRES_DB=acquirium", "-e", "TS_TUNE_MEMORY=8GB",
        "-e", "TS_TUNE_NUM_CPUS=4", "--shm-size=1g", IMG], check=True)
    t0 = time.time()
    while time.time() - t0 < 300:
        if sh(["docker", "exec", TSDB, "pg_isready", "-U", "acquirium", "-d", "acquirium"]).returncode == 0:
            time.sleep(3)
            if sh(["docker", "exec", TSDB, "pg_isready", "-U", "acquirium", "-d", "acquirium"]).returncode == 0:
                break
        time.sleep(2)
    say(f"timescale ready in {time.time() - t0:.0f}s, public tables: {psql('SELECT count(*) FROM information_schema.tables WHERE table_schema=%s' % chr(39) + 'public' + chr(39))}")

    # ---- server + driver config (same as the backend benchmark)
    rundir = root / "run"; rundir.mkdir()
    (rundir / "server.toml").write_text(
        f'[server]\nhost = "127.0.0.1"\nport = {PORT}\ndata_dir = ".acquirium"\n'
        f'timeseries_backend = "timescale"\npg_dsn = "{PG_DSN}"\nrecreate = true\n'
        f'timescale_async_merge = {"true" if a.mode == "async" else "false"}\n')
    (rundir / "driver.toml").write_text(f'''
[driver]
server_url  = "127.0.0.1"
server_port = {PORT}
use_ssl     = false

[[drivers]]
spec              = "{HERE / 'dpr_driver.py'}:DPRDriver"
interval          = 30.0
insert_batch_rows = 50000
source_id         = "dpr-trailer"
watch_dir         = "{data}"
glob              = "*.csv"
format            = "wide"
date_col          = "Date"
clock_col         = "Time"
date_format       = "%m/%d/%Y %I:%M:%S %p"
header_contains   = ["Date", "Time"]
null_values       = ["NaN"]
'''.lstrip())
    server_log = open(rundir / "server.log", "w", buffering=1)
    env = dict(os.environ, ACQUIRIUM_VERBOSE="0")
    srv = subprocess.Popen([str(venv / "bin" / "acquirium"), "server", "--config", "server.toml"], cwd=rundir,
                           stdout=server_log, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    say(f"server pid={srv.pid}; waiting for /health")
    t0 = time.time()
    while time.time() - t0 < 1500:
        if srv.poll() is not None:
            raise SystemExit(f"server exited early rc={srv.returncode}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=5) as r:
                if r.status == 200:
                    break
        except Exception:
            pass
        time.sleep(3)
    else:
        raise SystemExit("server not healthy")
    boot_s = time.time() - t0
    say(f"server healthy after {boot_s:.0f}s; async_merge in effect: {a.mode == 'async'}")

    # ---- probe: request latency while ingest runs
    sys.path.insert(0, str(venv / "lib" / next((venv / "lib").glob("python3*")).name / "site-packages"))
    from acquirium.internals.models import compute_ref_uri  # noqa: E402
    probe_uri = a.probe_uri or str(compute_ref_uri("dpr-trailer", a.probe_col))
    q = urllib.parse.urlencode({"uri": probe_uri, "start": "2024-08-16T00:00:00Z", "end": "2024-08-17T00:00:00Z"})
    say(f"probe uri: {probe_uri}")
    probes = []; stop = threading.Event()

    def probe():
        while not stop.is_set():
            for name, url in (("health", f"http://127.0.0.1:{PORT}/health"), ("timeseries_1d", f"http://127.0.0.1:{PORT}/timeseries?{q}")):
                t = time.time()
                try:
                    with urllib.request.urlopen(url, timeout=120) as r:
                        n = len(r.read()); code = r.status
                except Exception as e:  # noqa: BLE001
                    n = -1; code = getattr(e, "code", -1)
                probes.append({"t": t, "probe": name, "ms": (time.time() - t) * 1000, "code": code, "bytes": n})
            time.sleep(max(0.0, 1.0 - (time.time() - t)))
    th = threading.Thread(target=probe, daemon=True); th.start()

    # ---- driver
    t_drv = time.time()
    r = sh([str(venv / "bin" / "acquirium"), "driver", "start", "driver.toml"], cwd=rundir, timeout=1800)
    if r.returncode != 0:
        raise SystemExit(f"driver start failed: {r.stderr[-800:]}")
    say(f"driver started in {time.time() - t_drv:.1f}s (setup incl. graph insert + registration)")
    ins_re = re.compile(r"^.*?(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) INFO acquirium.driver DPRDriver: (.*?) — inserted (\d+) observation")
    merged_re = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) INFO acquirium.Storage.timescale_store acquirium: merged (\d+) rows from (\S+) into timeseries in ([\d.]+) ms")
    inserted = []; merged = []; seen = 0
    t0 = time.time(); last_progress = time.time()
    while time.time() - t0 < 7200:
        lines = (rundir / "server.log").read_text(errors="replace").splitlines()
        for line in lines[seen:]:
            m = ins_re.match(line)
            if m:
                inserted.append({"t": m.group(1), "file": m.group(2), "rows": int(m.group(3)), "wall": time.time()}); last_progress = time.time()
            m = merged_re.match(line)
            if m:
                merged.append({"t": m.group(1), "rows": int(m.group(2)), "table": m.group(3), "ms": float(m.group(4)), "wall": time.time()})
        seen = len(lines)
        if len(inserted) >= len(files):
            break
        if time.time() - last_progress > 1800:
            say("no progress for 30 min; giving up"); break
        time.sleep(2)
    t_ingest_end = time.time()
    pushed = sum(x["rows"] for x in inserted)
    say(f"driver finished {len(inserted)} files, {pushed:,} rows in {t_ingest_end - t_drv:.0f}s ({pushed / (t_ingest_end - t_drv):,.0f} rows/s driver-observed)")
    # wait until every staged batch is merged
    while time.time() - t_ingest_end < 1800:
        n_staged = int(psql("SELECT count(*) FROM pg_tables WHERE tablename LIKE '_acquirium_staged_%'") or 0)
        if n_staged == 0:
            break
        time.sleep(2)
    t_merge_end = time.time()
    n_db = int(psql("SELECT count(*) FROM timeseries"))
    stop.set(); th.join(timeout=130)
    say(f"all merged {t_merge_end - t_ingest_end:.0f}s after the driver finished; {n_db:,} rows in db")
    size = psql("SELECT table_bytes, index_bytes, total_bytes FROM hypertable_detailed_size('timeseries')")

    # ---- probe summary
    def summ(name):
        v = sorted(p["ms"] for p in probes if p["probe"] == name and t_drv <= p["t"] <= t_ingest_end)
        if not v: return {}
        qq = lambda f: v[min(len(v) - 1, int(f * (len(v) - 1)))]
        return {"n": len(v), "p50_ms": qq(.5), "p90_ms": qq(.9), "p99_ms": qq(.99), "max_ms": v[-1],
                "over_1s": sum(1 for x in v if x > 1000), "over_5s": sum(1 for x in v if x > 5000),
                "errors": sum(1 for p in probes if p["probe"] == name and p["code"] != 200)}
    result = {
        "mode": a.mode, "tag": a.tag or a.mode, "probe_uri": probe_uri, "files": len(inserted), "rows_pushed": pushed, "rows_in_db": n_db, "boot_s": boot_s,
        "driver_wall_s": t_ingest_end - t_drv, "merge_tail_s": t_merge_end - t_ingest_end, "total_s": t_merge_end - t_drv,
        "rows_per_s_driver": pushed / (t_ingest_end - t_drv), "rows_per_s_total": pushed / (t_merge_end - t_drv),
        "probe_health": summ("health"), "probe_timeseries_1d": summ("timeseries_1d"),
        "merged_batches": len(merged), "merge_ms_sum": sum(m["ms"] for m in merged), "size": size,
        "started": datetime.fromtimestamp(t_drv, timezone.utc).isoformat(),
    }
    (root / "result.json").write_text(json.dumps(result, indent=1))
    (root / "probes.json").write_text(json.dumps(probes))
    (root / "inserted.json").write_text(json.dumps(inserted))
    (root / "merged.json").write_text(json.dumps(merged))
    say(f"result: {json.dumps({k: v for k, v in result.items() if k not in ('size',)})}")
    # ---- stop
    try:
        os.killpg(os.getpgid(srv.pid), signal.SIGTERM)
    except Exception:
        srv.terminate()
    t0 = time.time()
    while srv.poll() is None and time.time() - t0 < 180:
        time.sleep(2)
    if srv.poll() is None:
        os.killpg(os.getpgid(srv.pid), signal.SIGKILL)
    subprocess.run("ray stop --force >/dev/null 2>&1; pkill -f 'ray::' ", shell=True)
    say(f"server stopped rc={srv.returncode}; staged tables left: {psql(chr(39).join(['SELECT count(*) FROM pg_tables WHERE tablename LIKE ', '_acquirium_staged_%', '']))}")


if __name__ == "__main__":
    main()
