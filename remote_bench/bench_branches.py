#!/usr/bin/env python3
"""Full-stack acquirium benchmark: ums-duckdb-fix vs main, on real DPR CSVs.

Runs the real thing — `acquirium server` plus the real DPRTrailerCSVDriver
pushed over HTTP — against /home/mete/dpr_1 (200 SCADA CSVs), and measures:

  ingest   wall time and rows/s for the driver to consume all 200 files,
           taken from the driver's own per-file "inserted N row(s)" log lines,
           plus the resulting .duckdb file size and a COUNT(*) cross-check.
  query    latency of the two HTTP query templates the API exposes,
           POST /timeseries_info and GET /timeseries, over a spread of
           selectivities, with warmups discarded.

Branch order is ums-duckdb-fix first, then main (as requested).

Designed to run unattended: every wait has a timeout, every phase checkpoints
its results to disk, a failing branch never blocks the other, and all child
processes are killed on the way out. Progress goes to <root>/bench.log.

Usage:  python3 bench_branches.py [--root DIR] [--data DIR] [--quick]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_URL = "https://github.com/DataDrivenCPS/acquirium.git"
BRANCHES = ["ums-duckdb-fix", "main"]        # new branch first, per request
PY_VERSION = "3.12"                          # system python is 3.14; pin a supported one

BOOT_TIMEOUT_S = 1800        # cold boot builds the embedding cache; be generous
INGEST_TIMEOUT_S = 8 * 3600  # ~140M rows; main is the slow one
IDLE_ABORT_S = 1800          # no new file ingested for this long -> call it stuck
POLL_S = 10

INFO_REPS = 20
TS_REPS = 20
WARMUP = 3

SCRIPT_DIR = Path(__file__).resolve().parent   # probe + dpr_scripts live beside this file

INSERTED_RE = re.compile(r"inserted\s+([\d,]+)\s+row", re.I)


class Bench:
    def __init__(self, root: Path, data: Path, quick: bool) -> None:
        self.root = root
        self.data = data
        self.quick = quick
        self.results_dir = root / "results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.logf = open(root / "bench.log", "a", buffering=1)
        self.n_files = len(list(data.glob("*.csv")))
        self.procs: list[subprocess.Popen] = []

    # ---------------------------------------------------------------- utils

    def log(self, msg: str) -> None:
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line, flush=True)
        self.logf.write(line + "\n")

    def run(self, cmd: list[str], cwd: Path | None = None, timeout: int = 1800,
            env: dict | None = None) -> subprocess.CompletedProcess:
        self.log(f"$ {' '.join(cmd)}" + (f"   (cwd={cwd})" if cwd else ""))
        p = subprocess.run(cmd, cwd=cwd, timeout=timeout, capture_output=True,
                           text=True, env={**os.environ, **(env or {})})
        if p.returncode != 0:
            self.log(f"  !! exit {p.returncode}\n  stdout: {p.stdout[-2000:]}\n  stderr: {p.stderr[-2000:]}")
        return p

    # ------------------------------------------------------------- setup

    def prepare(self, branch: str) -> tuple[Path, Path, Path]:
        """Clone the branch, build a venv, lay out a run directory."""
        bdir = self.root / branch.replace("/", "_")
        repo, rundir = bdir / "repo", bdir / "run"
        venv_py = bdir / ".venv" / "bin" / "python"

        if not (repo / ".git").exists():
            shutil.rmtree(repo, ignore_errors=True)
            repo.parent.mkdir(parents=True, exist_ok=True)
            self.run(["git", "clone", "--depth", "1", "--branch", branch, REPO_URL, str(repo)],
                     timeout=900)
        head = self.run(["git", "-C", str(repo), "rev-parse", "HEAD"]).stdout.strip()
        self.log(f"{branch}: HEAD={head}")

        if not venv_py.exists():
            self.run(["uv", "venv", "--python", PY_VERSION, str(bdir / ".venv")], timeout=900)
        # -e install so the console script and the package line up
        self.run(["uv", "pip", "install", "--python", str(venv_py), "-e", "."],
                 cwd=repo, timeout=2400)

        # fresh run dir every time: a stale .acquirium would skip the cold boot
        shutil.rmtree(rundir, ignore_errors=True)
        rundir.mkdir(parents=True)
        raw = rundir / "raw"
        if not raw.exists():
            raw.symlink_to(self.data)

        scripts = SCRIPT_DIR / "dpr_scripts"
        port = 8100 + BRANCHES.index(branch)

        # Server config carries no [[drivers]]: the driver is pushed separately
        # so ingest timing excludes server boot (which builds the embedding cache).
        (rundir / "server.toml").write_text(f"""
[server]
host = "127.0.0.1"
port = {port}
data_dir = ".acquirium"
timeseries_backend = "duckdb"
duckdb_path = ".acquirium/timeseries.duckdb"
recreate = true
""".lstrip())

        (rundir / "driver.toml").write_text(f"""
[server]
enabled = false

[driver]
server_url  = "127.0.0.1"
server_port = {port}
use_ssl     = false

[[drivers]]
spec              = "{scripts / 'dpr_csv_driver.py'}:DPRTrailerCSVDriver"
interval          = 30.0
insert_batch_rows = 50000
watch_dir         = "{raw}"
""".lstrip())

        return repo, rundir, venv_py

    # ------------------------------------------------------------- server

    def start_server(self, rundir: Path, venv_py: Path, port: int) -> subprocess.Popen:
        logfile = open(rundir / "server.log", "w", buffering=1)
        p = subprocess.Popen(
            [str(venv_py.parent / "acquirium"), "server", "--config", "server.toml"],
            cwd=rundir, stdout=logfile, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.procs.append(p)
        self.log(f"server pid={p.pid} port={port}, waiting for /health …")
        t0 = time.time()
        while time.time() - t0 < BOOT_TIMEOUT_S:
            if p.poll() is not None:
                raise RuntimeError(f"server exited early ({p.returncode}); see server.log")
            try:
                import urllib.request
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as r:
                    if r.status == 200:
                        self.log(f"server healthy after {time.time() - t0:.0f}s")
                        return p
            except Exception:
                pass
            time.sleep(POLL_S)
        raise TimeoutError(f"server not healthy within {BOOT_TIMEOUT_S}s")

    def stop_all(self) -> None:
        for p in self.procs:
            if p.poll() is None:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                except Exception:
                    p.terminate()
        t0 = time.time()
        for p in self.procs:
            while p.poll() is None and time.time() - t0 < 180:
                time.sleep(2)
            if p.poll() is None:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except Exception:
                    p.kill()
        self.procs.clear()

    # ------------------------------------------------------------- ingest

    def ingest(self, rundir: Path, venv_py: Path) -> dict[str, Any]:
        server_log = rundir / "server.log"
        t0 = time.time()
        # config is a positional argument on `driver start` (unlike `server --config`)
        r = self.run([str(venv_py.parent / "acquirium"), "driver", "start", "driver.toml"],
                     cwd=rundir, timeout=300)
        if r.returncode != 0:
            raise RuntimeError(f"driver start failed: {r.stderr[-500:]}")

        target = self.n_files
        seen, rows, last_change = 0, 0, time.time()
        while True:
            time.sleep(POLL_S)
            text = server_log.read_text(errors="replace")
            hits = INSERTED_RE.findall(text)
            n, total = len(hits), sum(int(h.replace(",", "")) for h in hits)
            if n != seen:
                seen, rows, last_change = n, total, time.time()
                el = time.time() - t0
                self.log(f"  ingest {n}/{target} files, {rows:,} rows, "
                         f"{rows/max(el,1):,.0f} rows/s, {el/60:.1f} min")
            if seen >= target:
                self.log(f"ingest complete: {target} files")
                break
            if time.time() - last_change > IDLE_ABORT_S:
                self.log(f"!! no progress for {IDLE_ABORT_S}s — stopping at {seen}/{target}")
                break
            if time.time() - t0 > INGEST_TIMEOUT_S:
                self.log(f"!! ingest timeout at {seen}/{target}")
                break
        wall = time.time() - t0
        return {"files_ingested": seen, "files_total": target, "rows_inserted": rows,
                "wall_s": round(wall, 1), "rows_per_s": round(rows / max(wall, 1), 1),
                "complete": seen >= target}

    # -------------------------------------------------------------- query

    def query_bench(self, port: int, venv_py: Path) -> dict[str, Any]:
        """Drive the two HTTP query templates. Runs in the branch's own venv
        so pyarrow parses the Arrow stream exactly as a real client would."""
        script = SCRIPT_DIR / "query_probe.py"
        r = self.run([str(venv_py), str(script), str(port), str(SCRIPT_DIR / "dpr_scripts"),
                      str(INFO_REPS if not self.quick else 3),
                      str(TS_REPS if not self.quick else 3), str(WARMUP)], timeout=7200)
        if r.returncode != 0:
            return {"error": r.stderr[-2000:]}
        try:
            return json.loads(r.stdout[r.stdout.index("{"):])
        except Exception as e:
            return {"error": f"unparseable probe output: {e}", "raw": r.stdout[-2000:]}

    # -------------------------------------------------------------- branch

    def run_branch(self, branch: str) -> dict[str, Any]:
        port = 8100 + BRANCHES.index(branch)
        out: dict[str, Any] = {"branch": branch, "port": port,
                               "started": datetime.now(timezone.utc).isoformat()}
        try:
            repo, rundir, venv_py = self.prepare(branch)
            out["head"] = self.run(["git", "-C", str(repo), "rev-parse", "HEAD"]).stdout.strip()

            boot0 = time.time()
            self.start_server(rundir, venv_py, port)
            out["boot_s"] = round(time.time() - boot0, 1)

            out["ingest"] = self.ingest(rundir, venv_py)
            self.save(branch, out)

            out["query"] = self.query_bench(port, venv_py)
            self.save(branch, out)
        except Exception as e:
            out["error"] = f"{type(e).__name__}: {e}"
            self.log(f"!! branch {branch} failed: {out['error']}")
        finally:
            self.stop_all()
            time.sleep(5)
            try:
                db = self.root / branch.replace("/", "_") / "run" / ".acquirium" / "timeseries.duckdb"
                if db.exists():
                    out["db_bytes"] = db.stat().st_size
                    rows = out.get("ingest", {}).get("rows_inserted", 0)
                    if rows:
                        out["db_bytes_per_row"] = round(db.stat().st_size / rows, 2)
                    venv_py = self.root / branch.replace("/", "_") / ".venv" / "bin" / "python"
                    c = self.run([str(venv_py), "-c",
                                  f"import duckdb;print(duckdb.connect(r'{db}',read_only=True)"
                                  f".execute('SELECT count(*) FROM timeseries').fetchone()[0])"],
                                 timeout=600)
                    if c.returncode == 0:
                        out["db_row_count"] = int(c.stdout.strip())
            except Exception as e:
                out["db_check_error"] = str(e)
        out["finished"] = datetime.now(timezone.utc).isoformat()
        self.save(branch, out)
        return out

    def save(self, branch: str, out: dict) -> None:
        (self.results_dir / f"{branch.replace('/', '_')}.json").write_text(json.dumps(out, indent=1))

    # -------------------------------------------------------------- report

    def report(self, all_out: list[dict]) -> None:
        lines = ["# acquirium full-stack benchmark — ums-duckdb-fix vs main", "",
                 f"Host: {os.uname().nodename}  ·  data: {self.data} ({self.n_files} CSVs)",
                 f"Finished: {datetime.now(timezone.utc).isoformat()}", "",
                 "## Ingest (real server + real DPR CSV driver over HTTP)", "",
                 "| branch | files | rows | wall | rows/s | boot | DB size | B/row | count(*) |",
                 "|---|---|---|---|---|---|---|---|---|"]
        for o in all_out:
            i = o.get("ingest", {})
            db = o.get("db_bytes")
            db_s = f"{db/1e9:.2f} GB" if db else "—"
            bpr = o.get("db_bytes_per_row", "—")
            cnt = o.get("db_row_count")
            cnt_s = f"{cnt:,}" if isinstance(cnt, int) else "—"
            note = "" if i.get("complete") else "  ⚠ incomplete"
            lines.append(
                f"| {o['branch']}{note} "
                f"| {i.get('files_ingested','—')}/{i.get('files_total','—')} "
                f"| {i.get('rows_inserted',0):,} "
                f"| {i.get('wall_s',0)/60:.1f} min "
                f"| {i.get('rows_per_s',0):,.0f} "
                f"| {o.get('boot_s','—')}s "
                f"| {db_s} | {bpr} | {cnt_s} |"
            )
            if o.get("error"):
                lines.append(f"| _{o['branch']} error: {o['error'][:150]}_ ||||||||")
        lines += ["", "## Query latency (ms)", ""]
        for o in all_out:
            q = o.get("query", {})
            lines.append(f"### {o['branch']}")
            if "error" in q:
                lines += [f"ERROR: {q['error'][:300]}", ""]
                continue
            info = q.get("timeseries_info", {})
            lines += [f"- timeseries_info batch({info.get('n_uris','?')} uris): "
                      f"p50 {info.get('batch_p50_ms','?')} ms, mean {info.get('batch_mean_ms','?')} ms",
                      f"- timeseries_info single: p50 {info.get('single_p50_ms','?')} ms", "",
                      "| query | rows | p50 ms | mean ms | p95 ms |", "|---|---|---|---|---|"]
            for t in q.get("timeseries", []):
                lines.append(f"| {t['label']} | {t['rows']:,} | {t['p50_ms']} | {t['mean_ms']} | {t['p95_ms']} |")
            lines.append("")
        (self.results_dir / "RESULTS.md").write_text("\n".join(lines))
        self.log("wrote results/RESULTS.md")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=Path.home() / "acq_bench")
    ap.add_argument("--data", type=Path, default=Path("/home/mete/dpr_1"))
    ap.add_argument("--quick", action="store_true", help="tiny reps, for a smoke test")
    ap.add_argument("--branches", default=",".join(BRANCHES),
                    help="comma-separated subset, in run order")
    a = ap.parse_args()

    requested = [b.strip() for b in a.branches.split(",") if b.strip()]
    unknown = [b for b in requested if b not in BRANCHES]
    if unknown:
        print(f"unknown branch(es): {unknown}", file=sys.stderr)
        return 2
    # keep the canonical list for port assignment; iterate only the requested ones
    run_list = requested

    b = Bench(a.root, a.data, a.quick)
    b.log("=" * 70)
    b.log(f"START branches={run_list} data={a.data} ({b.n_files} csv) quick={a.quick}")
    all_out = []
    try:
        for br in run_list:
            b.log(f"----- branch {br} -----")
            all_out.append(b.run_branch(br))
            b.report(all_out)
    finally:
        b.stop_all()
        b.report(all_out)
    b.log("ALL DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
