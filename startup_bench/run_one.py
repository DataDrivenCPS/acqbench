"""One measured acquirium start inside a capped container.

  run_one.py --name S --rep N --exact 0|1 [--template DIR] --mode traced|plain

Prepares data_dir (empty, or a fresh copy of a template), launches the
server, samples CPU/RSS of the server and its Ray children plus the
container cgroup, records /health and driver readiness, runs first-request
probes, stops the server, and records on-disk sizes.  Everything lands in
results/<S>/<mode>-<N>/.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
VENV = Path(os.environ.get("VENV_DIR", Path.home() / "acq_startup" / "venv"))
TICK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")

CONFIG = """[server]
host = "127.0.0.1"
port = {port}
data_dir = "{data_dir}"
timeseries_backend = "duckdb"
exact_only = {exact}
recreate = false
materialization_workers = 2

[driver]
server_url = "127.0.0.1"
server_port = {port}
use_ssl = false
interval = 10.0

[[drivers]]
spec     = "acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver"
interval = 30.0
"""

SPARQL = "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p"

PROBES = [
    ("embedding_status", "/embedding_status"),
    ("resolve exact class #1", "/resolve_text?" + urllib.parse.urlencode({"text": "aeration basin", "kind": "class"})),
    ("resolve fuzzy class #1", "/resolve_text?" + urllib.parse.urlencode({"text": "basin for aeration", "kind": "class"})),
    ("resolve fuzzy class #2", "/resolve_text?" + urllib.parse.urlencode({"text": "tank where sludge settles", "kind": "class"})),
    ("resolve fuzzy unit #1", "/resolve_text?" + urllib.parse.urlencode({"text": "milligrams per litre", "kind": "unit"})),
    ("resolve exact unit #1", "/resolve_text?" + urllib.parse.urlencode({"text": "mg/L", "kind": "unit"})),
    ("sparql #1", "/sparql_json?" + urllib.parse.urlencode({"query": SPARQL})),
    ("sparql #2", "/sparql_json?" + urllib.parse.urlencode({"query": SPARQL})),
    ("graph_version", "/graph_version"),
]


def now() -> int:
    return time.monotonic_ns()


def get(url: str, timeout: float = 600):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read()


def proc_tree(root: int) -> list[int]:
    kids: dict[int, list[int]] = {}
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            with open(f"/proc/{p}/stat") as f:
                ppid = int(f.read().rsplit(")", 1)[1].split()[1])
        except OSError:
            continue
        kids.setdefault(ppid, []).append(int(p))
    out, todo = [], [root]
    while todo:
        pid = todo.pop()
        out.append(pid)
        todo.extend(kids.get(pid, []))
    return out


def proc_stat(pid: int):
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        with open(f"/proc/{pid}/comm") as f:
            comm = f.read().strip()
    except OSError:
        return None
    cpu = (int(fields[11]) + int(fields[12])) / TICK
    rss = int(fields[21]) * PAGE
    return comm, cpu, rss


def cgroup():
    try:
        mem = int(Path("/sys/fs/cgroup/memory.current").read_text())
        usage = 0
        for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines():
            if line.startswith("usage_usec"):
                usage = int(line.split()[1])
        return mem, usage / 1e6
    except OSError:
        return None, None


def dir_size(p: Path) -> int:
    if p.is_file():
        return p.stat().st_size
    total = 0
    for root, _, files in os.walk(p):
        for fn in files:
            try:
                total += os.lstat(os.path.join(root, fn)).st_size
            except OSError:
                pass
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--exact", type=int, required=True)
    ap.add_argument("--template")
    ap.add_argument("--mode", choices=["traced", "plain"], default="traced")
    ap.add_argument("--port", type=int, default=8123)
    ap.add_argument("--keep", help="copy the resulting data_dir here (template creation)")
    args = ap.parse_args()

    out = Path(os.environ.get("RESULTS_ROOT", HERE / "results")) / args.name / f"{args.mode}-{args.rep}"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    work = Path.home() / "acq_startup" / "work"
    data_dir = work / "data"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    t = now()
    if args.template:
        subprocess.run(["cp", "-a", args.template, str(data_dir)], check=True)
    else:
        data_dir.mkdir()
    prep_s = (now() - t) / 1e9
    # flush the copy to disk so the start doesn't pay for someone else's writeback
    subprocess.run(["sync"], check=False)
    cfg = work / "acquirium.toml"
    cfg.write_text(CONFIG.format(port=args.port, data_dir=data_dir,
                                 exact="true" if args.exact else "false"))

    env = dict(os.environ)
    for k in list(env):
        if k.startswith("ACQUIRIUM_") or k in ("FASTEMBED_CACHE_PATH", "PG_DSN"):
            env.pop(k)
    env.update(TRACE_DIR=str(out), BENCH_CONFIG=str(cfg), PYTHONUNBUFFERED="1")
    if args.mode == "traced":
        cmd = [str(VENV / "bin" / "python"), str(HERE / "trace_server.py")]
    else:
        cmd = [str(VENV / "bin" / "acquirium"), "server", "-c", str(cfg), "-v",
               "--host", "127.0.0.1", "--port", str(args.port)]

    samples: list[dict] = []
    stop = threading.Event()
    log = open(out / "server.log", "wb")
    t_launch = now()
    env["T_LAUNCH_NS"] = str(t_launch)
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, cwd=work)

    def sampler():
        while not stop.is_set():
            ts = now()
            srv_cpu = srv_rss = ray_cpu = ray_rss = 0.0
            nray = 0
            for pid in proc_tree(proc.pid):
                s = proc_stat(pid)
                if s is None:
                    continue
                if pid == proc.pid:
                    srv_cpu, srv_rss = s[1], s[2]
                else:
                    ray_cpu += s[1]
                    ray_rss += s[2]
                    nray += 1
            mem, cpu = cgroup()
            samples.append({"t": (ts - t_launch) / 1e9, "srv_cpu": srv_cpu, "srv_rss": srv_rss,
                            "ray_cpu": ray_cpu, "ray_rss": ray_rss, "nproc": nray,
                            "cg_mem": mem, "cg_cpu": cpu})
            stop.wait(0.2)

    cg_mem0, cg_cpu0 = cgroup()
    load0 = Path("/proc/loadavg").read_text().split()[:3]
    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    base = f"http://127.0.0.1:{args.port}"
    marks: dict[str, float] = {}
    t_health = None
    while proc.poll() is None:
        try:
            if get(base + "/health", timeout=1)[0] == 200:
                t_health = now()
                break
        except Exception:
            pass
        time.sleep(0.05)
    if t_health is None:
        raise SystemExit(f"server exited rc={proc.returncode} before /health; see {out}/server.log")
    marks["health"] = (t_health - t_launch) / 1e9

    drivers = []
    deadline = time.time() + 600
    while time.time() < deadline:
        try:
            drivers = json.loads(get(base + "/drivers/list", timeout=5)[1])["drivers"]
            if drivers:
                marks["drivers_started"] = (now() - t_launch) / 1e9
                break
        except Exception:
            pass
        time.sleep(0.1)

    probes = []
    for label, path in PROBES:
        t0 = now()
        try:
            status, body = get(base + path)
            info = json.loads(body)
        except Exception as exc:
            status, info = None, {"error": repr(exc)}
        t1 = now()
        summary = {}
        if isinstance(info, dict):
            if "matches" in info:
                summary = {"n": len(info["matches"]),
                           "stages": sorted({m.get("match_stage") for m in info["matches"]}),
                           "top": info["matches"][0]["uri"] if info["matches"] else None}
            elif "semantic" in info:
                summary = {"semantic": info["semantic"],
                           "graph": {k: info["graph"].get(k) for k in ("state", "surfaces", "duration_s")},
                           "qudt": {k: info["qudt"].get(k) for k in ("state", "surfaces", "duration_s")}}
            elif "rows" in info:
                summary = {"rows": len(info["rows"])}
            else:
                summary = {k: v for k, v in info.items() if not isinstance(v, (list, dict))}
        probes.append({"label": label, "start": (t0 - t_launch) / 1e9,
                       "ms": (t1 - t0) / 1e6, "status": status, "summary": summary})

    t_stop = now()
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=90)
        stop_s = (now() - t_stop) / 1e9
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        stop_s = None
    stop.set()
    th.join()
    log.close()
    time.sleep(3)
    # Ray children that outlive the server would bleed into the next run
    leftovers = subprocess.run(["pgrep", "-f", "ray::|raylet|gcs_server"],
                               capture_output=True, text=True).stdout.split()
    if leftovers:
        subprocess.run(["pkill", "-9", "-f", "ray::|raylet|gcs_server"], check=False)

    sizes = {}
    for rel in [".oxigraph/source", ".oxigraph/query", ".ontoenv", "embedding_cache/models",
                "embedding_cache/graph", "embedding_cache/qudt", "timeseries.duckdb"]:
        p = data_dir / rel
        sizes[rel] = dir_size(p) if p.exists() else 0
    sizes["total"] = dir_size(data_dir)
    listing = sorted(str(p.relative_to(data_dir)) for p in data_dir.rglob("*")
                     if p.is_dir() and len(p.relative_to(data_dir).parts) <= 2)

    meta = {
        "name": args.name, "rep": args.rep, "mode": args.mode, "exact_only": bool(args.exact),
        "template": args.template, "prep_copy_s": prep_s, "t_launch": t_launch,
        "marks": marks, "stop_s": stop_s, "exit_code": proc.returncode,
        "leftover_ray_pids": leftovers, "drivers": [d.get("name") for d in drivers],
        "probes": probes, "sizes": sizes, "dirs": listing,
        "host": os.uname().nodename, "cgroup_cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        "cgroup_mem_max": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
        "cg_mem0": cg_mem0, "cg_cpu0": cg_cpu0, "loadavg_start": load0,
        "loadavg_end": Path("/proc/loadavg").read_text().split()[:3],
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=1))
    (out / "samples.json").write_text(json.dumps(samples))
    if args.keep:
        keep = Path(args.keep)
        if keep.exists():
            shutil.rmtree(keep)
        subprocess.run(["cp", "-a", str(data_dir), str(keep)], check=True)
    print(json.dumps({"name": args.name, "rep": args.rep, "mode": args.mode, **marks,
                      "stop_s": stop_s}))


if __name__ == "__main__":
    main()
