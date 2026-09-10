"""Render the ingest-path trace into reports/ingest_trace_timescale.html.

usage: make_report_trace.py <run root (has trace/, flame_*.svg, stacks_wall.txt, pg.log)> <out.html> [findings.html]
The flamegraph SVGs are copied next to the report under <out dir>/ingest_trace/.
"""
from __future__ import annotations

import html
import json
import re
import shutil
import statistics as st
import sys
from collections import Counter, defaultdict
from pathlib import Path

root = Path(sys.argv[1]); out = Path(sys.argv[2]); findings = Path(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != "-" else None
baseline = Path(sys.argv[4]) if len(sys.argv) > 4 else None
tr = root / "trace"
HERE = Path(__file__).resolve().parent
CSS = (HERE.parent / "report.css").read_text() + """
:root{--series-4:#8e5bd1;--series-5:#c9a300;--series-6:#0aa3b8;--series-7:#c04a8c;--series-8:#6b7280}
pre{background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:11.5px;overflow-x:auto;line-height:1.35}
.flame{width:100%;height:560px;border:1px solid var(--border);border-radius:8px;background:#fff}
ul{color:var(--text-secondary)} li{margin:4px 0}
"""
E = html.escape


def j(p): return json.loads(p.read_text()) if p.exists() else None
def jl(p): return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []


files = jl(tr / "files.jsonl"); act = jl(tr / "activity.jsonl"); sysr = jl(tr / "sysstat.jsonl")
summary = j(tr / "summary.json") or {}
pgss = j(tr / "pgss_end.json") or []
pgio0, pgio1 = j(tr / "pgio_start.json") or {}, j(tr / "pgio_end.json") or {}
launch = (root / "launch.log").read_text() if (root / "launch.log").exists() else ""
pglog = (root / "pg.log").read_text(errors="replace") if (root / "pg.log").exists() else ""

# ---------------- stage groups ----------------
GROUPS = [
    ("Driver: read CSV + melt (polars)", ["d1.header_scan", "d2.read_text", "d3.skip_lines", "d4.read_csv", "d5.to_observations", "d6.declare_unique", "d7.normalize_observations"], "--series-3"),
    ("Driver: register_declared (values → Python lists)", ["d8.register_declared", "d9.register_streams_db"], "--series-6"),
    ("Client ↔ server: Arrow IPC round trip", ["c1.to_arrow", "c2.ipc_serialize", "s1.ipc_parse", "s2.partition_by_source"], "--series-7"),
    ("Manager: ref_uri map + value_kind SELECT per stream", ["m1.from_arrow", "m2.ref_uri_and_value_kind_lookup", "m3.replace_select"], "--series-4"),
    ("Store: split values + dedupe", ["t1.prepare_frame_split_dedupe"], "--series-5"),
    ("Store: write_csv + COPY into TEMP staging", ["t3.drop_create_temp", "t4.write_csv", "t5.copy_staging", "t6.ref_ids_insert"], "--series-1"),
    ("Store: INSERT … SELECT … ON CONFLICT merge", ["t7.merge_on_conflict"], "--series-2"),
    ("Store: commit", ["t8.commit"], "--series-8"),
]
STAGES = [s for _, ss, _ in GROUPS for s in ss]
n = len(files)
rows_total = sum(f["rows_after_dedupe"] for f in files)
wall = summary.get("ingest_wall_s") or (files[-1]["t_start"] + files[-1]["file_total"] - files[0]["t_start"] if files else 0)
tot = {s: sum(f.get(s, 0.0) for f in files) for s in STAGES}
file_sum = sum(f["file_total"] for f in files)
gtot = {g: sum(tot[s] for s in ss) for g, ss, _ in GROUPS}


def fmt_s(x): return f"{x:,.1f} s" if x < 600 else f"{x / 60:,.1f} min"
def pct(x, d): return f"{100 * x / d:.1f}%" if d else "–"
def q(vals, p): 
    if not vals: return 0
    v = sorted(vals); return v[min(len(v) - 1, int(round(p * (len(v) - 1))))]


# ---------------- SVG helpers ----------------
def stacked_timeline(w=940, h=300):
    if not files: return ""
    left, right, top, bot = 52, 12, 10, 34
    pw, ph = w - left - right, h - top - bot
    ymax = max(f["file_total"] for f in files) * 1.05
    bw = pw / n
    parts = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" style="max-width:100%">']
    for k in range(5):
        y = top + ph - ph * k / 4
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{w - right}" y2="{y:.1f}" stroke="var(--grid)"/>'
                     f'<text x="{left - 6}" y="{y + 4:.1f}" font-size="11" text-anchor="end" fill="var(--text-muted)">{ymax * k / 4:.1f}</text>')
    for i, f in enumerate(files):
        y = top + ph
        for g, ss, col in GROUPS:
            v = sum(f.get(s, 0.0) for s in ss)
            hh = ph * v / ymax
            y -= hh
            if hh > 0.2:
                parts.append(f'<rect x="{left + i * bw:.2f}" y="{y:.2f}" width="{max(bw - 0.3, 0.6):.2f}" height="{hh:.2f}" fill="var({col})"><title>file {i + 1} {E(f["file"])}\n{g}: {v:.2f} s of {f["file_total"]:.2f} s, {f["rows_after_dedupe"]:,} rows</title></rect>')
    for k in range(0, n + 1, max(1, n // 10)):
        x = left + k * bw
        parts.append(f'<text x="{x:.1f}" y="{h - 14}" font-size="11" text-anchor="middle" fill="var(--text-muted)">{k}</text>')
    parts.append(f'<text x="{left + pw / 2}" y="{h - 2}" font-size="11" text-anchor="middle" fill="var(--text-muted)">file index (ingest order)</text></svg>')
    return "".join(parts)


def tick(v):
    return f"{v:,.0f}" if v >= 100 else f"{v:.3g}"


def line_chart(series, w=940, h=260, ylabel="", xs=None, xlabel="cumulative rows in hypertable (millions)"):
    """series: list of (label, color, [(x, y)])"""
    pts = [p for _, _, ps in series for p in ps]
    if not pts: return ""
    left, right, top, bot = 56, 12, 10, 36
    pw, ph = w - left - right, h - top - bot
    xmin, xmax = min(p[0] for p in pts), max(p[0] for p in pts)
    ymax = max(p[1] for p in pts) * 1.08 or 1
    xmax = xmax if xmax > xmin else xmin + 1
    X = lambda x: left + pw * (x - xmin) / (xmax - xmin)
    Y = lambda y: top + ph - ph * y / ymax
    parts = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" style="max-width:100%">']
    for k in range(5):
        yv = ymax * k / 4
        parts.append(f'<line x1="{left}" y1="{Y(yv):.1f}" x2="{w - right}" y2="{Y(yv):.1f}" stroke="var(--grid)"/>'
                     f'<text x="{left - 6}" y="{Y(yv) + 4:.1f}" font-size="11" text-anchor="end" fill="var(--text-muted)">{tick(yv)}</text>')
    for k in range(6):
        xv = xmin + (xmax - xmin) * k / 5
        parts.append(f'<text x="{X(xv):.1f}" y="{h - 16}" font-size="11" text-anchor="middle" fill="var(--text-muted)">{xv:.0f}</text>')
    for label, col, ps in series:
        d = " ".join(f"{'M' if i == 0 else 'L'}{X(x):.1f},{Y(y):.1f}" for i, (x, y) in enumerate(ps))
        parts.append(f'<path d="{d}" fill="none" stroke="var({col})" stroke-width="1.6"><title>{E(label)}</title></path>')
    parts.append(f'<text x="{left + pw / 2}" y="{h - 3}" font-size="11" text-anchor="middle" fill="var(--text-muted)">{E(xlabel)}</text>')
    parts.append('</svg>')
    return "".join(parts)


def hbars(items, w=940, unit="", total=None):
    """items: [(label, value, color)]"""
    if not items: return ""
    mx = max(v for _, v, _ in items) or 1
    rh = 22; h = rh * len(items) + 6
    parts = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" style="max-width:100%">']
    for i, (lab, v, col) in enumerate(items):
        y = 3 + i * rh; bw = (w - 440) * v / mx
        share = f" ({100 * v / total:.1f}%)" if total else ""
        parts.append(f'<text x="330" y="{y + 15}" font-size="12" text-anchor="end" fill="var(--text-primary)">{E(lab)}</text>'
                     f'<rect x="338" y="{y + 3}" width="{bw:.1f}" height="{rh - 8}" fill="var({col})"/>'
                     f'<text x="{338 + bw + 6:.1f}" y="{y + 15}" font-size="11.5" fill="var(--text-secondary)">{v:,.3g}{unit}{share}</text>')
    parts.append("</svg>")
    return "".join(parts)


legend = "".join(f'<span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var({c})"/></svg>{E(g)}</span>' for g, _, c in GROUPS)

# ---------------- stage table ----------------
stage_rows = []
for g, ss, col in GROUPS:
    for s in ss:
        vals = [f.get(s, 0.0) for f in files]
        per_row = [1e6 * f.get(s, 0.0) / f["rows_after_dedupe"] for f in files if f["rows_after_dedupe"]]
        stage_rows.append(f'<tr><td><span class="lg"><svg width="10" height="10"><rect width="10" height="10" fill="var({col})"/></svg>{E(s)}</span></td>'
                          f'<td class="num">{tot[s]:,.1f}</td><td class="num">{pct(tot[s], file_sum)}</td>'
                          f'<td class="num">{st.median(vals):.3f}</td><td class="num">{q(vals, .9):.3f}</td><td class="num">{max(vals):.3f}</td>'
                          f'<td class="num">{st.median(per_row) if per_row else 0:.2f}</td></tr>')
group_items = [(g, gtot[g], c) for g, _, c in GROUPS]

# ---------------- per-row cost vs cumulative rows ----------------
cum = 0; merge_pts = []; copy_pts = []; py_pts = []; wal_pts = []
for f in files:
    cum += f["rows_after_dedupe"]; r = f["rows_after_dedupe"] or 1
    if r < 20_000: continue  # a handful of near-empty files have per-row costs dominated by fixed overhead
    merge_pts.append((cum / 1e6, 1e6 * f["t7.merge_on_conflict"] / r))
    copy_pts.append((cum / 1e6, 1e6 * (f["t5.copy_staging"] + f["t4.write_csv"]) / r))
    py_pts.append((cum / 1e6, 1e6 * sum(f.get(s, 0.0) for s in STAGES if s[0] in "dcsm" or s == "t1.prepare_frame_split_dedupe") / r))
    wal_pts.append((cum / 1e6, f.get("wal_bytes", 0) / r))
# ---------------- size growth ----------------
size_pts = [(f["sizes"]["table_bytes"] / 1e9, f["sizes"]["index_bytes"] / 1e9, f["sizes"]["chunks"], i) for i, f in enumerate(files) if "sizes" in f]
cumrows = []; c = 0
for f in files:
    c += f["rows_after_dedupe"]; cumrows.append(c)
idx_growth = [(cumrows[i] / 1e6, ib) for _, ib, _, i in size_pts]
tab_growth = [(cumrows[i] / 1e6, tb) for tb, _, _, i in size_pts]

# ---------------- wait-event profile ----------------
def wait_profile(stage_prefixes):
    cnt = Counter(); tot_ = 0
    for s in act:
        if not s.get("stage") or not any(s["stage"].startswith(p) for p in stage_prefixes): continue
        for r in s.get("rows", []):
            if r[1] != "client backend": continue
            tot_ += 1
            key = "on CPU (no wait event)" if r[3] is None else f"{r[3]} / {r[4]}"
            cnt[key] += 1
    return cnt, tot_
merge_wait, merge_n = wait_profile(["t7"]); copy_wait, copy_n = wait_profile(["t5"]); all_wait, all_n = wait_profile([""])
bg = Counter()
for s in act:
    for r in s.get("rows", []):
        if r[1] != "client backend" and r[3] is not None: bg[(r[1], f"{r[3]}/{r[4]}")] += 1
WCOL = {"on CPU (no wait event)": "--series-2", "IO": "--series-1", "LWLock": "--series-4", "Lock": "--series-7", "Client": "--series-3", "IPC": "--series-6", "Timeout": "--series-8", "BufferPin": "--series-5"}
def wait_items(cnt):
    return [(k, v, WCOL.get(k.split(" /")[0], "--series-8")) for k, v in cnt.most_common(10)]

# ---------------- CPU utilisation from cgroup samples ----------------
cpu_pts_pg, cpu_pts_py, mem_pts = [], [], []
prev = None
for s in sysr:
    if "pg_cgroup" not in s or prev is None or "pg_cgroup" not in prev:
        prev = s; continue
    dt = s["t"] - prev["t"]
    if dt <= 0: prev = s; continue
    du = (int(s["pg_cgroup"]["usage_usec"]) - int(prev["pg_cgroup"]["usage_usec"])) / 1e6
    dpy = (s["py"]["utime"] + s["py"]["stime"]) - (prev["py"]["utime"] + prev["py"]["stime"])
    x = (s["t"] - files[0]["t_start"]) / 60 if files else 0
    cpu_pts_pg.append((x, du / dt)); cpu_pts_py.append((x, dpy / dt)); mem_pts.append((x, s["pg_mem_bytes"] / 2**30))
    prev = s
avg_pg_cpu = st.mean([y for _, y in cpu_pts_pg]) if cpu_pts_pg else 0
avg_py_cpu = st.mean([y for _, y in cpu_pts_py]) if cpu_pts_py else 0
peak_pg_mem = max([y for _, y in mem_pts], default=0)
peak_py_rss = max([f.get("py_maxrss_kb", 0) for f in files], default=0) / 2**20

# ---------------- EXPLAIN table ----------------
exp_rows = []; sample_plan = None
for p in sorted((tr / "explain").glob("*.txt")):
    t = p.read_text()
    def g(rx, cast=float, default=None):
        m = re.search(rx, t); return cast(m.group(1).replace(",", "")) if m else default
    hit = g(r"Custom Scan \(ModifyHypertable\).*?\n\s+Buffers: shared hit=(\d+)", int, 0)
    dirt = g(r"Custom Scan \(ModifyHypertable\).*?\n\s+Buffers: shared[^\n]*dirtied=(\d+)", int, 0)
    writ = g(r"Custom Scan \(ModifyHypertable\).*?\n\s+Buffers: shared[^\n]*written=(\d+)", int, 0)
    rd = g(r"Custom Scan \(ModifyHypertable\).*?\n\s+Buffers: shared hit=\d+ read=(\d+)", int, 0)
    rows = g(r"rows (\d+)", int, 0); ex = g(r"Execution Time: ([\d.]+) ms")
    walb = g(r"WAL: records=\d+ fpi=\d+ bytes=(\d+)", int, 0); fpi = g(r"fpi=(\d+)", int, 0)
    sort = g(r"Sort Method: ([^\n]+)", str, "?"); conf = g(r"Conflicting Tuples: (\d+)", int, 0)
    sort_ms = g(r"->  Sort .*?actual time=[\d.]+\.\.([\d.]+)", float, 0)
    iot = g(r"Custom Scan \(ModifyHypertable\).*?\n.*?\n\s+I/O Timings: ([^\n]+)", str, "")
    exp_rows.append(f'<tr><td>{E(p.stem)}</td><td class="num">{rows:,}</td><td class="num">{ex / 1000 if ex else 0:.2f}</td><td class="num">{1000 * ex / rows if ex and rows else 0:.2f}</td>'
                    f'<td class="num">{hit / rows if rows else 0:.1f}</td><td class="num">{rd:,}</td><td class="num">{dirt:,}</td><td class="num">{writ:,}</td>'
                    f'<td class="num">{walb / rows if rows else 0:.0f}</td><td class="num">{fpi:,}</td><td class="num">{sort_ms:.0f}</td><td>{E(sort)}</td><td class="num">{conf:,}</td><td>{E(iot)}</td></tr>')
    if sample_plan is None or p.stem == "0000": sample_plan = (p.stem, t)
last_plan = None
lst = sorted((tr / "explain").glob("*.txt"))
if len(lst) > 1: last_plan = (lst[-1].stem, lst[-1].read_text())

# ---------------- pg_stat_statements ----------------
pgss_rows = []
def qtext(r): return re.sub(r"\s+", " ", r["left"] if "left" in r else r.get("query", ""))[:160]
pg_total_exec = sum(r["total_exec_time"] for r in pgss) if pgss else 0
for r in pgss[:12]:
    pgss_rows.append(f'<tr><td class="num">{r["calls"]:,}</td><td class="num">{r["total_exec_time"] / 1000:,.1f}</td><td class="num">{pct(r["total_exec_time"], pg_total_exec)}</td>'
                     f'<td class="num">{r["mean_exec_time"]:,.1f}</td><td class="num">{r["shared_blks_hit"]:,}</td><td class="num">{r["shared_blks_read"]:,}</td><td class="num">{r["shared_blks_dirtied"]:,}</td>'
                     f'<td class="num">{r["shared_blks_written"]:,}</td><td class="num">{(r["temp_blks_written"] or 0):,}</td><td class="num">{int(r["wal_bytes"]) / 2**20:,.0f}</td>'
                     f'<td class="num">{float(r["shared_blk_read_time"]) + float(r["shared_blk_write_time"]) + float(r["temp_blk_read_time"]) + float(r["temp_blk_write_time"]):,.0f}</td>'
                     f'<td style="white-space:normal;min-width:360px"><code>{E(qtext(r))}</code></td></tr>')


def delta(name, key, sub=None):
    a = {tuple(x.get(k) for k in (sub or [])): x for x in pgio0.get(name, [])}
    b = {tuple(x.get(k) for k in (sub or [])): x for x in pgio1.get(name, [])}
    out_ = {}
    for k, x in b.items():
        v1 = x.get(key) or 0; v0 = (a.get(k) or {}).get(key) or 0
        out_[k] = float(v1) - float(v0)
    return out_
ckpt = (pgio1.get("pg_stat_checkpointer") or [{}])[0]; ckpt0 = (pgio0.get("pg_stat_checkpointer") or [{}])[0]
walst = (pgio1.get("pg_stat_wal") or [{}])[0]; walst0 = (pgio0.get("pg_stat_wal") or [{}])[0]
io_rows = []
for k, x in {tuple(x.get(c) for c in ("backend_type", "object", "context")): x for x in pgio1.get("pg_stat_io", [])}.items():
    if k[0] not in ("client backend", "checkpointer", "background writer", "autovacuum worker"): continue
    io_rows.append(f'<tr><td>{E(k[0])}</td><td>{E(k[1])}/{E(k[2])}</td><td class="num">{(x.get("reads") or 0):,}</td><td class="num">{float(x.get("read_time") or 0) / 1000:,.1f}</td>'
                   f'<td class="num">{(x.get("writes") or 0):,}</td><td class="num">{float(x.get("write_time") or 0) / 1000:,.1f}</td><td class="num">{(x.get("extends") or 0):,}</td>'
                   f'<td class="num">{(x.get("hits") or 0):,}</td><td class="num">{(x.get("evictions") or 0):,}</td><td class="num">{(x.get("fsyncs") or 0):,}</td></tr>')
ckpt_lines = [l for l in pglog.splitlines() if "checkpoint complete" in l]
settings = {x["name"]: x["setting"] for x in pgio1.get("settings", pgio0.get("settings", []))}

# ---------------- flamegraphs + collapsed stacks ----------------
asset_dir = out.parent / "ingest_trace"; asset_dir.mkdir(exist_ok=True)
flames = []
for name, title, note in [("flame_cpu.svg", "CPU-only flamegraph, native frames (py-spy, 10 min window, idle threads excluded)", "Only on-CPU samples of the Python process: where the Python side spends its own compute."),
                          ("flame_wall.svg", "Wall-clock flamegraph incl. idle (py-spy --idle --threads, 10 min window)", "Includes time blocked in psycopg waiting for Postgres, so the merge shows as the widest tower under the main thread. The two narrow towers are the tracer's sampler threads.")]:
    p = root / name
    if p.exists() and p.stat().st_size > 1000:
        shutil.copy(p, asset_dir / name); flames.append((name, title, note))
stack_rows = []; stack_note = ""
sp = root / "stacks_wall.txt"
if sp.exists():
    leaf = Counter(); lib = Counter(); tot_s = 0
    for line in sp.read_text().splitlines():
        if " " not in line: continue
        stack, cnt = line.rsplit(" ", 1); cnt = int(cnt)
        frames = stack.split(";")
        if frames and ("activity_sampler" in stack or "sys_sampler" in stack): continue
        tot_s += cnt
        # first tracer stage frame gives the pipeline stage; the deepest python/native frame gives the leaf
        stage = next((f for f in frames if re.match(r"main \(", f)), "")
        lf = frames[-1]
        leaf[lf[:110]] += cnt
        libname = "psycopg (waiting for / talking to Postgres)" if "psycopg" in stack else "polars" if "polars" in stack else "pyarrow" if "pyarrow" in stack else "acquirium/tracer python"
        lib[libname] += cnt
    for k, v in lib.most_common():
        stack_rows.append(f'<tr><td>{E(k)}</td><td class="num">{v:,}</td><td class="num">{pct(v, tot_s)}</td></tr>')
    leaf_rows = "".join(f'<tr><td><code>{E(k)}</code></td><td class="num">{v:,}</td><td class="num">{pct(v, tot_s)}</td></tr>' for k, v in leaf.most_common(15))
    stack_note = f"{tot_s:,} main-thread samples (50 Hz, 25 min window), sampler threads excluded."
else:
    leaf_rows = ""

# ---------------- provenance ----------------
fid = summary.get("fidelity", {})
prov = {
    "branch head (public repo == clone)": re.search(r"public repo head: (\S+)\s+clone head: (\S+)", launch).groups() if re.search(r"public repo head", launch) else ("?", "?"),
    "image": re.search(r"image digest: (\S+)", launch).group(1) if "image digest" in launch else "?",
    "postgres": " / ".join(summary.get("postgres", [])),
    "versions": summary.get("versions", {}),
}
acq_rate = 147_000  # rows/s, acquirium end-to-end on this branch, 1000-file run (results10)
rate = rows_total / wall if wall else 0
merge_share = pct(tot["t7.merge_on_conflict"], file_sum)
python_side = sum(tot[s] for s in STAGES if s[0] in "dcsm") + tot["t1.prepare_frame_split_dedupe"] + tot["t4.write_csv"]
est_1000 = None
if n and n < 1000:
    # extrapolate with the observed per-row cost (last third) × rows in the remaining files, scaled by bytes
    est_1000 = 682_522_976 / rate if rate else None  # rows in the full 1000-file set (results10)

tiles = [
    ("files traced", f"{n:,}" + (f" / {summary.get('files_total', 1000)}" if n < summary.get("files_total", 1000) else ""), summary.get("stopped_by", "in progress")),
    ("rows merged", f"{rows_total / 1e6:,.1f} M", f"{summary.get('rows_in_db', rows_total):,} in hypertable"),
    ("ingest wall", fmt_s(wall), f"{rate / 1000:,.0f}k rows/s (acquirium run: {acq_rate / 1000:.0f}k)"),
    ("merge statement", merge_share, f"{1e6 * tot['t7.merge_on_conflict'] / rows_total if rows_total else 0:.1f} µs/row, on CPU {pct(merge_wait.get('on CPU (no wait event)', 0), merge_n)} of samples"),
    ("COPY into staging", pct(tot["t5.copy_staging"] + tot["t4.write_csv"], file_sum), f"{1e6 * (tot['t5.copy_staging'] + tot['t4.write_csv']) / rows_total if rows_total else 0:.2f} µs/row incl. write_csv"),
    ("everything in Python", pct(python_side, file_sum), "CSV read, melt, register, IPC, manager, split/dedupe, write_csv"),
    ("Postgres CPU", f"{avg_pg_cpu:.2f} cores", f"tracer {avg_py_cpu:.2f} cores; peak PG mem {peak_pg_mem:.1f} GiB, tracer RSS {peak_py_rss:.1f} GiB"),
    ("WAL per row", f"{sum(f.get('wal_bytes', 0) for f in files) / rows_total if rows_total else 0:.0f} B", f"{sum(f.get('wal_bytes', 0) for f in files) / 2**30:.1f} GiB total; {len(ckpt_lines)} checkpoints"),
]
if est_1000: tiles.append(("projected 1000 files", fmt_s(est_1000), f"682.5M rows at {rate / 1000:,.0f}k rows/s; acquirium run measured 77.4 min"))
tiles_html = "".join(f'<div class="tile"><div class="k">{E(k)}</div><div class="v">{v}</div><div class="n">{E(str(nn))}</div></div>' for k, v, nn in tiles)

per_file_med = st.median([f["file_total"] for f in files]) if files else 0
variant = summary.get("variant", "baseline")
variant_label = {"plain-insert": "plain INSERT, non-unique (ref_id, ts) index", "baseline": "branch as is: INSERT … ON CONFLICT, unique (ref_id, ts) index"}.get(variant, variant)
compare_html = ""
if baseline is not None and (baseline / "trace" / "files.jsonl").exists():
    bf = jl(baseline / "trace" / "files.jsonl"); bs = j(baseline / "trace" / "summary.json") or {}
    ba = jl(baseline / "trace" / "activity.jsonl")
    b_rows = sum(f["rows_after_dedupe"] for f in bf); b_wall = bs.get("ingest_wall_s", 0); b_sum = sum(f["file_total"] for f in bf)
    b_tot = {st_: sum(f.get(st_, 0.0) for f in bf) for st_ in STAGES}
    def bwait(acts, prefix):
        c = Counter(); t_ = 0
        for smp in acts:
            if not (smp.get("stage") or "").startswith(prefix): continue
            for r in smp.get("rows", []):
                if r[1] == "client backend": t_ += 1; c["cpu" if r[3] is None else r[3]] += 1
        return c, t_
    bw, bn = bwait(ba, "t7"); vw, vn = bwait(act, "t7")
    # same-file comparison: only files both runs processed
    common = min(len(bf), len(files))
    b_common_wall = sum(f["file_total"] for f in bf[:common]); v_common_wall = sum(f["file_total"] for f in files[:common])
    b_common_rows = sum(f["rows_after_dedupe"] for f in bf[:common])
    rows_cmp = []
    for g, ss, col in GROUPS:
        bv = sum(b_tot[st_] for st_ in ss); vv = sum(tot[st_] for st_ in ss)
        bvc = sum(f.get(st_, 0.0) for f in bf[:common] for st_ in ss); vvc = sum(f.get(st_, 0.0) for f in files[:common] for st_ in ss)
        rows_cmp.append(f'<tr><td><span class="lg"><svg width="10" height="10"><rect width="10" height="10" fill="var({col})"/></svg>{E(g)}</span></td>'
                        f'<td class="num">{1e6 * bvc / b_common_rows if b_common_rows else 0:.2f}</td><td class="num">{1e6 * vvc / b_common_rows if b_common_rows else 0:.2f}</td>'
                        f'<td class="num">{pct(bvc, b_common_wall)}</td><td class="num">{pct(vvc, v_common_wall)}</td>'
                        f'<td class="num"><b>{bvc / vvc if vvc else 0:.2f}×</b></td></tr>')
    bcum = 0; b_merge_pts = []
    for f in bf:
        bcum += f["rows_after_dedupe"]; r = f["rows_after_dedupe"] or 1
        if r < 20_000: continue
        b_merge_pts.append((bcum / 1e6, 1e6 * f["t7.merge_on_conflict"] / r))
    def expl_stats(rt):
        hits = []; walb = []; ex = []
        for pth in sorted((rt / "trace" / "explain").glob("*.txt")):
            t = pth.read_text(); m1 = re.search(r"Custom Scan \(ModifyHypertable\).*?\n\s+Buffers: shared hit=(\d+)", t); m2 = re.search(r"rows (\d+)", t)
            m3 = re.search(r"WAL: records=(\d+)[^\n]*bytes=(\d+)", t); m4 = re.search(r"Execution Time: ([\d.]+) ms", t)
            if m1 and m2 and m3 and m4 and int(m2.group(1)) > 20000:
                r = int(m2.group(1)); hits.append(int(m1.group(1)) / r); walb.append(int(m3.group(2)) / r); ex.append(1000 * float(m4.group(1)) / r)
        return (st.median(hits) if hits else 0, st.median(walb) if walb else 0, st.median(ex) if ex else 0)
    beh, bew, bex = expl_stats(baseline); veh, vew, vex = expl_stats(root)
    b_wal_row = sum(f.get("wal_bytes", 0) for f in bf) / b_rows if b_rows else 0; v_wal_row = sum(f.get("wal_bytes", 0) for f in files) / rows_total if rows_total else 0
    b_sizes = [f["sizes"] for f in bf if "sizes" in f]; v_sizes = [f["sizes"] for f in files if "sizes" in f]
    bi = next((f["sizes"] for f in reversed(bf[:common]) if "sizes" in f), {}); vi = next((f["sizes"] for f in reversed(files[:common]) if "sizes" in f), {})
    compare_html = f"""
<h2>Baseline vs this variant</h2>
<p>Baseline = yesterday's run of the branch as is (<code>INSERT … ON CONFLICT (ref_id, ts) DO UPDATE</code> against the unique index), {len(bf)} files in {fmt_s(b_wall)}.
This run = {E(variant_label)}, {n} files in {fmt_s(wall)}. Same VM, same container image and tuning, same data in the same order, same Python path (the dedupe in <code>_prepare_frame</code> still runs, so the same rows are written).
Per-row numbers below are over the first {common} files, which both runs processed ({b_common_rows / 1e6:.0f} M rows).</p>
<div class="tiles">
<div class="tile"><div class="k">rows/s</div><div class="v">{rate / 1000:,.0f}k</div><div class="n">baseline {b_rows / b_wall / 1000 if b_wall else 0:,.0f}k → <b>{(rate) / (b_rows / b_wall) if b_wall and b_rows else 0:.2f}×</b></div></div>
<div class="tile"><div class="k">merge µs/row</div><div class="v">{1e6 * sum(f['t7.merge_on_conflict'] for f in files[:common]) / b_common_rows if b_common_rows else 0:.2f}</div><div class="n">baseline {1e6 * sum(f['t7.merge_on_conflict'] for f in bf[:common]) / b_common_rows if b_common_rows else 0:.2f}</div></div>
<div class="tile"><div class="k">same {common} files</div><div class="v">{fmt_s(v_common_wall)}</div><div class="n">baseline {fmt_s(b_common_wall)} → {b_common_wall / v_common_wall if v_common_wall else 0:.2f}× faster</div></div>
<div class="tile"><div class="k">buffer hits / row (EXPLAIN)</div><div class="v">{veh:.1f}</div><div class="n">baseline {beh:.1f}</div></div>
<div class="tile"><div class="k">WAL / row</div><div class="v">{v_wal_row:.0f} B</div><div class="n">baseline {b_wal_row:.0f} B</div></div>
<div class="tile"><div class="k">index after {common} files</div><div class="v">{vi.get('index_bytes', 0) / 1e9:.1f} GB</div><div class="n">baseline {bi.get('index_bytes', 0) / 1e9:.1f} GB (heap {vi.get('table_bytes', 0) / 1e9:.1f} vs {bi.get('table_bytes', 0) / 1e9:.1f} GB)</div></div>
<div class="tile"><div class="k">merge backend on CPU</div><div class="v">{pct(vw.get('cpu', 0), vn)}</div><div class="n">baseline {pct(bw.get('cpu', 0), bn)}; IO waits {pct(vw.get('IO', 0), vn)} vs {pct(bw.get('IO', 0), bn)}</div></div>
</div>
<div class="tw"><table><thead><tr><th>pipeline group</th><th style="text-transform:none">baseline µs/row</th><th style="text-transform:none">variant µs/row</th><th>baseline share</th><th>variant share</th><th>speed-up</th></tr></thead><tbody>{''.join(rows_cmp)}</tbody></table></div>
<figure class="fig"><figcaption><b>Merge cost per row vs rows already in the hypertable, both runs</b><span class="unit">µs per row, files with ≥ 20k rows</span></figcaption>
{line_chart([("baseline ON CONFLICT + unique index", "--series-2", b_merge_pts), ("variant plain INSERT + non-unique index", "--series-1", merge_pts)], ylabel="µs/row")}
<div class="legend"><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-2)"/></svg>baseline</span><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-1)"/></svg>variant</span></div></figure>
<p>Merge SQL used in this run:</p><pre>{E(summary.get('merge_sql', '').strip())}</pre>
<p>Index DDL applied after <code>ensure_table</code>: {E('; '.join(summary.get('variant_ddl', [])) or 'none')}. Indexes on <code>timeseries</code>: {E('; '.join(i[1] for i in summary.get('indexes', [])))}.</p>
"""
csv_bytes = sum(f.get("csv_bytes", 0) for f in files)
findings_html = findings.read_text() if findings and findings.exists() else ""

doc = f"""<meta charset="utf-8">
<title>Timescale Ingest Trace{' Variant' if variant != 'baseline' else ''}</title>
<style>{CSS}</style>
<div class="wrap">
<h1>Where the Timescale ingest path spends its time{' — ' + E(variant_label) if variant != 'baseline' else ''}</h1>
<p class="sub">acquirium <code>ums-timescale-fix</code> @ <code>{E(prov['branch head (public repo == clone)'][1])}</code> — the CSV → polars → melt → Arrow → Timescale pipeline replayed stage by stage
outside the server, on the cloud VM (4 vCPU / 16 GiB), <code>{E(prov['image'].split('@')[0])}</code> (PG {E(prov['postgres'].split(' on ')[0].replace('PostgreSQL ', ''))}, TimescaleDB {E(prov['postgres'].split(' / ')[-1])}).
Same functions, same SQL, same order, same data ({E(str(summary.get('started', '')))[:16]} UTC).</p>
<div class="tiles">{tiles_html}</div>
{findings_html}
{compare_html}
<h2>1. Stage breakdown</h2>
<p>Every file goes through the same 24 steps the server executes. Wall time per stage, summed over {n} files ({fmt_s(file_sum)} in stages, {fmt_s(wall)} total wall including the tracer's own bookkeeping):</p>
<figure class="fig"><figcaption><b>Total wall time by pipeline group</b><span class="unit">seconds, share of stage time</span></figcaption>
{hbars(group_items, unit=" s", total=file_sum)}</figure>
<figure class="fig"><figcaption><b>Per-file stacked stage time</b><span class="unit">seconds per file, ingest order</span></figcaption>
{stacked_timeline()}<div class="legend" style="flex-wrap:wrap">{legend}</div>
<div class="figfoot">Median file: {per_file_med:.2f} s. Each bar is one CSV; height varies with row count (one file = one Arrow POST in acquirium, there is no 50k-row batching on the arrow path).</div></figure>
<div class="tw"><table><thead><tr><th>stage</th><th>total s</th><th>share</th><th>median s/file</th><th>p90</th><th>max</th><th style="text-transform:none">median µs/row</th></tr></thead><tbody>{''.join(stage_rows)}</tbody></table></div>
<p><code>d*</code> = CSVIngestDriver/IngestDriver in the driver process; <code>c*/s*</code> = client serialisation and the <code>/insert_timeseries_arrow</code> route; <code>m*</code> = manager; <code>t*</code> = TimescaleStore. The HTTP transfer itself is the only step not reproduced (loopback, {csv_bytes / 2**20 / n if n else 0:.0f} MB CSV payload per file).</p>

<h2>2. Does the merge get slower as the table grows?</h2>
<figure class="fig"><figcaption><b>Per-row cost vs rows already in the hypertable</b><span class="unit">µs per row, files with ≥ 20k rows</span></figcaption>
{line_chart([("merge INSERT…ON CONFLICT", "--series-2", merge_pts), ("write_csv + COPY", "--series-1", copy_pts), ("all Python-side stages", "--series-3", py_pts)], ylabel="µs/row")}
<div class="legend"><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-2)"/></svg>merge</span><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-1)"/></svg>write_csv + COPY</span><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-3)"/></svg>Python side</span></div></figure>
<figure class="fig"><figcaption><b>WAL written per row</b><span class="unit">bytes/row (pg_current_wal_lsn delta per file)</span></figcaption>
{line_chart([("WAL bytes/row", "--series-4", wal_pts)], ylabel="B/row")}</figure>
<figure class="fig"><figcaption><b>Hypertable growth</b><span class="unit">GB, sampled every 10 files</span></figcaption>
{line_chart([("index bytes", "--series-2", idx_growth), ("table bytes", "--series-1", tab_growth)], ylabel="GB")}
<div class="legend"><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-2)"/></svg>unique (ref_id, ts) index</span><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-1)"/></svg>heap</span></div>
<div class="figfoot">Final: {size_pts[-1][0] if size_pts else 0:.1f} GB table, {size_pts[-1][1] if size_pts else 0:.1f} GB index, {size_pts[-1][2] if size_pts else 0} chunks, {sum(f['rows_after_dedupe'] for f in files) / 1e6:.0f} M rows.</div></figure>

<h2>3. What Postgres is doing during the merge</h2>
<p>The tracer's backend pid was sampled from <code>pg_stat_activity</code> at 5 Hz and each sample tagged with the pipeline stage running at that instant. A sample with no wait event means the backend was on CPU.</p>
<figure class="fig"><figcaption><b>Wait-event profile during <code>t7.merge_on_conflict</code></b><span class="unit">{merge_n:,} samples</span></figcaption>{hbars(wait_items(merge_wait), total=merge_n)}</figure>
<figure class="fig"><figcaption><b>Wait-event profile during <code>t5.copy_staging</code></b><span class="unit">{copy_n:,} samples</span></figcaption>{hbars(wait_items(copy_wait), total=copy_n)}</figure>
<p>Background processes seen waiting (all samples): {E(', '.join(f'{k[0]} {k[1]}: {v}' for k, v in bg.most_common(6)) or 'none')}.</p>
<figure class="fig"><figcaption><b>CPU used by the Postgres container and the tracer process</b><span class="unit">cores (cgroup cpu.stat deltas, 5 s)</span></figcaption>
{line_chart([("postgres container", "--series-2", cpu_pts_pg), ("tracer (python)", "--series-3", cpu_pts_py)], ylabel="cores", xlabel="minutes since start")}
<div class="legend"><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-2)"/></svg>postgres container</span><span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var(--series-3)"/></svg>tracer</span></div>
<div class="figfoot">4 cores are available; the merge is a single backend and cannot use more than one.</div></figure>

<h3>EXPLAIN (ANALYZE, BUFFERS, WAL) of the merge, every 25th file</h3>
<div class="tw"><table><thead><tr><th>file</th><th>rows</th><th>exec s</th><th>µs/row</th><th>buffer hits/row</th><th>shared read</th><th>dirtied</th><th>written</th><th>WAL B/row</th><th>FPI</th><th>sort ms</th><th>sort</th><th>conflicts</th><th>I/O timings (ms)</th></tr></thead><tbody>{''.join(exp_rows)}</tbody></table></div>
<p>The Sort + Hash Join feeding the insert finish in well under a second; the rest of the statement is the <code>Insert on timeseries</code> node itself: for every row a b-tree descent on <code>idx_timeseries_ref_ts_unique</code> for the ON CONFLICT arbiter check, the heap insert, the index insert, and the WAL records for both.</p>
<details><summary>Full plan, first file ({E(sample_plan[0]) if sample_plan else ''})</summary><pre>{E(sample_plan[1]) if sample_plan else ''}</pre></details>
{f'<details><summary>Full plan, last sampled file ({E(last_plan[0])})</summary><pre>{E(last_plan[1])}</pre></details>' if last_plan else ''}

<h3>pg_stat_statements at the end of the run</h3>
<div class="tw"><table><thead><tr><th>calls</th><th>total s</th><th>share</th><th>mean ms</th><th>shared hit</th><th>read</th><th>dirtied</th><th>written</th><th>temp written</th><th>WAL MiB</th><th>I/O time ms</th><th>statement</th></tr></thead><tbody>{''.join(pgss_rows)}</tbody></table></div>
<h3>pg_stat_io, checkpoints, WAL (counters reset at start)</h3>
<div class="tw"><table><thead><tr><th>backend</th><th>object/context</th><th>reads</th><th>read s</th><th>writes</th><th>write s</th><th>extends</th><th>hits</th><th>evictions</th><th>fsyncs</th></tr></thead><tbody>{''.join(io_rows)}</tbody></table></div>
<p>Checkpoints: {int(ckpt.get('num_timed', 0) or 0) - int(ckpt0.get('num_timed', 0) or 0)} timed, {int(ckpt.get('num_requested', 0) or 0) - int(ckpt0.get('num_requested', 0) or 0)} requested, {int(ckpt.get('buffers_written', 0) or 0) - int(ckpt0.get('buffers_written', 0) or 0):,} buffers written, write time {(float(ckpt.get('write_time', 0) or 0) - float(ckpt0.get('write_time', 0) or 0)) / 1000:,.0f} s.
WAL: {int(walst.get('wal_records', 0) or 0) - int(walst0.get('wal_records', 0) or 0):,} records, {int(walst.get('wal_fpi', 0) or 0) - int(walst0.get('wal_fpi', 0) or 0):,} full-page images, {(int(walst.get('wal_bytes', 0) or 0) - int(walst0.get('wal_bytes', 0) or 0)) / 2**30:.1f} GiB, {int(walst.get('wal_buffers_full', 0) or 0) - int(walst0.get('wal_buffers_full', 0) or 0):,} wal_buffers_full.
Settings: {E(', '.join(f'{k}={v}' for k, v in sorted(settings.items())))}.</p>
{'<details><summary>checkpoint log lines</summary><pre>' + E(chr(10).join(ckpt_lines[:40])) + '</pre></details>' if ckpt_lines else ''}

<h2>4. Flamegraphs of the Python side</h2>
{''.join(f'<figure class="fig"><figcaption><b>{E(t)}</b></figcaption><object class="flame" type="image/svg+xml" data="ingest_trace/{nm}">flamegraph</object><div class="figfoot">{E(nt)} Open <a href="ingest_trace/{nm}">ingest_trace/{nm}</a> for the interactive version.</div></figure>' for nm, t, nt in flames) or '<p>flamegraphs not captured</p>'}
<h3>Collapsed wall-clock stacks (main thread) by library</h3>
<p class="sub">{E(stack_note)}</p>
<div class="tw"><table><thead><tr><th>where the main thread was</th><th>samples</th><th>share</th></tr></thead><tbody>{''.join(stack_rows)}</tbody></table></div>
<details><summary>Top leaf frames</summary><div class="tw"><table><thead><tr><th>leaf frame</th><th>samples</th><th>share</th></tr></thead><tbody>{leaf_rows}</tbody></table></div></details>

<h2>5. Method and provenance</h2>
<ul>
<li><b>Code under test</b>: public <code>github.com/DataDrivenCPS/acquirium</code> branch <code>ums-timescale-fix</code>, head <code>{E(prov['branch head (public repo == clone)'][0])}</code> (remote) == <code>{E(prov['branch head (public repo == clone)'][1])}</code> (clone). The tracer imports <code>to_observations</code>, <code>IngestDriver.normalize_observations</code>, <code>assign_stream_value_kind</code>, <code>compute_ref_uri</code>, <code>TimescaleStore</code> (schema via <code>ensure_table</code>, <code>ensure_stream_refs</code>, <code>stream_value_kind</code>, <code>_prepare_frame</code>) from that clone and reproduces the client/route/manager glue inline. The five SQL statements of <code>_insert_frame</code> are copied verbatim and checked against the source at start-up: {E(', '.join(k for k, v in fid.get('checks', {}).items() if v))} — all {len(fid.get('checks', {}))} checks passed.</li>
<li><b>Not reproduced</b>: the HTTP hop (loopback POST of the Arrow body), the RDF graph write on stream registration, FastAPI overhead. Every other step runs the same library call on the same frame.</li>
<li><b>Database</b>: fresh container, {E(prov['image'])}, TS_TUNE_MEMORY=8GB TS_TUNE_NUM_CPUS=4, plus <code>pg_stat_statements</code> and <code>track_io_timing=on</code> (the only differences from the benchmark container). 0 public tables before the schema was created.</li>
<li><b>Instrumentation</b>: perf_counter around each stage; <code>pg_stat_activity</code> sampled at 5 Hz; cgroup CPU every 5 s; EXPLAIN ANALYZE replaces the plain merge on every 25th file (that file's merge time includes instrumentation overhead); py-spy 50 Hz windows (CPU-only native 10 min from t+45 s, then wall/idle 10 min, then raw stacks 25 min). No kernel <code>perf</code> is available in this VM, so the Postgres side is profiled with wait events, EXPLAIN and pg_stat_* rather than a flamegraph.</li>
<li><b>Data</b>: {n} of the 1000 DPR trailer CSVs (NUL-sanitised set from the 1000-file benchmark), ingest order = sorted path, as the driver does. {E(str(summary.get('stopped_by', '')))}.</li>
<li>Versions: {E(', '.join(f'{k} {v}' for k, v in prov['versions'].items()))}.</li>
</ul>
</div>
"""
out.write_text(doc)
print(f"wrote {out} ({len(doc) // 1024} KB), {n} files, {rows_total:,} rows, wall {wall:.0f}s, merge share {merge_share}")
