"""Render the sync-vs-async merge end-to-end bench: make_report_async.py <bench dir> <out.html> [findings.html]"""
from __future__ import annotations

import html
import json
import sys
from datetime import datetime
from pathlib import Path

bench = Path(sys.argv[1]); out = Path(sys.argv[2]); findings = Path(sys.argv[3]) if len(sys.argv) > 3 else None
HERE = Path(__file__).resolve().parent
CSS = (HERE.parent / "report.css").read_text() + "\n:root{--series-4:#8e5bd1;--series-5:#c9a300}\n.legend{flex-wrap:wrap}\n"
E = html.escape
MODES = [
    ("sync", "main as is: merge inside the request, route on the event loop", "--series-2"),
    ("async", "async_merge, route on the event loop", "--series-1"),
    ("sync-thread", "sync merge, route call in the threadpool", "--series-5"),
    ("async-thread", "async_merge + route call in the threadpool", "--series-3"),
]


def j(p): return json.loads(p.read_text()) if p.exists() else None


R = {m: {"result": j(bench / m / "result.json"), "probes": j(bench / m / "probes.json") or [],
         "inserted": j(bench / m / "inserted.json") or [], "merged": j(bench / m / "merged.json") or []} for m, _, _ in MODES}


def fmt_s(x): return f"{x:,.0f} s" if x < 600 else f"{x / 60:,.1f} min"


def line_chart(series, w=940, h=260, xlabel="", ylog=False, ymax_cap=None):
    pts = [p for _, _, ps in series for p in ps]
    if not pts: return ""
    left, right, top, bot = 56, 12, 10, 36
    pw, ph = w - left - right, h - top - bot
    xmin, xmax = min(p[0] for p in pts), max(p[0] for p in pts); xmax = xmax if xmax > xmin else xmin + 1
    ymax = max(p[1] for p in pts) * 1.08 or 1
    if ymax_cap: ymax = min(ymax, ymax_cap)
    X = lambda x: left + pw * (x - xmin) / (xmax - xmin)
    Y = lambda y: top + ph - ph * min(y, ymax) / ymax
    parts = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" style="max-width:100%">']
    for k in range(5):
        yv = ymax * k / 4
        parts.append(f'<line x1="{left}" y1="{Y(yv):.1f}" x2="{w - right}" y2="{Y(yv):.1f}" stroke="var(--grid)"/>'
                     f'<text x="{left - 6}" y="{Y(yv) + 4:.1f}" font-size="11" text-anchor="end" fill="var(--text-muted)">{yv:,.0f}</text>')
    for k in range(6):
        xv = xmin + (xmax - xmin) * k / 5
        parts.append(f'<text x="{X(xv):.1f}" y="{h - 16}" font-size="11" text-anchor="middle" fill="var(--text-muted)">{xv:.1f}</text>')
    for label, col, ps in series:
        d = " ".join(f"{'M' if i == 0 else 'L'}{X(x):.1f},{Y(y):.1f}" for i, (x, y) in enumerate(ps))
        parts.append(f'<path d="{d}" fill="none" stroke="var({col})" stroke-width="1.4" opacity="0.9"><title>{E(label)}</title></path>')
    parts.append(f'<text x="{left + pw / 2}" y="{h - 3}" font-size="11" text-anchor="middle" fill="var(--text-muted)">{E(xlabel)}</text></svg>')
    return "".join(parts)


def legend(items): return '<div class="legend">' + "".join(f'<span class="lg"><svg width="12" height="12"><rect width="12" height="12" fill="var({c})"/></svg>{E(l)}</span>' for l, c in items) + "</div>"


rows = []; tiles = []; probe_series = {"timeseries_1d": [], "health": []}; cadence = []
for m, label, col in MODES:
    r = R[m]["result"]
    if not r: continue
    ins = R[m]["inserted"]
    start = datetime.fromisoformat(r["started"]).timestamp()
    for name in probe_series:
        ps = [((p["t"] - start) / 60, p["ms"]) for p in R[m]["probes"] if p["probe"] == name and p["t"] >= start]
        probe_series[name].append((f"{m}", col, ps))
    cadence.append((m, col, [((x["wall"] - start) / 60, i + 1) for i, x in enumerate(ins)]))
    pt, ph = r["probe_timeseries_1d"], r["probe_health"]
    rows.append(f'<tr><td><span class="lg"><svg width="10" height="10"><rect width="10" height="10" fill="var({col})"/></svg>{E(label)}</span></td>'
                f'<td class="num">{r["files"]}</td><td class="num">{r["rows_pushed"] / 1e6:.1f} M</td><td class="num">{fmt_s(r["driver_wall_s"])}</td>'
                f'<td class="num">{r["rows_per_s_driver"] / 1000:,.0f}k</td><td class="num">{fmt_s(r["merge_tail_s"])}</td><td class="num">{fmt_s(r["total_s"])}</td><td class="num">{r["rows_per_s_total"] / 1000:,.0f}k</td>'
                f'<td class="num">{pt.get("p50_ms", 0):,.0f}</td><td class="num">{pt.get("p99_ms", 0):,.0f}</td><td class="num">{pt.get("max_ms", 0):,.0f}</td><td class="num">{pt.get("over_1s", 0)}/{pt.get("n", 0)}</td>'
                f'<td class="num">{ph.get("p50_ms", 0):,.0f}</td><td class="num">{ph.get("max_ms", 0):,.0f}</td><td class="num">{r["rows_in_db"]:,}</td></tr>')

rs = R["sync"]["result"]
ra = R["async-thread"]["result"] or R["async"]["result"]
ra_label = "async-thread" if R["async-thread"]["result"] else "async"
if rs and ra:
    tiles = [
        (f"driver-observed ingest, {ra_label} vs sync", f"{ra['rows_per_s_driver'] / rs['rows_per_s_driver']:.2f}×", f"{ra['rows_per_s_driver'] / 1000:,.0f}k vs {rs['rows_per_s_driver'] / 1000:,.0f}k rows/s"),
        ("time to fully merged", f"{rs['total_s'] / ra['total_s']:.2f}×", f"{fmt_s(ra['total_s'])} vs {fmt_s(rs['total_s'])} for {ra['files']} files"),
        ("merge tail after last file", fmt_s(ra["merge_tail_s"]), f"sync: {fmt_s(rs['merge_tail_s'])}"),
        ("1-day read during ingest, p99", f"{ra['probe_timeseries_1d'].get('p99_ms', 0):,.0f} ms", f"sync: {rs['probe_timeseries_1d'].get('p99_ms', 0):,.0f} ms; max {ra['probe_timeseries_1d'].get('max_ms', 0):,.0f} vs {rs['probe_timeseries_1d'].get('max_ms', 0):,.0f} ms"),
        ("reads slower than 1 s", f"{ra['probe_timeseries_1d'].get('over_1s', 0)} of {ra['probe_timeseries_1d'].get('n', 0)}", f"sync: {rs['probe_timeseries_1d'].get('over_1s', 0)} of {rs['probe_timeseries_1d'].get('n', 0)}"),
        ("/health p50 during ingest", f"{ra['probe_health'].get('p50_ms', 0):,.0f} ms", f"sync: {rs['probe_health'].get('p50_ms', 0):,.0f} ms (max {rs['probe_health'].get('max_ms', 0):,.0f})"),
        ("rows in db", f"{ra['rows_in_db']:,}", f"sync: {rs['rows_in_db']:,}"),
    ]
tiles_html = "".join(f'<div class="tile"><div class="k">{E(k)}</div><div class="v">{v}</div><div class="n">{E(n)}</div></div>' for k, v, n in tiles)
findings_html = findings.read_text() if findings and findings.exists() else ""
lg = legend([(m, c) for m, _, c in MODES])

doc = f"""<meta charset="utf-8">
<title>Async Merge Ingest Bench</title>
<style>{CSS}</style>
<div class="wrap">
<h1>Timescale async merge: does the driver stop waiting for the merge?</h1>
<p class="sub">acquirium main + <code>timescale_async_merge</code> (working tree, branch <code>ums-async-merge</code>), server + DPR CSV driver on the cloud VM (4 vCPU / 16 GiB),
fresh <code>timescale/timescaledb:latest-pg17</code> per run, {rs['files'] if rs else '?'} DPR files. A probe hit <code>GET /health</code> and a 1-day <code>GET /timeseries</code> window once a second while the driver ingested.</p>
<div class="tiles">{tiles_html}</div>
{findings_html}
<h2>Summary</h2>
<div class="tw"><table><thead><tr><th>mode</th><th>files</th><th>rows</th><th>driver wall</th><th>rows/s (driver)</th><th>merge tail</th><th>total</th><th>rows/s (total)</th><th>read p50 ms</th><th>read p99</th><th>read max</th><th>reads &gt; 1 s</th><th>health p50</th><th>health max</th><th>rows in db</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<p>"driver wall" is from <code>acquirium driver start</code> returning to the last "inserted" line in the server log; "merge tail" is how long after that the last staged batch was merged (0 for sync, where the request only returns after the merge); "total" is until every row is queryable.</p>
<h2>Read latency during ingest</h2>
<p>In the first two runs (event-loop route) the read probe addressed a stream by a URI the server did not know, so it returned an empty stream without touching the database; those two read series only show event-loop wait. <code>/health</code> is a threadpool route whose response still has to be written by the event loop, so its latency is the time the loop was blocked. The threaded-route runs probe <code>urn:dpr/feed-pump-out-flow-rate</code>, a registered point, but the window (2024-08-16) precedes the data's first timestamp (2024-08-20), so those reads also return an empty stream after the store lookup; what they measure is how long a small read waits on the store's connection.</p>
<figure class="fig"><figcaption><b>GET /timeseries, 1-day window of one stream</b><span class="unit">ms per request (capped at 30 s), minutes since driver start</span></figcaption>
{line_chart(probe_series["timeseries_1d"], xlabel="minutes since driver start", ymax_cap=30000)}{lg}</figure>
<figure class="fig"><figcaption><b>GET /health</b><span class="unit">ms per request</span></figcaption>
{line_chart(probe_series["health"], xlabel="minutes since driver start", ymax_cap=30000)}{lg}</figure>
<h2>Ingest cadence</h2>
<figure class="fig"><figcaption><b>Files inserted (driver's view) over time</b><span class="unit">count</span></figcaption>
{line_chart([(m, c, ps) for m, c, ps in cadence], xlabel="minutes since driver start")}{lg}</figure>
<h2>Method</h2>
<ul>
<li>Same server, driver config and data as the backend benchmark (<code>insert_batch_rows = 50000</code>, one Arrow POST per file). The only difference between the two runs is <code>timescale_async_merge = true</code> in server.toml.</li>
<li>Async mode: the request path COPYs the batch into an UNLOGGED per-batch table and returns; one worker thread merges batches in order on its own connection (queue depth 2, back-pressure by blocking the next COPY). <code>flush()</code> waits for the queue; <code>close()</code> drains it; leftover batch tables are merged at the next startup.</li>
<li>Probe latencies are wall-clock from a thread in the bench process on the same VM. "route on the event loop" runs are main's <code>/insert_timeseries_arrow</code> (an async handler calling the store directly); "route call in the threadpool" runs hand that call to <code>asyncio.to_thread</code>.</li>
</ul>
</div>
"""
out.write_text(doc)
print(f"wrote {out}: " + "; ".join(f"{m}: {R[m]['result']['rows_per_s_driver']:,.0f} rows/s driver, total {R[m]['result']['total_s']:.0f}s" for m, _, _ in MODES if R[m]["result"]))
