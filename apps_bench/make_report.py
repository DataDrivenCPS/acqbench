#!/usr/bin/env python3
"""Render the app-path benchmark results into a local HTML report."""

from __future__ import annotations

import html
import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
RES = HERE / "results"
OUT = HERE.parent / "reports" / "apps_branch_bench.html"

M_COLOR, B_COLOR = "var(--series-1)", "var(--series-2)"


def load(name):
    p = RES / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else {}


def fmt(v, nd=1, suffix=""):
    if v is None:
        return "—"
    if isinstance(v, str):
        return html.escape(v)
    return f"{v:,.{nd}f}{suffix}"


def ratio(main_v, branch_v, lower_is_better=True):
    if not main_v or not branch_v:
        return "—", ""
    if lower_is_better:
        if branch_v < main_v:
            return f"branch {main_v/branch_v:.1f}×", "win-b"
        return f"main {branch_v/main_v:.1f}×", "win-m"
    if branch_v > main_v:
        return f"branch {branch_v/main_v:.1f}×", "win-b"
    return f"main {main_v/branch_v:.1f}×", "win-m"


# ------------------------------------------------------------------ charts
def grouped_bar(series, categories, title, unit, note=""):
    """Two-series grouped bar chart as inline SVG. series = [(label,color,values)]"""
    W, H = 720, 300
    ml, mr, mt, mb = 70, 16, 16, 42
    pw, ph = W - ml - mr, H - mt - mb
    allv = [v for _, _, vals in series for v in vals if v]
    if not allv:
        return ""
    vmax = max(allv) * 1.18
    n = len(categories)
    group_w = pw / n
    bar_w = min(34, group_w / (len(series) + 1))

    def y(v):
        return mt + ph - (v / vmax) * ph if vmax else mt + ph

    parts = [f'<svg viewBox="0 0 {W} {H}" class="chart" role="img" '
             f'aria-label="{html.escape(title)}">']
    for i in range(5):
        v = vmax * i / 4
        yy = y(v)
        parts.append(f'<line x1="{ml}" x2="{W-mr}" y1="{yy:.1f}" y2="{yy:.1f}" class="grid"/>')
        parts.append(f'<text x="{ml-8}" y="{yy+4:.1f}" class="tick tick-y">{v:,.0f}</text>')
    for gi, cat in enumerate(categories):
        gx = ml + gi * group_w
        for si, (label, color, vals) in enumerate(series):
            v = vals[gi]
            if v is None:
                continue
            bx = gx + group_w / 2 - (len(series) * bar_w) / 2 + si * bar_w
            by, bh = y(v), (mt + ph) - y(v)
            parts.append(
                f'<rect x="{bx+1:.1f}" y="{by:.1f}" width="{bar_w-2:.1f}" '
                f'height="{max(bh,0):.1f}" rx="4" fill="{color}">'
                f'<title>{html.escape(label)} @ {cat}: {v:,.1f}{unit}</title></rect>')
            if bh > 24:
                lab = f"{v:,.0f}" if v >= 10 else f"{v:,.1f}"
                parts.append(f'<text x="{bx+bar_w/2:.1f}" y="{by-5:.1f}" class="barlab">{lab}</text>')
        parts.append(f'<text x="{gx+group_w/2:.1f}" y="{mt+ph+20:.1f}" class="tick tick-x">{cat}</text>')
    parts.append(f'<line x1="{ml}" x2="{W-mr}" y1="{mt+ph}" y2="{mt+ph}" class="axis"/>')
    parts.append("</svg>")
    legend = " ".join(
        f'<span class="lg"><i style="background:{c}"></i>{html.escape(l)}</span>'
        for l, c, _ in series)
    cap = f'<span class="unit">{html.escape(unit)}</span>'
    foot = f'<p class="figfoot">{note}</p>' if note else ""
    return (f'<figure class="fig"><figcaption><b>{html.escape(title)}</b>{cap}</figcaption>'
            f'<div class="legend">{legend}</div>{"".join(parts)}{foot}</figure>')


def main() -> int:
    reg_m, reg_b = load("register_main"), load("register_branch")
    ser_m, ser_b = load("register_serial_main"), load("register_serial_branch")
    std_m, std_b = load("steady_main"), load("steady_branch")
    ovr_m, ovr_b = load("overrun_main"), load("overrun_branch")
    env_b, env_m = load("envspec_branch"), load("envspec_main")

    rm = {c["n"]: c for c in reg_m["result"]["counts"]}
    rb = {c["n"]: c for c in reg_b["result"]["counts"]}
    # The 50-app count of the escalation run overlapped a macOS idle sleep, so
    # it was re-measured on its own, twice per ref, under caffeinate. Those runs
    # replace it everywhere; the escalation's own 50-row is discarded.
    r50 = {ref: [load(f"register50_{ref}_r{rep}")["result"]["counts"][0] for rep in (1, 2)]
           for ref in ("main", "branch")}
    warm50_branch = rb.get(50)
    rm[50] = r50["main"][0]
    rb[50] = r50["branch"][0]
    counts = sorted(set(rm) | set(rb))
    sm, sb = std_m["result"], std_b["result"]
    om, ob = ovr_m["result"], ovr_b["result"]

    # ---------- registration table + charts
    reg_rows = []
    for n in counts:
        a, b = rm.get(n, {}), rb.get(n, {})

        def cell(c, key, sub=None, nd=1, suffix=""):
            v = c.get(key)
            if isinstance(v, dict) and sub:
                v = v.get(sub)
            return fmt(v, nd, suffix)

        a_ok = f'{a.get("completed", 0)}/{n}'
        b_ok = f'{b.get("completed", 0)}/{n}'
        a_cls = "warn" if a.get("completed", 0) < n else ""
        b_cls = "warn" if b.get("completed", 0) < n else ""
        reg_rows.append(f"""<tr>
  <td class="num">{n}</td>
  <td class="num {a_cls}">{a_ok}</td><td class="num {b_cls}">{b_ok}</td>
  <td class="num">{cell(a,'all_registered_s',nd=1,suffix=' s')}</td>
  <td class="num">{cell(b,'all_registered_s',nd=1,suffix=' s')}</td>
  <td class="num">{cell(a,'probe','apps_list') if not isinstance(a.get('probe'),dict) else fmt(a['probe']['apps_list'].get('p95_ms'),0,' ms')}</td>
  <td class="num">{fmt(b['probe']['apps_list'].get('p95_ms'),0,' ms') if b else '—'}</td>
  <td class="num">{fmt(a['probe']['health'].get('p95_ms'),0,' ms') if a else '—'}</td>
  <td class="num">{fmt(b['probe']['health'].get('p95_ms'),0,' ms') if b else '—'}</td>
</tr>""")

    chart_applist = grouped_bar(
        [("main", M_COLOR, [rm[n]["probe"]["apps_list"]["p95_ms"] if n in rm else None for n in counts]),
         ("ums-apps-improve", B_COLOR, [rb[n]["probe"]["apps_list"]["p95_ms"] if n in rb else None for n in counts])],
        [f"{n} apps" for n in counts],
        "/apps/list p95 while N registrations are in flight", "ms (lower is better)",
        note="main's 60,000 ms bars are the probe's own timeout — the true value is "
             "unbounded. At 50 the server never answered again, in either repetition.")

    chart_regtime = grouped_bar(
        [("main", M_COLOR, [rm[n]["all_registered_s"] if n in rm else None for n in counts]),
         ("ums-apps-improve", B_COLOR, [rb[n]["all_registered_s"] if n in rb else None for n in counts])],
        [f"{n} apps" for n in counts],
        "Wall clock until every registration returned", "s (lower is better)",
        note="main's 50-app bar is the 300 s budget expiring with 0 of 50 returned, "
             "not a completed run.")

    # ---------- steady state
    steady_rows = [
        ("Ticks delivered per second", sm["ticks_per_s"], sb["ticks_per_s"], False, 2, ""),
        ("Tick period p50", sm["period_ms"]["p50_ms"], sb["period_ms"]["p50_ms"], True, 0, " ms"),
        ("Tick period p95", sm["period_ms"]["p95_ms"], sb["period_ms"]["p95_ms"], True, 0, " ms"),
        ("App run duration p50", sm["run_ms"]["p50_ms"], sb["run_ms"]["p50_ms"], True, 0, " ms"),
        ("Server read inside a run p50", sm["read_ms"]["p50_ms"], sb["read_ms"]["p50_ms"], True, 0, " ms"),
        ("In-app compute p95", sm["compute_ms"]["p95_ms"], sb["compute_ms"]["p95_ms"], True, 1, " ms"),
        ("/graph_version requests per second", sm["server_requests_per_s"].get("/graph_version"),
         sb["server_requests_per_s"].get("/graph_version"), True, 2, ""),
        ("/apps/list p95", sm["probe"]["apps_list"]["p95_ms"], sb["probe"]["apps_list"]["p95_ms"], True, 1, " ms"),
        ("/health p50", sm["probe"]["health"]["p50_ms"], sb["probe"]["health"]["p50_ms"], True, 1, " ms"),
        ("Server-tree CPU, mean", sm["resources"]["cpu_pct_mean"], sb["resources"]["cpu_pct_mean"], True, 0, " %"),
        ("Server-tree RSS, peak", sm["resources"]["rss_mb_peak"], sb["resources"]["rss_mb_peak"], True, 0, " MB"),
    ]
    steady_html = []
    for label, a, b, lower, nd, suf in steady_rows:
        txt, cls = ratio(a, b, lower)
        steady_html.append(
            f'<tr><td>{label}</td><td class="num">{fmt(a,nd,suf)}</td>'
            f'<td class="num">{fmt(b,nd,suf)}</td><td class="{cls}">{txt}</td></tr>')

    chart_steady = grouped_bar(
        [("main", M_COLOR, [sm["server_requests_per_s"].get("/graph_version"),
                            sm["period_ms"]["p95_ms"] / 10, sm["read_ms"]["p50_ms"] / 10,
                            sm["probe"]["apps_list"]["p95_ms"] / 10]),
         ("ums-apps-improve", B_COLOR, [sb["server_requests_per_s"].get("/graph_version"),
                                        sb["period_ms"]["p95_ms"] / 10, sb["read_ms"]["p50_ms"] / 10,
                                        sb["probe"]["apps_list"]["p95_ms"] / 10])],
        ["/graph_version req/s", "tick period p95 ÷10 ms",
         "run read p50 ÷10 ms", "/apps/list p95 ÷10 ms"],
        "Steady state: 25 apps on a 1 s interval", "mixed units, all lower-is-better",
        note="Latencies are divided by 10 so they share one axis with the request rate.")

    # ---------- overrun
    ovr_rows = [
        ("Runs dispatched in the 60 s window", om["runs_recorded"], ob["runs_recorded"], True, 0, ""),
        ("Ticks the app actually completed", om["ticks_received"], ob["ticks_received"], True, 0, ""),
        ("Peak concurrent runs", om["peak_live_runs"], ob["peak_live_runs"], True, 0, ""),
        ("Ticks skipped (reported by the app)", om.get("skipped"), ob.get("skipped"), False, 0, ""),
        ("Server-tree RSS growth", om["resources"]["rss_mb_peak"] - om["resources"]["rss_mb_start"],
         ob["resources"]["rss_mb_peak"] - ob["resources"]["rss_mb_start"], True, 0, " MB"),
        ("Server-tree CPU, mean", om["resources"]["cpu_pct_mean"], ob["resources"]["cpu_pct_mean"], True, 0, " %"),
    ]
    ovr_html = []
    for label, a, b, lower, nd, suf in ovr_rows:
        txt, cls = ratio(a, b, lower) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else ("—", "")
        ovr_html.append(
            f'<tr><td>{label}</td><td class="num">{fmt(a,nd,suf)}</td>'
            f'<td class="num">{fmt(b,nd,suf)}</td><td class="{cls}">{txt}</td></tr>')

    rt_m = om.get("run_timeout", {})
    rt_b = ob.get("run_timeout", {})
    envr = env_b.get("result", {})
    envm = env_m.get("result", {})

    rerun_rows = []
    for ref, label in (("main", "main"), ("branch", "ums-apps-improve")):
        for rep, c in enumerate(r50[ref], start=1):
            rerun_rows.append(
                f'<tr><td>{label}</td><td class="num">rep {rep}</td>'
                f'<td class="num {"warn" if c["completed"] < 50 else ""}">{c["completed"]}/50</td>'
                f'<td class="num">{fmt(c["all_registered_s"],1," s")}</td>'
                f'<td class="num">{fmt(c["probe"]["apps_list"]["p95_ms"],1," ms")}</td>'
                f'<td class="num">{fmt(c["probe"]["health"]["p95_ms"],1," ms")}</td>'
                f'<td class="num">{fmt(c["clock_gap_s"],1," s")}</td>'
                f'<td class="{"warn" if not c["server_alive_after_teardown"] else "ok"}">'
                f'{"dead" if not c["server_alive_after_teardown"] else "alive"}</td></tr>')
    branch_err = (r50["branch"][0].get("errors") or [""])[0]
    branch_err = branch_err.split("response body: ")[-1][:220]

    sm_ser = ser_m["result"]["counts"][0]
    sb_ser = ser_b["result"]["counts"][0]

    doc = f"""<meta charset="utf-8">
<title>App Path Bench</title>
<style>
:root {{
  color-scheme: light;
  --surface-0:#f5f5f3; --surface-1:#fcfcfb; --surface-2:#efefec;
  --border:#dedcd6; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#7b7a75;
  --series-1:#2a78d6; --series-2:#eb6834;
  --good:#0b7a4b; --bad:#b3261e; --grid:#e6e5e0;
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    color-scheme: dark;
    --surface-0:#121211; --surface-1:#1a1a19; --surface-2:#232322;
    --border:#3a3a37; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#94938b;
    --series-1:#3987e5; --series-2:#d95926;
    --good:#43c08a; --bad:#e66767; --grid:#2e2e2b;
  }}
}}
:root[data-theme="dark"] {{
  color-scheme: dark;
  --surface-0:#121211; --surface-1:#1a1a19; --surface-2:#232322;
  --border:#3a3a37; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#94938b;
  --series-1:#3987e5; --series-2:#d95926;
  --good:#43c08a; --bad:#e66767; --grid:#2e2e2b;
}}
* {{ box-sizing:border-box; }}
body {{ background:var(--surface-0); color:var(--text-primary); margin:0;
  font:15px/1.6 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }}
.wrap {{ max-width:1000px; margin:0 auto; padding:40px 22px 80px; }}
h1 {{ font-size:29px; margin:0 0 6px; letter-spacing:-.02em; }}
h2 {{ font-size:20px; margin:44px 0 12px; letter-spacing:-.01em;
  padding-bottom:7px; border-bottom:1px solid var(--border); }}
h3 {{ font-size:15px; margin:26px 0 8px; color:var(--text-secondary); }}
.sub {{ color:var(--text-secondary); }}
p {{ color:var(--text-secondary); }}
code {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12.5px;
  background:var(--surface-2); padding:1px 5px; border-radius:4px; color:var(--text-primary); }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; margin:22px 0 8px; }}
.tile {{ background:var(--surface-1); border:1px solid var(--border); border-radius:11px; padding:14px 16px; }}
.tile .k {{ font-size:11.5px; text-transform:uppercase; letter-spacing:.06em; color:var(--text-muted); }}
.tile .v {{ font-size:25px; font-weight:640; margin-top:5px; letter-spacing:-.02em; }}
.tile .n {{ font-size:12.5px; color:var(--text-secondary); margin-top:3px; }}
.tw {{ overflow-x:auto; margin:14px 0; }}
table {{ border-collapse:collapse; width:100%; font-size:13.5px; background:var(--surface-1);
  border:1px solid var(--border); border-radius:10px; }}
th,td {{ padding:8px 11px; text-align:left; border-bottom:1px solid var(--border); white-space:nowrap; }}
th {{ font-size:11.5px; text-transform:uppercase; letter-spacing:.05em;
  color:var(--text-muted); font-weight:600; background:var(--surface-2); }}
tr:last-child td {{ border-bottom:none; }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
.win-b {{ color:var(--series-2); font-weight:600; }}
.win-m {{ color:var(--series-1); font-weight:600; }}
.ok {{ color:var(--good); }} .warn {{ color:var(--bad); font-weight:600; }}
.fig {{ margin:20px 0 8px; background:var(--surface-1); border:1px solid var(--border);
  border-radius:11px; padding:14px 16px 8px; }}
figcaption {{ display:flex; justify-content:space-between; align-items:baseline;
  gap:12px; margin-bottom:2px; flex-wrap:wrap; }}
figcaption b {{ font-size:14.5px; }}
.unit {{ color:var(--text-muted); font-size:12px; }}
.figfoot {{ font-size:12px; color:var(--text-muted); margin:6px 2px 4px; white-space:normal; }}
.legend {{ display:flex; gap:16px; margin:6px 0 4px; font-size:12.5px; color:var(--text-secondary); }}
.lg {{ display:flex; align-items:center; gap:6px; }}
.lg i {{ width:11px; height:11px; border-radius:3px; display:inline-block; }}
.chart {{ width:100%; height:auto; display:block; overflow:visible; }}
.grid {{ stroke:var(--grid); stroke-width:1; }}
.axis {{ stroke:var(--border); stroke-width:1; }}
.tick {{ fill:var(--text-muted); font-size:11px; }}
.tick-y {{ text-anchor:end; }} .tick-x {{ text-anchor:middle; }}
.barlab {{ fill:var(--text-secondary); font-size:10.5px; text-anchor:middle;
  font-variant-numeric:tabular-nums; }}
pre.code {{ background:var(--surface-2); border:1px solid var(--border); border-radius:8px;
  padding:11px 14px; overflow-x:auto; font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
  font-size:12.5px; color:var(--text-primary); margin:12px 0; }}
.note {{ background:var(--surface-1); border-left:3px solid var(--series-1);
  border-radius:0 8px 8px 0; padding:11px 15px; margin:16px 0; font-size:13.5px; }}
.note.bad {{ border-left-color:var(--bad); }}
.note.good {{ border-left-color:var(--good); }}
ul {{ color:var(--text-secondary); padding-left:20px; }}
li {{ margin:4px 0; }}
</style>
<div class="wrap">
<h1>App path — <code>main</code> vs <code>ums-apps-improve</code></h1>
<p class="sub">Both refs run their own <code>acquirium server</code> (DuckDB backend, warm
embedding cache, one at a time) on this laptop, on a throwaway port. Apps are registered and
run through the HTTP API from the ref's own client, so each branch exercises its native
path; every number below comes from the same app source and the same trigger receiver.</p>

<div class="tiles">
  <div class="tile"><div class="k">50 concurrent registrations</div>
    <div class="v warn">main: server dead</div>
    <div class="n">0 of 50 returned in 300 s; <code>/health</code> stopped answering.
      Branch: {r50['branch'][0]['completed']} of 50, server responsive throughout.</div></div>
  <div class="tile"><div class="k">/apps/list p95 during a 25-app storm</div>
    <div class="v">{fmt(rb[25]['probe']['apps_list']['p95_ms'],1,' ms')}</div>
    <div class="n">branch · main ≥60,000 ms (probe timeout)</div></div>
  <div class="tile"><div class="k">Server polls, 25 apps @ 1 s</div>
    <div class="v">{fmt(sb['server_requests_per_s'].get('/graph_version'),2)}/s</div>
    <div class="n">branch · main {fmt(sm['server_requests_per_s'].get('/graph_version'),2)}/s
      of <code>/graph_version</code></div></div>
  <div class="tile"><div class="k">Slow app, 5 s run on a 1 s tick</div>
    <div class="v">{ob['runs_recorded']} runs</div>
    <div class="n">branch, {ob.get('skipped')} ticks skipped · main {om['runs_recorded']} runs,
      up to {om['peak_live_runs']} at once</div></div>
</div>

<h2>1 · Concurrent registration, and what it does to the rest of the API</h2>
<p>Each row registers N apps at once from N client threads and starts them keep-alive, while a
background probe hits <code>/health</code> and <code>/apps/list</code> every 250 ms.
<code>/apps/list</code> takes the app supervisor's record lock; <code>/health</code> takes no
lock but shares the same bounded request threadpool — so the two together separate "the lock is
held" from "the whole server is gone".</p>

<div class="tw"><table>
<tr><th rowspan="2">Apps</th><th colspan="2">Registered</th><th colspan="2">Wall clock</th>
    <th colspan="2">/apps/list p95</th><th colspan="2">/health p95</th></tr>
<tr><th>main</th><th>branch</th><th>main</th><th>branch</th>
    <th>main</th><th>branch</th><th>main</th><th>branch</th></tr>
{''.join(reg_rows)}
</table></div>

{chart_applist}
{chart_regtime}

<div class="note bad"><b>main deadlocks at 50 concurrent registrations.</b>
Not "slows down" — stops. None of the 50 requests returned inside the 300 s budget, the probe's
<code>/health</code> calls started timing out too, and the server never recovered: it had to be
killed. <code>AppSupervisor.register_app</code> holds <code>_lock</code> across
<code>ray.get(actor.register)</code>, and that actor call re-enters the same server over HTTP.
Every sync endpoint runs on one bounded threadpool, so once enough registration requests are
parked on the lock there is no thread left to serve the callback the lock-holder is waiting for.
This reproduced twice — once during registration, once while deleting 50 apps.</div>

<div class="note good"><b>The branch's lock split works.</b> Registration reserves the app name
with a placeholder and serializes only the build on a separate <code>_build_lock</code> that no
request path takes. The visible effect is dramatic: <code>/apps/list</code> p95 stays between
{fmt(min(rb[n]['probe']['apps_list']['p95_ms'] for n in rb),1)} and
{fmt(max(rb[n]['probe']['apps_list']['p95_ms'] for n in rb),1)} ms at every count, versus
main's climb to the 60 s probe ceiling by 25 apps.</div>

<h3>The control: the same 10 apps, one at a time</h3>
<p>Serial registration removes contention entirely, so what is left is the per-app cost and
whatever the lock blocks anyway.</p>
<div class="tw"><table>
<tr><th>Metric</th><th>main</th><th>ums-apps-improve</th><th></th></tr>
<tr><td>Wall clock for 10 apps</td><td class="num">{fmt(sm_ser['all_registered_s'],1,' s')}</td>
    <td class="num">{fmt(sb_ser['all_registered_s'],1,' s')}</td>
    <td class="{ratio(sm_ser['all_registered_s'], sb_ser['all_registered_s'])[1]}">{ratio(sm_ser['all_registered_s'], sb_ser['all_registered_s'])[0]}</td></tr>
<tr><td>Registration p50</td><td class="num">{fmt(sm_ser['register_ms']['p50_ms'],0,' ms')}</td>
    <td class="num">{fmt(sb_ser['register_ms']['p50_ms'],0,' ms')}</td>
    <td class="{ratio(sm_ser['register_ms']['p50_ms'], sb_ser['register_ms']['p50_ms'])[1]}">{ratio(sm_ser['register_ms']['p50_ms'], sb_ser['register_ms']['p50_ms'])[0]}</td></tr>
<tr><td>/apps/list p95 during it</td><td class="num">{fmt(sm_ser['probe']['apps_list']['p95_ms'],0,' ms')}</td>
    <td class="num">{fmt(sb_ser['probe']['apps_list']['p95_ms'],1,' ms')}</td>
    <td class="{ratio(sm_ser['probe']['apps_list']['p95_ms'], sb_ser['probe']['apps_list']['p95_ms'])[1]}">{ratio(sm_ser['probe']['apps_list']['p95_ms'], sb_ser['probe']['apps_list']['p95_ms'])[0]}</td></tr>
</table></div>
<p>Even with one registration in flight, main parks <code>/apps/list</code> for seconds — the
lock is held for the whole build, not just the dict update. The branch's faster registration is
partly the dropped <code>resolve_dependencies</code> pass (main runs an extra
<code>resolved_nodes()</code> SPARQL per query at registration; the branch removed
<code>depends_on</code> entirely).</p>

<div class="note bad"><b>New on the branch: most of a 50-way registration storm fails with HTTP 400.</b>
The supervisor builds an <code>Acquirium(...)</code> client <em>inside</em> the request handler,
and that constructor health-checks the server it is already running in
(<code>_wait_for_server</code>, 3 s read timeout, 60 s budget, over <code>localhost</code>).
Under 50-way concurrency that self-call times out and registration fails with a misleading
<code>ConnectionError: … did not answer /health within 60s … Is the server running?</code>.
On a cold server only {r50['branch'][0]['completed']}–{r50['branch'][1]['completed']} of 50
survive it. The server stays healthy, so this is a bounded failure rather than main's total
one — but it is a real regression risk at scale, and the error text points at the wrong thing.
Numbers below.</div>

<h3>The 50-app case, re-measured</h3>
<p>The first pass at 50 apps overlapped a macOS idle sleep, so it was thrown away and run
again on its own — two repetitions per ref, each on a freshly booted server, the whole thing
wrapped in <code>caffeinate -dimsu</code>. Each cell also records the gap between its wall
clock and its monotonic clock, which is how a sleep would show up (macOS pauses the monotonic
clock while suspended, so a slept run looks <em>fast</em>, not slow). All four gaps are zero.</p>

<div class="tw"><table>
<tr><th>Ref</th><th>Rep</th><th>Registered</th><th>Wall clock</th>
    <th>/apps/list p95</th><th>/health p95</th><th>Clock gap</th><th>Server after</th></tr>
{''.join(rerun_rows)}
</table></div>

<p><b>main reproduces exactly.</b> Both repetitions: 0 of 50 returned inside 300 s, both probe
endpoints timing out, teardown unable to remove a single app, server dead afterwards. Three
observations of the same deadlock now, including the discarded one.</p>

<p><b>The branch's failure rate is worse than the first pass suggested.</b> On a cold server
only {r50['branch'][0]['completed']} and {r50['branch'][1]['completed']} of 50 registrations
succeed; the earlier {warm50_branch['completed']}/50 came at the end of an escalation, on a
server whose Ray workers were already warm from the 1/10/25 counts, so each registration
finished well inside the 60 s self-check budget. Cold, the storm is slow enough that the
supervisor's own health check gives up. The client-side error is now captured verbatim:</p>
<pre class="code">{html.escape(branch_err)}</pre>
<p>What makes it self-inflicted: each of the 50 in-flight registrations builds its own
<code>Acquirium</code>, which polls <code>/health</code> every 3 s for up to 60 s — so the
failing requests generate the very load that starves the endpoint they are waiting on. Note the
server itself stays fine throughout: <code>/apps/list</code> p95 is
{fmt(r50['branch'][0]['probe']['apps_list']['p95_ms'],1,' ms')} and all 50 apps tear down in
{fmt(r50['branch'][0]['teardown_s'],1,' s')}.</p>

<h2>2 · Steady state: 25 apps on a 1 s interval</h2>
<p>All 25 apps online first, then a 90 s measurement window. Everything here is measured inside
that window; the ramp is excluded.</p>

<div class="tw"><table>
<tr><th>Metric</th><th>main</th><th>ums-apps-improve</th><th>Winner</th></tr>
{''.join(steady_html)}
</table></div>

{chart_steady}

<p><b>Where the polling went.</b> On main every app calls <code>graph_status()</code> on the
server <em>once per tick</em>, so the poll rate is the fleet's tick rate: {fmt(sm['server_requests_per_s'].get('/graph_version'),2)} req/s
for 25 apps at 1 s. The branch floors graph polling at <code>max(interval, 10 s)</code>,
independent of how fast the app runs: {fmt(sb['server_requests_per_s'].get('/graph_version'),2)} req/s.
That is a whole request per tick removed from a five-request tick — and it is the request that
sat in front of the run on the actor's own thread.</p>

<p><b>And the tick grid holds.</b> main's loop waits <code>interval</code> <em>after</em> each
run finishes, so the period is interval + run time and drifts:
p95 {fmt(sm['period_ms']['p95_ms'],0,' ms')} against a 1,000 ms request. The branch's
<code>IntervalScheduler</code> schedules from the previous deadline, so p95 is
{fmt(sb['period_ms']['p95_ms'],0,' ms')}.</p>

<h2>3 · When a run outlasts its interval</h2>
<p>One app, 1 s interval, each run sleeping 5 s, for 60 s. A tick can only be honoured every
5 s; the question is what the runtime does with the other four.</p>

<div class="tw"><table>
<tr><th>Metric</th><th>main</th><th>ums-apps-improve</th><th>Winner</th></tr>
{''.join(ovr_html)}
</table></div>

<div class="note bad"><b>main piles up.</b> It dispatched {om['runs_recorded']} runs for an app
that can complete about 12, running up to {om['peak_live_runs']} concurrently and leaving
{om['run_status_counts'].get('running', 0)} still in flight when the window closed. Nothing
bounds this: a slow app on a fast interval keeps spawning Ray tasks. The branch dispatched
{ob['runs_recorded']}, skipped {ob.get('skipped')} ticks and counted them, and never had more
than {ob['peak_live_runs']} run in flight.</div>

<h3>run_timeout</h3>
<p>Branch only. A deliberately hung run (<code>time.sleep(3600)</code>, no cancellation point)
with <code>run_timeout=3</code>:
recorded <b>{rt_b.get('status_counts', {}).get('timeout', 0)} timeouts</b> in
{fmt(rt_b.get('observed_s'),0,' s')} and kept skipping ticks ({rt_b.get('skipped')} skipped)
instead of stalling. On main the parameter does not exist:
<code>{fmt(rt_m.get('error'))}</code> — a hung run there is unrecoverable short of killing the app.</p>

<h2>4 · EnvSpec (branch only)</h2>
<p>An app declaring <code>pip: ["cowsay==6.1"]</code> gets a persistent overlay venv under
<code>&lt;data_dir&gt;/envs/&lt;fingerprint&gt;</code>, handed to Ray as
<code>py_executable</code>. Undeclared apps keep the inherit-the-server-env path and pay nothing.</p>
<div class="tw"><table>
<tr><th>Case</th><th>register</th><th>first tick after run</th></tr>
<tr><td>No env declared (baseline)</td><td class="num">{fmt(envr.get('no_env',{}).get('register_s'),2,' s')}</td><td class="num">—</td></tr>
<tr><td>Cold: env built for the first time</td><td class="num">{fmt(envr.get('cold',{}).get('register_s'),2,' s')}</td><td class="num">{fmt(envr.get('cold',{}).get('first_tick_s'),2,' s')}</td></tr>
<tr><td>Warm: same fingerprint reused</td><td class="num">{fmt(envr.get('warm',{}).get('register_s'),2,' s')}</td><td class="num">{fmt(envr.get('warm',{}).get('first_tick_s'),2,' s')}</td></tr>
</table></div>
<p>For one small pure-Python wheel the cold build costs about
{fmt((envr.get('cold',{}).get('register_s') or 0) - (envr.get('no_env',{}).get('register_s') or 0),1,' s')}
over a plain registration and the warm path about
{fmt((envr.get('warm',{}).get('register_s') or 0) - (envr.get('no_env',{}).get('register_s') or 0),1,' s')}.
A real dependency set (idaes, watertap) will be minutes, not seconds — the thing that matters is
that it is paid once and survives restarts, and that it happens outside both supervisor locks so
a cold build cannot stall other registrations. On main the parameter does not exist:
<code>{fmt(envm.get('error'))}</code>.</p>

<h2>5 · What this adds up to</h2>
<ul>
<li><b>The branch fixes a server-killing bug.</b> 50 concurrent registrations deadlock main
permanently; the branch survives them with the rest of the API answering in single-digit
milliseconds. That alone justifies the change.</li>
<li><b>Registration itself is not faster under load.</b> Both refs serialize the build — the
branch just moved it to a lock nothing else takes. Registering 25 apps still takes
{fmt(rm[25]['all_registered_s'],0,' s')} (main) / {fmt(rb[25]['all_registered_s'],0,' s')} (branch),
and per-app p50 grows linearly with N on both. If registration throughput matters, that build
lock is the next thing to attack.</li>
<li><b>Steady-state server load drops by the poll rate.</b> One fewer request per app per tick,
{fmt((sm['server_requests_per_s'].get('/graph_version') or 0) / (sb['server_requests_per_s'].get('/graph_version') or 1),1)}×
less <code>/graph_version</code> traffic, and app runs come back
{ratio(sm['run_ms']['p50_ms'], sb['run_ms']['p50_ms'])[0]} faster at p50.</li>
<li><b>Overrun protection is real and cheap.</b> No pile-up, no unbounded Ray task growth, and
the skip counter is visible in <code>/apps/list</code>.</li>
<li><b>One new problem to fix before merge:</b> the in-handler <code>Acquirium(...)</code>
self-health-check that fails roughly two thirds of a cold 50-way registration storm. It should not health-check the
server it runs inside, and it should not reach itself through <code>localhost</code>.</li>
</ul>

<h2>Caveats</h2>
<ul>
<li>One laptop, one run per cell, no repetitions — treat 10-20% differences as noise. The
findings above are all order-of-magnitude or categorical (dead / not dead).</li>
<li>Apps are read-only and trivial (one bound query, ~18 ms of local compute). Nothing here says
how a real soft sensor behaves; it isolates runtime overhead, not app work.</li>
<li>The registration storm is deliberately hostile — N clients registering simultaneously.
Deployments that register apps from a config file at boot hit the serial path instead, where
main survives but still blocks <code>/apps/list</code> for seconds per app.</li>
<li>50 apps was the agreed ceiling for this machine; neither ref's actual app-count limit was
probed.</li>
<li>Teardown of 50 apps also wedged main once (no timeout on the stop/deregister actor calls;
the branch bounds them at 10 s and then kills). It is reported here as an observation, not a
measured comparison — the harness stops hammering a wedged server rather than characterising it.</li>
</ul>
</div>"""

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(doc)
    print(f"wrote {OUT} ({len(doc):,} chars)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
