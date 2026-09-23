"""Render reports/startup_breakdown.html from startup_bench/results/.

  python make_report.py [results_dir] [out.html]
"""
from __future__ import annotations

import json
import re
import statistics as st
import sys
from collections import defaultdict
from itertools import product
from pathlib import Path

HERE = Path(__file__).resolve().parent
# argv: out.html  label=layout_dir ...   each layout_dir holds one subdir per ref (REFS)
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "reports" / "startup_breakdown.html"
SETS = [a.split("=", 1) for a in sys.argv[2:]] or [["Paired", str(HERE / "results" / "paired")],
                                                    ["Parallel ×4", str(HERE / "results" / "parallel")]]
REFS = [("main", "main", "d894861"), ("lri", "local-runtime-init", "a9d4f65"),
        ("emb", "ums-embedding-improvement", "84b1c7a"),
        ("emb2", "ums-embedding-improvement-2", "441dc16")]

SCENARIOS = [
    ("cold_emb", "Cold · embeddings", "#1"),
    ("cold_exact", "Cold · exact_only", "#2"),
    ("warm_emb__embdir", "Warm · embeddings · dir with embeddings", "#3 = #5a"),
    ("warm_exact__embdir", "Warm · exact_only · dir with embeddings", "#4a"),
    ("warm_exact__exactdir", "Warm · exact_only · dir without embeddings", "#4b"),
    ("warm_emb__exactdir", "Warm · embeddings · dir without embeddings", "#5b"),
]

# Coarse groups for the stacked chart (<= 8, fixed order = fixed colour).
GROUPS = [
    "Python start + imports",
    "Store open (DuckDB + Oxigraph RocksDB)",
    "Ontology parse (rdflib)",
    "Ontology → Oxigraph (ontoenv.add)",
    "Named-graph copy + concept extraction",
    "Embedding model (download + load)",
    "Embedding compute + cache",
    "Other (ontoenv connect, lifespan, uvicorn, tracer)",
]

POST_GROUPS = ["ray.init", "Driver actor create", "Driver setup (ray.get)", "Supervisor / other"]


def _has(path: list[str], *suffixes: str) -> bool:
    return any(sfx in p for p in path for sfx in suffixes)


def group_of(path: list[str]) -> int:
    if path[0].startswith("import ") or path[0] == "python start" \
            or any(p.startswith("lazy import") for p in path):
        return 0
    if _has(path, "create_timeseries_store", "._open_dataset"):
        return 1
    if _has(path, "load_bundled_graph"):
        return 2
    if _has(path, "ontoenv.env.add"):
        return 3
    if any("ModelManagement" in p or "TextEmbedding" in p or "OnnxTextEmbedding" in p
           or "InferenceSession" in p or "snapshot_download" in p for p in path) \
            or _has(path, "._ensure_model"):
        return 5
    if _has(path, "._embed", "._try_load_cache", "._save_cache", "._concepts_hash"):
        return 6
    if _has(path, ".named_graph", "._extract_concepts_for_embedding", ".extract_concepts",
            ".select_in_graphs", ".copy_graphs", "select_values", ".concept_query"):
        return 4
    return 7


def post_group_of(path: list[str]) -> int:
    if _has(path, "ray.init"):
        return 0
    if _has(path, "ray.ActorClass._remote", "ray.ActorClass.options"):
        return 1
    if _has(path, "ray.get"):
        return 2
    return 3


# wrapper frames that add nothing to a path label
_HIDDEN = {"uvicorn.Server.startup (async)", "manager.Manager.from_env"}


def load_run(d: Path) -> dict | None:
    meta_p = d / "meta.json"
    if not meta_p.exists():
        return None
    meta = json.loads(meta_p.read_text())
    run = {"meta": meta, "samples": json.loads((d / "samples.json").read_text())}
    sp = d / "spans.jsonl"
    if sp.exists():
        spans, events, logs = [], [], []
        for line in sp.open():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if "name" in r:
                spans.append(r)
            elif "ev" in r:
                events.append(r)
            elif "log" in r:
                logs.append(r)
        run["spans"], run["events"], run["logs"] = spans, events, logs
        agg = d / "aggregates.json"
        run["agg"] = json.loads(agg.read_text()) if agg.exists() else {}
    return run


def build_tree(spans: list[dict]) -> list[dict]:
    """Nest spans of one thread by interval containment."""
    items = sorted(spans, key=lambda r: (r["t0"], -(r["t1"] - r["t0"])))
    roots, stack = [], []
    for r in items:
        node = {"name": r["name"], "t0": r["t0"], "t1": r["t1"], "kids": []}
        while stack and not (r["t0"] >= stack[-1]["t0"] and r["t1"] <= stack[-1]["t1"]):
            stack.pop()
        (stack[-1]["kids"] if stack else roots).append(node)
        stack.append(node)
    return roots


def walk_self(node, path, out, lo, hi):
    """Yield (path, self_ns) clipped to [lo, hi]."""
    a, b = max(node["t0"], lo), min(node["t1"], hi)
    if b <= a:
        return
    covered = 0
    for k in node["kids"]:
        ka, kb = max(k["t0"], a), min(k["t1"], b)
        if kb > ka:
            covered += kb - ka
            walk_self(k, path + [k["name"]], out, lo, hi)
    out.append((path, (b - a) - covered))


def analyse(run: dict) -> dict:
    m = run["meta"]
    t_launch = m["t_launch"]
    health = t_launch + int(m["marks"]["health"] * 1e9)
    drv = m["marks"].get("drivers_started")
    res = {"health": m["marks"]["health"], "drivers": drv, "stop": m.get("stop_s"),
           "probes": {p["label"]: p for p in m["probes"]}, "sizes": m["sizes"],
           "exit": m.get("exit_code"), "prep": m.get("prep_copy_s"),
           "load": m.get("loadavg_start"), "host": m.get("host")}
    s = run["samples"]
    res["peak_rss"] = max((x["srv_rss"] + x["ray_rss"]) for x in s) if s else None
    res["peak_srv_rss"] = max(x["srv_rss"] for x in s) if s else None
    res["peak_cg"] = max((x["cg_mem"] or 0) for x in s) if s else None
    pre = [x for x in s if x["t"] <= m["marks"]["health"]]
    if pre:
        res["cpu_to_health"] = pre[-1]["srv_cpu"] + pre[-1]["ray_cpu"]
    if "spans" not in run:
        return res

    ev = {e["ev"]: e for e in run["events"]}
    t_interp = ev["interp_start"]["t_interp"]
    by_thread = defaultdict(list)
    for r in run["spans"]:
        by_thread[r["thread"]].append(r)
    main = build_tree(by_thread["MainThread"])

    # --- startup: launch -> health on MainThread
    groups = [0.0] * len(GROUPS)
    steps = defaultdict(lambda: [0, 0])  # path -> [self_ns, incl_ns]
    groups[0] += t_interp - t_launch
    steps[("python start",)][0] += t_interp - t_launch
    covered = 0
    for root in main:
        out = []
        walk_self(root, [root["name"]], out, t_interp, health)
        for path, ns in out:
            groups[group_of(path)] += ns
            steps[tuple(path)][0] += ns
            covered += ns
    gap = (health - t_interp) - covered
    groups[7] += gap
    steps[("(untraced: uvicorn serve loop / event loop)",)][0] += gap
    res["groups_s"] = [g / 1e9 for g in groups]

    # inclusive times for the step table
    def incl(node, path):
        a, b = max(node["t0"], t_interp), min(node["t1"], health)
        if b > a:
            steps[tuple(path)][1] += b - a
        for k in node["kids"]:
            incl(k, path + [k["name"]])
    for root in main:
        incl(root, [root["name"]])
    steps[("python start",)][1] = t_interp - t_launch
    steps[("(untraced: uvicorn serve loop / event loop)",)][1] = gap
    res["steps"] = {" › ".join(p for p in k if p not in _HIDDEN): [v[0] / 1e6, v[1] / 1e6]
                    for k, v in steps.items()}

    # --- post-health driver startup (asyncio_* threads)
    post = [0.0] * len(POST_GROUPS)
    post_steps = defaultdict(lambda: [0, 0])
    for th, sps in by_thread.items():
        if not th.startswith("asyncio"):
            continue
        for root in build_tree(sps):
            if "start_driver" not in root["name"] and "_deploy_config_apps" not in root["name"]:
                continue
            out = []
            walk_self(root, [root["name"]], out, root["t0"], root["t1"])
            for path, ns in out:
                post[post_group_of(path)] += ns
                post_steps[" › ".join(path)][0] += ns

            def pincl(node, path):
                post_steps[" › ".join(path)][1] += node["t1"] - node["t0"]
                for k in node["kids"]:
                    pincl(k, path + [k["name"]])
            pincl(root, [root["name"]])
    res["post_s"] = [p / 1e9 for p in post]
    res["post_steps"] = {k: [v[0] / 1e6, v[1] / 1e6] for k, v in post_steps.items()}

    # --- flame data (all threads, from launch until end of probes)
    t_end = t_launch + int((max(p["start"] + p["ms"] / 1e3 for p in m["probes"]) + 0.5) * 1e9)
    flame = []
    thread_names = sorted(by_thread, key=lambda t: (t != "MainThread", min(r["t0"] for r in by_thread[t])))
    lanes = []
    for th in thread_names:
        rows = []

        def emit(node, depth, path):
            dur = node["t1"] - node["t0"]
            if dur < 2e5 or node["t0"] > t_end:  # < 0.2 ms
                return
            rows.append([round((node["t0"] - t_launch) / 1e6, 2), round(dur / 1e6, 2), depth,
                         node["name"], group_of(path) if th == "MainThread" and node["t0"] < health else
                         (post_group_of(path) + 8 if th.startswith("asyncio") else 9)])
            for k in node["kids"]:
                emit(k, depth + 1, path + [k["name"]])
        for root in build_tree(by_thread[th]):
            emit(root, 0, [root["name"]])
        if rows:
            lanes.append({"thread": th, "rows": rows})
    res["flame"] = {"lanes": lanes, "end_ms": (t_end - t_launch) / 1e6,
                    "interp_ms": (t_interp - t_launch) / 1e6}
    res["logs"] = [[round((l["t"] - t_launch) / 1e6, 1), l["lvl"], l["lg"].replace("acquirium.", ""), l["log"][:160]]
                   for l in run["logs"] if l["lvl"] != "DEBUG" or l["log"].startswith("←")][:1500]
    res["agg"] = run["agg"]
    return res


def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None


def build_set(ROOT: Path) -> list[dict]:
    out = []
    for (key, label, tag), (ref, ref_name, ref_sha) in product(SCENARIOS, REFS):
        sdir = ROOT / ref / key
        if not sdir.exists():
            continue
        traced, plain = [], []
        for d in sorted(sdir.iterdir()):
            run = load_run(d)
            if run is None:
                continue
            a = analyse(run)
            a["rep"] = d.name
            (traced if d.name.startswith("traced") else plain).append((a, run))
        if not traced:
            continue
        T = [a for a, _ in traced]
        P = [a for a, _ in plain]
        # the representative run for flame/resources = the traced run with median /health
        order = sorted(T, key=lambda a: a["health"])
        rep = order[len(order) // 2]
        n = len(T)
        groups_mean = [sum(a["groups_s"][i] for a in T) / n for i in range(len(GROUPS))]
        post_mean = [sum(a["post_s"][i] for a in T) / n for i in range(len(POST_GROUPS))]
        step_keys = set().union(*(a["steps"] for a in T))
        steps = {k: [sum(a["steps"].get(k, [0, 0])[0] for a in T) / n,
                     sum(a["steps"].get(k, [0, 0])[1] for a in T) / n] for k in step_keys}
        pkeys = set().union(*(a["post_steps"] for a in T))
        post_steps = {k: [sum(a["post_steps"].get(k, [0, 0])[0] for a in T) / n,
                          sum(a["post_steps"].get(k, [0, 0])[1] for a in T) / n] for k in pkeys}
        probe_labels = [p for p in rep["probes"]]
        probes = {lab: {"ms": med([a["probes"][lab]["ms"] for a in T if lab in a["probes"]]),
                        "all": [a["probes"][lab]["ms"] for a in T if lab in a["probes"]],
                        "summary": rep["probes"][lab]["summary"]} for lab in probe_labels}
        agg_names = set().union(*(a["agg"] for a in T))
        agg = {k: {"calls": med([a["agg"].get(k, {}).get("calls") for a in T]),
                   "total_ms": med([a["agg"].get(k, {}).get("total_ns", 0) / 1e6 for a in T]),
                   "self_ms": med([a["agg"].get(k, {}).get("self_ns", 0) / 1e6 for a in T])}
               for k in agg_names}
        rep_run = next(r for a, r in traced if a is rep)
        samples = rep_run["samples"]
        rs = []
        for i, x in enumerate(samples):
            cores = None
            j = i
            while j > 0 and x["t"] - samples[j]["t"] < 1.0:
                j -= 1
            prev = samples[j]
            if j < i and x["cg_cpu"] is not None and x["t"] > prev["t"]:
                cores = (x["cg_cpu"] - prev["cg_cpu"]) / (x["t"] - prev["t"])
            rs.append([round(x["t"], 2), None if cores is None else round(cores, 3),
                       round(x["srv_rss"] / 2**20, 1), round(x["ray_rss"] / 2**20, 1),
                       round((x["cg_mem"] or 0) / 2**20, 1)])
        out.append({
            "key": key, "label": label, "tag": tag, "n": n, "n_plain": len(P),
            "ref": ref, "ref_name": ref_name, "ref_sha": ref_sha,
            "health": [a["health"] for a in T], "health_plain": [a["health"] for a in P],
            "drivers": [a["drivers"] for a in T], "stop": [a["stop"] for a in T],
            "drivers_plain": [a["drivers"] for a in P],
            "exit": sorted({a["exit"] for a in T + P}),
            "cpu_to_health": [a.get("cpu_to_health") for a in T],
            "peak_rss": [a["peak_rss"] for a in T], "peak_srv_rss": [a["peak_srv_rss"] for a in T],
            "peak_cg": [a["peak_cg"] for a in T],
            "groups": groups_mean, "post": post_mean, "steps": steps, "post_steps": post_steps,
            "probes": probes, "sizes": rep["sizes"], "agg": agg,
            "rep": rep["rep"], "flame": rep["flame"], "logs": rep["logs"], "res": rs,
            "rep_health": rep["health"], "rep_drivers": rep["drivers"],
            "hosts": sorted({a["host"] for a in T + P}),
            "load": [a["load"] for a in T + P],
            "prep": med([a["prep"] for a in T]),
        })
    return out


def import_summary(files: list[Path]) -> dict:
    imports = []
    for f in files:
        mods = {}
        for line in f.read_text().splitlines():
            mm = re.match(r"import time:\s+(\d+) \|\s+(\d+) \|(\s*)(\S+)", line)
            if mm:
                mods.setdefault(mm[4], [0, 0])
                mods[mm[4]][0] += int(mm[1])
                mods[mm[4]][1] = max(mods[mm[4]][1], int(mm[2]))
        imports.append(mods)
    tops = defaultdict(list)
    for mods in imports:
        per = defaultdict(float)
        for nme, (self_us, _) in mods.items():
            per[nme.split(".")[0]] += self_us / 1e3
        for k, v in per.items():
            tops[k].append(v)
    by_pkg = {k: med(v) for k, v in tops.items()}
    return {"n": len(imports), "by_pkg": by_pkg,
            "total_ms": med([sum(m[0] for m in mods.values()) / 1e3 for mods in imports])}


def main() -> None:
    data = {"sets": [], "groups": GROUPS, "post_groups": POST_GROUPS}
    for label, d in SETS:
        sc = build_set(Path(d))
        if sc:
            data["sets"].append({"label": label, "scenarios": sc})
    roots = [Path(d) for _, d in SETS]
    data["imports"] = {}
    data["env"] = {}
    for ref, ref_name, ref_sha in REFS:
        imp_root = next((r / ref for r in roots if any((r / ref).glob("importtime-*.txt"))), None)
        if imp_root is not None:
            data["imports"][ref] = import_summary(sorted(imp_root.glob("importtime-*.txt")))
        env_p = next((r / ref / "env.json" for r in roots if (r / ref / "env.json").exists()), None)
        if env_p is not None:
            data["env"][ref] = json.loads(env_p.read_text())
    data["refs"] = [{"key": r, "name": n, "sha": h} for r, n, h in REFS]
    html = (HERE / "report_template.html").read_text()
    html = html.replace("/*DATA*/null", json.dumps(data, separators=(",", ":")))
    OUT.write_text(html)
    print(f"wrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
