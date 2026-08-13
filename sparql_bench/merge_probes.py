#!/usr/bin/env python3
"""Merge probe slices per branch and build the corpus for the timed run.

A query is "viable" when it completed inside the probe budget on BOTH branches.
Queries that blew the budget on one branch only are the interesting ones and are
kept in a separate file so the report can name them instead of quietly dropping
them.
"""

from __future__ import annotations

import glob
import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
RES = HERE / "results"


def merge(branch: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for f in sorted(glob.glob(str(RES / "probe" / f"{branch}_*.json")),
                    key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0])):
        d = json.loads(pathlib.Path(f).read_text())
        for p in d.get("probe", []):
            out[p["id"]] = p
    (RES / f"probe_{branch}.json").write_text(json.dumps(list(out.values()), indent=2))
    return out


def main():
    corpus = json.loads((HERE / "extracted_queries.json").read_text())
    pm, pd = merge("main"), merge("derived")
    print(f"probed: main={len(pm)} derived={len(pd)} corpus={len(corpus)}")

    def ok(p):
        return p is not None and p.get("probe_ms") is not None and not p.get("error")

    viable, divergent, dead = [], [], []
    for q in corpus:
        a, b = pm.get(q["id"]), pd.get(q["id"])
        if ok(a) and ok(b):
            viable.append(q)
        elif ok(a) or ok(b):
            divergent.append({
                "id": q["id"], "union": q["union"], "count": q["count"],
                "nchars": q["nchars"],
                "main": a.get("probe_ms") if ok(a) else (a or {}).get("error"),
                "derived": b.get("probe_ms") if ok(b) else (b or {}).get("error"),
            })
        else:
            dead.append({"id": q["id"], "count": q["count"],
                         "main": (a or {}).get("error"), "derived": (b or {}).get("error")})

    (RES / "viable_corpus.json").write_text(json.dumps(viable, indent=2))
    (RES / "divergent.json").write_text(json.dumps(divergent, indent=2))
    (RES / "dead.json").write_text(json.dumps(dead, indent=2))

    print(f"viable on both:  {len(viable)}")
    print(f"one-sided:       {len(divergent)}")
    print(f"over budget both:{len(dead)}")
    if divergent:
        print("\n-- completed on one branch only --")
        for d in divergent:
            print(f"  {d['id']} logged={d['count']} union={d['union']} "
                  f"main={d['main']} derived={d['derived']}")
    if dead:
        print("\n-- over budget on both --")
        for d in dead:
            print(f"  {d['id']} logged={d['count']} main={d['main']} derived={d['derived']}")

    # probe-level slowest, for context
    both = [(q["id"], pm[q["id"]]["probe_ms"], pd[q["id"]]["probe_ms"])
            for q in viable if ok(pm.get(q["id"])) and ok(pd.get(q["id"]))]
    both.sort(key=lambda t: -max(t[1], t[2]))
    print("\n-- slowest viable (probe ms) --")
    for i, a, b in both[:10]:
        print(f"  {i}  main={a:9.1f}  derived={b:9.1f}")


if __name__ == "__main__":
    main()
