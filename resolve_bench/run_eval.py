"""Run examples.json against a warm acquirium server and score the answers.

Starts a server on a copy of a warm data_dir (embeddings already built), asks
/resolve_text for every example twice — once with the kind hint a caller may
know, once without — records the full ranked answer, then stops the server.

  VENV_DIR=~/acq_startup/venv python3 run_eval.py \
      --template ~/acq_startup/templates/tplE --label "main d894861" --out results_main.json
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
VENV = Path(os.environ.get("VENV_DIR", Path.home() / "acq_startup" / "venv"))

CONFIG = """[server]
host = "127.0.0.1"
port = {port}
data_dir = "{data_dir}"
timeseries_backend = "duckdb"
exact_only = {exact}
recreate = false
"""


def get(url: str, timeout: float = 120):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--examples", default=str(HERE / "examples.json"))
    ap.add_argument("--passes", default="with_kind,no_kind")
    ap.add_argument("--out", required=True)
    ap.add_argument("--port", type=int, default=8140)
    ap.add_argument("--exact-only", action="store_true")
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--min-score", type=float, default=0.5)
    args = ap.parse_args()

    examples = json.loads(Path(args.examples).read_text())["examples"]
    work = Path.home() / "acq_startup" / "eval_work"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    data_dir = work / "data"
    subprocess.run(["cp", "-a", args.template, str(data_dir)], check=True)
    cfg = work / "acquirium.toml"
    cfg.write_text(CONFIG.format(port=args.port, data_dir=data_dir,
                                 exact="true" if args.exact_only else "false"))

    env = {k: v for k, v in os.environ.items() if not k.startswith("ACQUIRIUM_")}
    log = open(work / "server.log", "wb")
    proc = subprocess.Popen([str(VENV / "bin" / "acquirium"), "server", "-c", str(cfg)],
                            stdout=log, stderr=subprocess.STDOUT, env=env, cwd=work)
    base = f"http://127.0.0.1:{args.port}"
    t0 = time.monotonic()
    while proc.poll() is None:
        try:
            get(base + "/health", timeout=2)
            break
        except Exception:
            time.sleep(0.1)
    if proc.poll() is not None:
        raise SystemExit(f"server exited rc={proc.returncode}; see {work}/server.log")
    started = time.monotonic() - t0
    status = get(base + "/embedding_status")

    results = []
    try:
        wanted = args.passes.split(",")
        for pass_name, use_kind in [p for p in (("with_kind", True), ("no_kind", False))
                                    if p[0] in wanted]:
            for e in examples:
                q = {"text": e["text"], "top_k": args.top_k, "min_score": args.min_score}
                if use_kind and e.get("expected_kind"):
                    q["kind"] = e["expected_kind"]
                t = time.monotonic()
                try:
                    matches = get(base + "/resolve_text?" + urllib.parse.urlencode(q))["matches"]
                    err = None
                except Exception as exc:
                    matches, err = [], repr(exc)
                ms = (time.monotonic() - t) * 1000
                uris = [m["uri"] for m in matches]
                rank = uris.index(e["expected_uri"]) + 1 if e["expected_uri"] in uris else None
                results.append({
                    "id": e["id"], "pass": pass_name, "text": e["text"],
                    "perturbation": e["perturbation"], "expected_kind": e["expected_kind"],
                    "expected_uri": e["expected_uri"], "expected_label": e["expected_label"],
                    "source_surface": e["source_surface"],
                    "top1_uri": uris[0] if uris else None,
                    "top1_label": matches[0]["label"] if matches else None,
                    "top1_kind": matches[0]["kind"] if matches else None,
                    "top1_score": matches[0]["score"] if matches else None,
                    "top1_stage": matches[0]["match_stage"] if matches else None,
                    "rank": rank, "n_matches": len(matches), "ms": round(ms, 2), "error": err,
                })
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
        subprocess.run(["pkill", "-9", "-f", "ray::|raylet|gcs_server"], check=False)

    Path(args.out).write_text(json.dumps({
        "label": args.label, "template": args.template, "exact_only": args.exact_only,
        "top_k": args.top_k, "min_score": args.min_score,
        "startup_s": round(started, 2), "embedding_status": status,
        "host": os.uname().nodename, "results": results,
    }, indent=1))
    top1 = [r for r in results if r["pass"] == "with_kind"]
    ok = sum(1 for r in top1 if r["rank"] == 1)
    print(f"{args.label}: with_kind top-1 {ok}/{len(top1)} -> {args.out}")


if __name__ == "__main__":
    main()
