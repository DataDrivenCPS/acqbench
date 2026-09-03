"""Run one measurement phase against one ref. Executed with that ref's venv.

    <wt>/<ref>/.venv/bin/python run_phase.py --ref main --phase register \
        --root <bench root> --out results/register_main.json

Everything (server, client, apps) runs in the ref's own interpreter, so each
branch exercises its native register/run path; the harness only compares the
JSON the phases emit.

Phases
------
register  register + start N apps concurrently, with an API probe running, at
          escalating N. Measures registration throughput and whether unrelated
          requests survive the storm.
steady    N keep-alive apps at a short interval: tick period and jitter, the
          server's own request rate (the app runners poll it), CPU, RSS.
overrun   one app whose run outlasts its interval: do runs pile up, and does
          run_timeout cancel a hung run (branch only).
envspec   branch only: cost of an app that declares pip deps (cold build vs
          warm reuse).
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from bench_app import POINT_URI, BenchApp  # noqa: E402
from probe import ApiProbe, ResourceSampler, count_requests, summarize  # noqa: E402
from receiver import Receiver  # noqa: E402
from serverctl import Server, fresh_data_dir, free_port, get_json  # noqa: E402

from acquirium import Acquirium  # noqa: E402

SOURCE_ID = "appbench"
REF_NAME = "temp0"


# ─────────────────────────── helpers ───────────────────────────


def bench_setup(aq: Acquirium) -> None:
    """The minimum graph an app's query needs, plus two rows of data."""
    aq.register_datasource(SOURCE_ID)
    aq.register_streams([{
        "point_uri": POINT_URI,
        "source_id": SOURCE_ID,
        "ref_name": REF_NAME,
        "label": "appbench temperature 0",
    }])
    now = datetime.now(timezone.utc)
    aq.insert_timeseries(
        SOURCE_ID, REF_NAME,
        [(now - timedelta(seconds=20), 21.5), (now - timedelta(seconds=10), 22.0)],
        point_uri=POINT_URI,
    )


def make_app(name: str) -> BenchApp:
    app = BenchApp(point_uri=POINT_URI)
    app.name = name
    return app


def app_status(base_url: str, app_id: str) -> dict:
    try:
        return get_json(f"{base_url}/apps/list?app_id={app_id}", timeout=30)
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def alive(base_url: str, timeout: float = 10.0) -> bool:
    try:
        return bool(get_json(f"{base_url}/health", timeout=timeout).get("ok"))
    except Exception:
        return False


def stop_and_delete(aq: Acquirium, base_url: str, names: list[str],
                    *, call_timeout: float = 20.0, give_up_after: int = 3) -> dict:
    """Tear apps down between counts. Timed: teardown is a measurement too.

    A teardown call that times out does NOT free the server thread serving it —
    the request keeps waiting on the actor — so a run that keeps hammering a
    wedged server only exhausts the request threadpool faster (and takes
    hours). After ``give_up_after`` consecutive timeouts the rest are skipped
    and the server is reported as wedged.
    """
    import requests
    t0 = time.perf_counter()
    errors: list[str] = []
    done = 0
    consecutive = 0
    wedged = False
    for name in names:
        failed = False
        for endpoint in ("stop", "delete"):
            try:
                requests.post(f"{base_url}/apps/{endpoint}",
                              json={"app_id": name}, timeout=call_timeout)
            except requests.exceptions.Timeout:
                failed = True
                errors.append(f"{endpoint} {name}: timeout>{call_timeout:g}s")
            except Exception as exc:
                failed = True
                errors.append(f"{endpoint} {name}: {type(exc).__name__}")
        if failed:
            consecutive += 1
            if consecutive >= give_up_after:
                wedged = True
                break
        else:
            consecutive = 0
            done += 1
    return {
        "teardown_s": round(time.perf_counter() - t0, 2),
        "teardown_ok": done,
        "teardown_attempted": len(names),
        "teardown_wedged": wedged,
        "server_alive_after_teardown": alive(base_url),
        # Not "errors": phase cells merge this dict in, and a shared key would
        # silently overwrite the phase's own error list.
        "teardown_errors": errors[:6],
    }


class Timed:
    """Run fn on a daemon thread, record its latency or that it never returned."""

    def __init__(self, fn, *a, **kw):
        self.fn, self.a, self.kw = fn, a, kw
        self.ms: float | None = None
        self.error: str | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        t0 = time.perf_counter()
        try:
            self.fn(*self.a, **self.kw)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
        self.ms = (time.perf_counter() - t0) * 1e3

    def start(self):
        self.thread.start()
        return self


# ─────────────────────────── phases ───────────────────────────


def phase_register(ctx: dict, args) -> dict:
    """Escalating N, all registered at once, with the API probe running."""
    aq, base_url, rec = ctx["aq"], ctx["url"], ctx["receiver"]
    out = {"counts": [], "budget_s": args.budget}
    offset = 0

    for n in args.counts:
        names = [f"reg{n}_{i}" for i in range(n)]
        rec.clear()
        # 60s probe timeout, not the whole budget: a probe that blocks for the
        # full budget stops sampling entirely, and "took at least 60s" already
        # says everything a longer wait would.
        probe = ApiProbe(base_url, interval=0.25, timeout=60.0).start()
        sampler = ResourceSampler(ctx["server"].proc.pid).start()
        log_mark = ctx["server"].log_path.stat().st_size

        def one(name: str, results: dict):
            app = make_app(name)
            t0 = time.perf_counter()
            aq.register_app(app, replace=True)
            t1 = time.perf_counter()
            aq.run_app(
                name, keep_alive=True, interval=args.interval,
                params={"point_uri": POINT_URI, "receiver_url": rec.url},
            )
            t2 = time.perf_counter()
            results[name] = {"register_ms": (t1 - t0) * 1e3, "run_ms": (t2 - t1) * 1e3}

        results: dict[str, dict] = {}
        t_start = time.perf_counter()
        wall_start = time.time()
        workers = [Timed(one, name, results) for name in names]
        # Serial mode is the control: the same work with no contention at all.
        if args.serial:
            for w in workers:
                w.start(); w.thread.join(timeout=args.budget)
        else:
            for w in workers:
                w.start()
            deadline = time.perf_counter() + args.budget
            for w in workers:
                w.thread.join(timeout=max(0.0, deadline - time.perf_counter()))
        all_done_s = time.perf_counter() - t_start
        # macOS pauses the monotonic clock while the machine sleeps, so a
        # suspended run looks fast rather than slow. Wall clock keeps counting:
        # a gap between the two is the machine having gone to sleep mid-count,
        # which invalidates every timing in this cell.
        slept_s = (time.time() - wall_start) - all_done_s
        stuck = [w for w in workers if w.thread.is_alive()]

        # Online = the receiver has seen at least one run from that app.
        online_deadline = time.perf_counter() + max(30.0, args.interval * n)
        while time.perf_counter() < online_deadline:
            if len(rec.apps_seen() & set(names)) >= n:
                break
            time.sleep(0.25)
        all_online_s = time.perf_counter() - t_start
        online = len(rec.apps_seen() & set(names))

        reg = [v["register_ms"] for v in results.values()]
        run = [v["run_ms"] for v in results.values()]
        errs = [w.error for w in workers if w.error]
        req_counts, _ = count_requests(ctx["server"].log_path, log_mark)
        cell = {
            "n": n,
            "serial": bool(args.serial),
            "completed": len(results),
            "wedged": len(stuck),
            "online": online,
            "all_registered_s": round(all_done_s, 2),
            "clock_gap_s": round(slept_s, 1),
            "machine_slept": slept_s > 5.0,
            "all_online_s": round(all_online_s, 2) if online >= n else None,
            "register_ms": summarize(reg),
            "run_ms": summarize(run),
            "errors": errs[:5],
            "probe": probe.stop(),
            "resources": sampler.stop(),
            "server_requests": dict(sorted(req_counts.items(), key=lambda kv: -kv[1])[:8]),
        }
        cell.update(stop_and_delete(aq, base_url, names))
        out["counts"].append(cell)
        print(f"[{args.ref}] register n={n} registered={len(results)}/{n} "
              f"wedged={len(stuck)} online={online} all_reg={all_done_s:.1f}s "
              f"apps_list_p95={cell['probe']['apps_list']['p95_ms']}ms"
              + (f" SLEPT {slept_s:.0f}s" if slept_s > 5 else ""), flush=True)
        offset += n
        # A wedged server poisons every later count; stop escalating.
        if stuck:
            out["stopped_early"] = f"n={n} left {len(stuck)} registrations unfinished"
            break
        if cell.get("teardown_wedged") or not cell.get("server_alive_after_teardown"):
            out["stopped_early"] = f"n={n} left the server unresponsive after teardown"
            break
        time.sleep(3)
    return out


def phase_steady(ctx: dict, args) -> dict:
    """N keep-alive apps at a short interval; measure the steady window only."""
    aq, base_url, rec = ctx["aq"], ctx["url"], ctx["receiver"]
    n = args.counts[-1]
    names = [f"st_{i}" for i in range(n)]

    for name in names:
        aq.register_app(make_app(name), replace=True)
        aq.run_app(name, keep_alive=True, interval=args.interval,
                   params={"point_uri": POINT_URI, "receiver_url": rec.url})

    # Wait for the fleet to be online before measuring anything.
    ramp_deadline = time.perf_counter() + max(60.0, 4 * args.interval * n)
    t0 = time.perf_counter()
    while time.perf_counter() < ramp_deadline:
        if len(rec.apps_seen() & set(names)) >= n:
            break
        time.sleep(0.25)
    ramp_s = time.perf_counter() - t0
    online = len(rec.apps_seen() & set(names))

    rec.clear()
    probe = ApiProbe(base_url, interval=0.5, timeout=60).start()
    sampler = ResourceSampler(ctx["server"].proc.pid).start()
    log_mark = ctx["server"].log_path.stat().st_size
    window_start = time.perf_counter()
    time.sleep(args.seconds)
    window_s = time.perf_counter() - window_start
    req_counts, _ = count_requests(ctx["server"].log_path, log_mark)
    ticks = rec.snapshot()
    probe_rep, res_rep = probe.stop(), sampler.stop()

    # Per-app inter-arrival: how close the tick grid stays to the request.
    per_app: dict[str, list[float]] = {}
    for t in ticks:
        per_app.setdefault(t.app_id, []).append(t.arrived)
    periods: list[float] = []
    for arrivals in per_app.values():
        arrivals.sort()
        periods += [(b - a) * 1e3 for a, b in zip(arrivals, arrivals[1:])]
    run_ms = [(t.finished_epoch - t.started_epoch) * 1e3 for t in ticks
              if t.started_epoch and t.finished_epoch]

    return {
        "n": n,
        "interval_s": args.interval,
        "window_s": round(window_s, 1),
        "online": online,
        "ramp_s": round(ramp_s, 1),
        "ticks": len(ticks),
        "ticks_per_s": round(len(ticks) / window_s, 2),
        "expected_ticks_per_s": round(n / args.interval, 2),
        "period_ms": summarize(periods),
        "run_ms": summarize(run_ms),
        "compute_ms": summarize([t.compute_us / 1e3 for t in ticks if t.compute_us]),
        "read_ms": summarize([t.read_us / 1e3 for t in ticks if t.read_us]),
        "probe": probe_rep,
        "resources": res_rep,
        "server_requests": dict(sorted(req_counts.items(), key=lambda kv: -kv[1])[:10]),
        "server_requests_per_s": {
            k: round(v / window_s, 2)
            for k, v in sorted(req_counts.items(), key=lambda kv: -kv[1])[:10]
        },
        "teardown": stop_and_delete(aq, base_url, names),
    }


def phase_overrun(ctx: dict, args) -> dict:
    """One app whose run outlasts its interval — do runs pile up?"""
    aq, base_url, rec = ctx["aq"], ctx["url"], ctx["receiver"]
    name = "overrun_app"
    sleep_s = args.sleep_s
    rec.clear()
    aq.register_app(make_app(name), replace=True)
    sampler = ResourceSampler(ctx["server"].proc.pid).start()
    aq.run_app(name, keep_alive=True, interval=args.interval,
               params={"point_uri": POINT_URI, "receiver_url": rec.url,
                       "sleep_s": sleep_s})

    samples = []
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < args.seconds:
        st = app_status(base_url, name)
        runs = st.get("runs", []) if isinstance(st, dict) else []
        live = sum(1 for r in runs if r.get("status") in (None, "running", "pending"))
        samples.append({
            "t": round(time.perf_counter() - t0, 1),
            "runs": len(runs),
            "live": live,
            # Branch-only fields; absent on main.
            "in_flight": st.get("in_flight"),
            "dispatched": st.get("dispatched"),
            "skipped": st.get("skipped"),
        })
        time.sleep(2.0)

    final = app_status(base_url, name)
    runs = final.get("runs", []) if isinstance(final, dict) else []
    status_counts: dict[str, int] = {}
    for r in runs:
        status_counts[str(r.get("status"))] = status_counts.get(str(r.get("status")), 0) + 1
    res_rep = sampler.stop()
    out = {
        "app": name,
        "interval_s": args.interval,
        "sleep_s": sleep_s,
        "window_s": args.seconds,
        "expected_ticks": int(args.seconds / args.interval),
        "runs_recorded": len(runs),
        "run_status_counts": status_counts,
        "peak_live_runs": max((s["live"] for s in samples), default=0),
        "peak_in_flight": max((s["in_flight"] or 0 for s in samples), default=0),
        "skipped": final.get("skipped"),
        "dispatched": final.get("dispatched"),
        "ticks_received": len(rec.snapshot()),
        "samples": samples,
        "resources": res_rep,
    }
    out["teardown"] = stop_and_delete(aq, base_url, [name])

    # run_timeout: only the branch accepts it; on main the extra field is
    # rejected by the request model, which is itself the result.
    if args.run_timeout:
        rec.clear()
        hang = "hang_app"
        aq.register_app(make_app(hang), replace=True)
        t0 = time.perf_counter()
        try:
            aq.run_app(hang, keep_alive=True, interval=args.interval,
                       run_timeout=args.run_timeout,
                       params={"point_uri": POINT_URI, "receiver_url": rec.url,
                               "hang": True})
            time.sleep(max(args.run_timeout * 3, 20))
            st = app_status(base_url, hang)
            runs = st.get("runs", []) if isinstance(st, dict) else []
            counts: dict[str, int] = {}
            for r in runs:
                counts[str(r.get("status"))] = counts.get(str(r.get("status")), 0) + 1
            out["run_timeout"] = {
                "supported": True,
                "timeout_s": args.run_timeout,
                "observed_s": round(time.perf_counter() - t0, 1),
                "runs": len(runs),
                "status_counts": counts,
                "skipped": st.get("skipped"),
            }
        except Exception as exc:
            out["run_timeout"] = {"supported": False,
                                  "error": f"{type(exc).__name__}: {exc}"}
        out["run_timeout_teardown"] = stop_and_delete(aq, base_url, [hang])
    return out


def phase_envspec(ctx: dict, args) -> dict:
    """Branch only: what a declared pip environment costs, cold then warm."""
    aq, base_url, rec = ctx["aq"], ctx["url"], ctx["receiver"]
    env = {"pip": ["cowsay==6.1"]}
    out: dict = {"env": env}
    try:
        for label, name in (("cold", "env_a"), ("warm", "env_b")):
            t0 = time.perf_counter()
            aq.register_app(make_app(name), env=env, replace=True)
            out[label] = {"register_s": round(time.perf_counter() - t0, 2)}
            t0 = time.perf_counter()
            aq.run_app(name, keep_alive=True, interval=args.interval,
                       params={"point_uri": POINT_URI, "receiver_url": rec.url})
            deadline = time.perf_counter() + 180
            while time.perf_counter() < deadline and name not in rec.apps_seen():
                time.sleep(0.5)
            out[label]["first_tick_s"] = round(time.perf_counter() - t0, 2)
            out[label]["online"] = name in rec.apps_seen()
        # A no-env app registered right after, for the zero-cost baseline.
        t0 = time.perf_counter()
        aq.register_app(make_app("env_none"), replace=True)
        out["no_env"] = {"register_s": round(time.perf_counter() - t0, 2)}
        out["teardown"] = stop_and_delete(aq, base_url, ["env_a", "env_b", "env_none"])
    except TypeError as exc:
        out["supported"] = False
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


PHASES = {
    "register": phase_register,
    "steady": phase_steady,
    "overrun": phase_overrun,
    "envspec": phase_envspec,
}


# ─────────────────────────── driver ───────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", required=True)
    ap.add_argument("--phase", required=True, choices=sorted(PHASES))
    ap.add_argument("--root", required=True, help="bench root (templates/, work/)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--counts", default="1,10,25,50")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--sleep-s", type=float, default=5.0)
    ap.add_argument("--run-timeout", type=float, default=None)
    ap.add_argument("--budget", type=float, default=300.0,
                    help="per-count wall-clock budget before calling it wedged")
    ap.add_argument("--serial", action="store_true")
    args = ap.parse_args()
    args.counts = [int(x) for x in args.counts.split(",") if x]

    root = Path(args.root)
    work = root / "work" / f"{args.ref}-{args.phase}"
    data_dir = fresh_data_dir(root / "templates" / args.ref, work / "data")
    port = free_port()
    report: dict = {
        "ref": args.ref, "phase": args.phase, "port": port,
        "args": {k: v for k, v in vars(args).items()},
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    server = Server(Path(sys.executable), work, data_dir=data_dir,
                    port=port, recreate=False, tag=f"{args.ref}-{args.phase}")
    receiver = Receiver().start()
    try:
        server.start(timeout=900)
        report["server_boot_s"] = round(server.boot_s, 1)
        aq = Acquirium(server_url="127.0.0.1", server_port=port)
        t0 = time.perf_counter()
        bench_setup(aq)
        report["setup_s"] = round(time.perf_counter() - t0, 2)
        ctx = {"aq": aq, "url": server.url, "receiver": receiver, "server": server}
        report["result"] = PHASES[args.phase](ctx, args)
    except Exception as exc:
        report["fatal"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        Path(args.out).write_text(json.dumps(report, indent=2, default=str))
        receiver.stop()
        server.stop()
        print(f"[{args.ref}/{args.phase}] wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
