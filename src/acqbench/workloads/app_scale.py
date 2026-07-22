"""App-scalability: push each branch to its breaking point.

The two branches run apps very differently — `main` starts one Docker container
per app, `ums-ray-backend` one Ray actor per app — so this measures the
overhead of that infrastructure and finds where it runs out. For an escalating
sequence of app counts it: registers + runs N simple read-only keep-alive apps,
waits until all N are genuinely online (each has emitted its first trigger to
the receiver), measures a steady-state window, records per-app memory, then
stops them and moves to a larger N. It stops escalating the moment a count can't
bring every app online — that count is the ceiling.

Two measurement rules, both from the user:
  * startup time = registration start until the LAST app comes online.
  * every per-app latency / throughput number comes ONLY from the steady-state
    window after all N are up — never the startup ramp.
Both live in receiver.analyze().

Apps are registered by exec'ing run_bench.py with the *ref's* interpreter, so
each branch uses its native register_app path; the harness only speaks to them
through the shared latency receiver.
"""

from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..receiver import Receiver
from .base import Context, Workload, register

DEFAULT_COUNTS = [1, 10, 25, 50, 75, 100, 150, 200]
RUN_BENCH = Path(__file__).resolve().parents[1] / "run_bench.py"


@register("app_scale")
class AppScale(Workload):
    """Escalate app count to the breaking point, per branch."""

    requires_cold_server = True  # its own server; apps are heavy and stateful

    def __init__(
        self,
        *,
        counts: list[int] | None = None,
        interval: float = 2.0,
        steady_seconds: float = 30.0,
        per_app_startup_budget: float = 4.0,
        **params: Any,
    ):
        super().__init__(
            counts=counts, interval=interval, steady_seconds=steady_seconds,
            per_app_startup_budget=per_app_startup_budget, **params,
        )
        self.counts = counts or DEFAULT_COUNTS
        self.interval = interval
        self.steady_seconds = steady_seconds
        self.per_app_startup_budget = per_app_startup_budget

    def _branch(self, ctx: Context) -> str:
        # main uses Docker-per-app; everything else is treated as the Ray path.
        return "main" if ctx.cell.ref.target == "main" else "ums"

    def _receiver_url(self, branch: str, port: int) -> str:
        # main's apps run inside containers (reach host via host.docker.internal);
        # ums-ray's run as host Ray tasks (reach the receiver on loopback).
        host = "host.docker.internal" if branch == "main" else "127.0.0.1"
        return f"{host}:{port}/alerts"

    def run(self, ctx: Context) -> dict[str, Any]:
        branch = self._branch(ctx)
        cfg = ctx.cell.effective_config()
        results: list[dict[str, Any]] = []
        ceiling = 0
        ceiling_reason = "not reached"

        # Bind 0.0.0.0 so main's worker containers can POST back via
        # host.docker.internal; ums-ray reaches it on loopback all the same.
        with Receiver(host="0.0.0.0") as receiver:
            self._setup_point(ctx)
            # App memory is the increase in this cgroup's resident memory once N
            # apps are up. Both execution models live inside one cgroup — main's
            # worker containers roll up as nested-cgroup children, ums-ray's Ray
            # actors as descendants of the server — so a memory.current delta is
            # an accurate, consistent measure for both (real resident memory, not
            # RSS-summed shared pages, and it works where nested `docker stats`
            # reports 0). Falls back to server-tree RSS where there is no cgroup.
            self._baseline_rss_mb = ctx.server.resources().get("rss_mb", 0.0)
            self._baseline_cg_mb = _cgroup_mem_mb()
            for n in self.counts:
                point = self._run_count(ctx, receiver, branch, cfg.app_image, n)
                results.append(point)
                if point["complete"]:
                    ceiling = n
                else:
                    ceiling_reason = point.get("failure", "not all apps came online")
                    break  # breaking point found

        return {
            "branch": branch,
            "ceiling": ceiling,
            "ceiling_reason": ceiling_reason,
            "counts_attempted": [p["n"] for p in results],
            "points": results,
            **{f"server_{k}": v for k, v in ctx.server.resources().items()},
        }

    # -- one app count ----------------------------------------------------

    def _run_count(
        self, ctx: Context, receiver: Receiver, branch: str, image: str, n: int
    ) -> dict[str, Any]:
        # Teardown must ALWAYS run, even if a step raises (a subprocess timeout
        # at a saturating count would otherwise leak N app containers), so the
        # whole body is wrapped and teardown lives in the finally.
        port = ctx.server.port
        try:
            return self._measure_count(ctx, receiver, branch, image, n, port)
        except subprocess.TimeoutExpired:
            return _failed(n, f"timed out registering/starting {n} apps (saturated)")
        except Exception as e:  # noqa: BLE001 — any error at a count = that count is the ceiling
            return _failed(n, f"{type(e).__name__}: {e}"[:200])
        finally:
            self._teardown(ctx, branch, n, port)

    def _measure_count(
        self, ctx: Context, receiver: Receiver, branch: str, image: str, n: int, port: int
    ) -> dict[str, Any]:
        receiver_url = self._receiver_url(branch, receiver.port)
        # Fresh receiver slate so this count's startup/steady analysis is clean.
        with receiver._lock:
            receiver._records.clear()

        registration_start = datetime.now(timezone.utc)
        start_proc = self._run_bench(
            ctx, "start", port, branch,
            extra=["--n", str(n), "--interval", str(self.interval),
                   "--receiver-url", receiver_url]
            + (["--docker-image", image] if branch == "main" else []),
            timeout=n * self.per_app_startup_budget + 300,
        )
        if start_proc.returncode != 0:
            return _failed(n, f"registration failed: {start_proc.stderr[-400:]}")

        # Wait until all N apps have emitted their first trigger (genuinely
        # online), or give up — that's the ceiling.
        startup_budget = n * self.per_app_startup_budget + 90.0
        online = self._await_all_online(receiver, n, startup_budget)
        if online < n:
            mem = self._app_memory(ctx, branch)
            r = _failed(n, f"only {online}/{n} apps came online within {startup_budget:.0f}s")
            r["app_memory_mb"] = mem
            return r

        # All online — measure a steady-state window.
        time.sleep(self.steady_seconds)
        mem = self._app_memory(ctx, branch)
        analysis = receiver.analyze(n, registration_start)

        from ..metrics import summarize
        return {
            "n": n,
            "complete": True,
            "apps_online": analysis.apps_online,
            "startup_s": analysis.startup_s,
            "per_app_online_s_p95": _p95(analysis.per_app_online_s),
            "steady_triggers": analysis.steady_triggers,
            "steady_span_s": analysis.steady_span_s,
            "received_to_completed_ms": summarize(analysis.received_to_completed_ms),
            "completed_to_endpoint_ms": summarize(analysis.completed_to_endpoint_ms),
            "steady_throughput_per_s": (
                analysis.steady_triggers / analysis.steady_span_s
                if analysis.steady_span_s > 0 else 0.0
            ),
            "app_memory_mb": mem,
            "per_app_memory_mb": (mem / n) if mem and n else None,
        }

    # -- helpers ----------------------------------------------------------

    def _setup_point(self, ctx: Context) -> None:
        proc = self._run_bench(ctx, "setup", ctx.server.port, self._branch(ctx), timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"bench_setup failed: {proc.stderr[-500:]}")

    def _run_bench(
        self, ctx: Context, command: str, port: int, branch: str,
        *, extra: list[str] | None = None, timeout: float = 300,
    ) -> subprocess.CompletedProcess:
        cmd = [
            str(ctx.ref_python), str(RUN_BENCH), command,
            "--port", str(port), "--host", "127.0.0.1", "--branch", branch,
        ] + (extra or [])
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    def _await_all_online(self, receiver: Receiver, n: int, budget: float) -> int:
        deadline = time.monotonic() + budget
        best = 0
        while time.monotonic() < deadline:
            seen = len({t.app_id for t in receiver.snapshot()})
            best = max(best, seen)
            if seen >= n:
                return seen
            time.sleep(1.0)
        return best

    def _app_memory(self, ctx: Context, branch: str) -> float | None:
        """Total memory (MB) attributable to the running apps — the
        apps-infrastructure overhead being measured.

        Preferred: the cgroup memory.current delta from the apps-idle baseline,
        which captures both models (main's containers and ums-ray's actors) as
        real resident memory. Falls back per-branch where there is no cgroup:
        docker stats for main's containers, server-tree RSS delta for ums-ray.
        """
        base_cg = getattr(self, "_baseline_cg_mb", None)
        now_cg = _cgroup_mem_mb()
        if base_cg is not None and now_cg is not None:
            return max(0.0, now_cg - base_cg)
        if branch == "main":
            return _docker_app_memory_mb()
        now = ctx.server.resources().get("rss_mb", 0.0)
        return max(0.0, now - getattr(self, "_baseline_rss_mb", 0.0))

    def _teardown(self, ctx: Context, branch: str, n: int, port: int) -> None:
        # Stop via the API (clean), then force-reap anything left so the next
        # count starts from a clean slate and memory is actually freed.
        try:
            self._run_bench(
                ctx, "stop", port, branch, extra=["--n", str(n)], timeout=max(120, n * 2),
            )
        except subprocess.SubprocessError:
            pass
        if branch == "main":
            _docker_remove_app_containers()
        # Give the runtime a moment to release memory before the next count.
        time.sleep(3.0)


def _failed(n: int, reason: str) -> dict[str, Any]:
    return {"n": n, "complete": False, "apps_online": 0, "failure": reason}


def _p95(vals: list[float]) -> float:
    if not vals:
        return 0.0
    from ..metrics import percentile
    return percentile(sorted(vals), 95)


def _cgroup_mem_mb() -> float | None:
    """Current resident memory (MB) of this cgroup, or None if not on cgroup v2.

    Reads memory.current, which on a container rolls up the server, the harness,
    and every app (nested-container children for main, actor descendants for
    ums-ray) — so a delta from an apps-idle baseline is the apps' real footprint.
    """
    try:
        with open("/sys/fs/cgroup/memory.current") as f:
            return int(f.read().strip()) / 1e6
    except (OSError, ValueError):
        return None


def _docker_app_memory_mb() -> float | None:
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    total = 0.0
    found = False
    for line in out.stdout.splitlines():
        if "acquirium_app_" not in line:
            continue
        try:
            usage = line.split("\t")[1].split("/")[0].strip()  # "123.4MiB / 7.75GiB"
            total += _parse_mem(usage)
            found = True
        except (IndexError, ValueError):
            continue
    return total if found else 0.0


def _parse_mem(s: str) -> float:
    s = s.strip()
    for unit, mult in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024),
                       ("GB", 1000), ("MB", 1), ("kB", 1 / 1000), ("B", 1 / 1e6)):
        if s.endswith(unit):
            return float(s[: -len(unit)]) * mult
    return float(s)


def _docker_remove_app_containers() -> None:
    try:
        ids = subprocess.run(
            ["docker", "ps", "-aq", "--filter", "name=acquirium_app_"],
            capture_output=True, text=True, timeout=60,
        ).stdout.split()
        if ids:
            subprocess.run(["docker", "rm", "-f", *ids], capture_output=True, timeout=120)
    except (OSError, subprocess.SubprocessError):
        pass
