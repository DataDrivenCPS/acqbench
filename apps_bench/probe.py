"""Background probes: is the server still answering, and what is it doing?

`ApiProbe` polls two cheap endpoints on a fixed cadence while a phase runs.
`/apps/list` takes the supervisor's record lock; `/health` takes no lock but
shares the same bounded request threadpool. Comparing the two separates "the
lock is held" from "the whole server is wedged".

`ResourceSampler` samples the server process tree (uvicorn + every Ray worker
it spawned) so app-driven memory growth is visible.
"""

from __future__ import annotations

import statistics
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

import psutil


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    idx = min(len(s) - 1, int(round(q / 100.0 * (len(s) - 1))))
    return round(s[idx], 3)


def summarize(values: list[float]) -> dict:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "p50_ms": pct(values, 50),
        "p95_ms": pct(values, 95),
        "max_ms": round(max(values), 3),
        "mean_ms": round(statistics.fmean(values), 3),
    }


@dataclass
class ApiProbe:
    base_url: str
    interval: float = 0.25
    timeout: float = 30.0
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    lat: dict[str, list[float]] = field(default_factory=lambda: {"health": [], "apps_list": []})
    errors: dict[str, int] = field(default_factory=lambda: {"health": 0, "apps_list": 0})
    first_error: dict[str, str] = field(default_factory=dict)

    def _hit(self, key: str, path: str) -> None:
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(self.base_url + path, timeout=self.timeout) as r:
                r.read()
            self.lat[key].append((time.perf_counter() - t0) * 1e3)
        except Exception as exc:
            self.errors[key] += 1
            self.first_error.setdefault(key, f"{type(exc).__name__}: {exc}")
            # A timeout is still a latency observation: the request took at
            # least the timeout. Recording it keeps p95/max honest.
            self.lat[key].append((time.perf_counter() - t0) * 1e3)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._hit("health", "/health")
            self._hit("apps_list", "/apps/list")
            self._stop.wait(self.interval)

    def start(self) -> "ApiProbe":
        self._stop.clear()
        for v in self.lat.values():
            v.clear()
        for k in self.errors:
            self.errors[k] = 0
        self.first_error.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> dict:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.timeout + 5)
        return self.report()

    def report(self) -> dict:
        return {
            key: {**summarize(self.lat[key]),
                  "errors": self.errors[key],
                  "first_error": self.first_error.get(key)}
            for key in self.lat
        }


@dataclass
class ResourceSampler:
    """RSS and CPU of the server process tree, sampled on a background thread."""

    pid: int
    interval: float = 1.0
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    rss_mb: list[float] = field(default_factory=list)
    procs: list[int] = field(default_factory=list)
    cpu_pct: list[float] = field(default_factory=list)
    _cpu_cache: dict = field(default_factory=dict, repr=False)

    def _sample(self) -> None:
        try:
            root = psutil.Process(self.pid)
            tree = [root] + root.children(recursive=True)
        except psutil.Error:
            return
        rss = 0.0
        cpu = 0.0
        alive = 0
        for p in tree:
            try:
                rss += p.memory_info().rss
                # cpu_percent(None) is a delta since this Process object's
                # last call, so the objects must be cached across samples —
                # a fresh one always reports 0.
                cached = self._cpu_cache.setdefault(p.pid, p)
                cpu += cached.cpu_percent(None)
                alive += 1
            except psutil.Error:
                self._cpu_cache.pop(p.pid, None)
                continue
        self.rss_mb.append(rss / 1e6)
        self.cpu_pct.append(cpu)
        self.procs.append(alive)

    def _loop(self) -> None:
        self._sample()  # prime cpu_percent deltas
        self.rss_mb.clear(); self.cpu_pct.clear(); self.procs.clear()
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self.interval)

    def start(self) -> "ResourceSampler":
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> dict:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        return self.report()

    def report(self) -> dict:
        if not self.rss_mb:
            return {"samples": 0}
        return {
            "samples": len(self.rss_mb),
            "rss_mb_start": round(self.rss_mb[0], 1),
            "rss_mb_peak": round(max(self.rss_mb), 1),
            "rss_mb_end": round(self.rss_mb[-1], 1),
            "procs_peak": max(self.procs),
            "procs_end": self.procs[-1],
            "cpu_pct_mean": round(statistics.fmean(self.cpu_pct), 1),
            "cpu_pct_peak": round(max(self.cpu_pct), 1),
        }


def count_requests(log_path, since_byte: int = 0) -> tuple[dict[str, int], int]:
    """Count uvicorn access-log lines per path from ``since_byte`` onward.

    The access log is the only place the server's own request rate is visible
    — the app runners' graph-version polls are ordinary HTTP requests.
    """
    import re
    pat = re.compile(r'"(?:GET|POST|PUT|DELETE) ([^ ?]+)')
    counts: dict[str, int] = {}
    with open(log_path, "rb") as fh:
        fh.seek(since_byte)
        data = fh.read()
        end = fh.tell()
    for line in data.decode("utf-8", "replace").splitlines():
        m = pat.search(line)
        if m:
            counts[m.group(1)] = counts.get(m.group(1), 0) + 1
    return counts, end
