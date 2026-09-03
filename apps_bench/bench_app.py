"""The app under test — one module, shipped to the server by register_app.

Kept import-light (acquirium + stdlib only): the source is written to the app
storage dir and imported inside a Ray actor / task, so anything not in the
server's environment would fail there.

The constructor must be callable with no arguments — ``AppRunner._load_app``
instantiates the class as ``cls()`` and never forwards kwargs — so everything
per-run is read from ``ctx.params``:

    receiver_url  where to POST the per-run trigger
    sleep_s       artificial run duration (the overrun phase makes this
                  longer than the tick interval)
    hang          when true, sleep effectively forever (run_timeout probe)
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone

from acquirium import Acquirium, App, AppContext, Output

POINT_URI = "urn:appbench:point:temp0"

# Fixed, I/O-free unit of in-app compute (~1-2 ms on an idle core). Chained
# SHA-1 so it cannot be optimised away; the absolute value is uninteresting,
# how it moves as the fleet grows is the point.
_COMPUTE_ITERS = 12_000


def output_uri(point_uri: str = POINT_URI) -> str:
    return f"urn:appbench:trigger:{hashlib.sha1(point_uri.encode()).hexdigest()[:12]}"


class BenchApp(App):
    name = "bench_app"
    version = "0.1"
    app_type = "soft_sensor"
    outputs = []

    point_uri = POINT_URI

    def __init__(self, point_uri: str | None = None) -> None:
        if point_uri is not None:
            self.point_uri = point_uri
        self.outputs = [{"kind": "trigger", "point_uri": output_uri(self.point_uri)}]

    def build_query(self, aq: Acquirium):
        return aq.find_all_data(uri=self.point_uri)

    def run(self, ctx: AppContext) -> list[Output]:
        t_start = time.time()
        time_received = datetime.now(timezone.utc).isoformat()
        params = ctx.params or {}

        t = time.perf_counter()
        digest = b"appbench"
        for _ in range(_COMPUTE_ITERS):
            digest = hashlib.sha1(digest).digest()
        compute_us = (time.perf_counter() - t) * 1e6

        t = time.perf_counter()
        try:
            ctx.query.latest_data(cast_value="float")
        except Exception:
            pass
        read_us = (time.perf_counter() - t) * 1e6

        # Artificial work: how a run outlasts its interval without needing a
        # real model. hang=True is the run_timeout probe (no cancellation
        # point of its own — only killing the worker frees the slot).
        if params.get("hang"):
            time.sleep(3600)
        sleep_s = float(params.get("sleep_s") or 0.0)
        if sleep_s > 0:
            time.sleep(sleep_s)

        return [
            Output.trigger(
                url=params.get("receiver_url"),
                message={
                    "app_id": ctx.app_id,
                    "time_received": time_received,
                    "time_completed": datetime.now(timezone.utc).isoformat(),
                    "started_epoch": t_start,
                    "finished_epoch": time.time(),
                    "compute_us": round(compute_us, 1),
                    "read_us": round(read_us, 1),
                },
                point_uri=params.get("point_uri") or self.point_uri,
            )
        ]
