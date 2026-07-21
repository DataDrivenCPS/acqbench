"""Minimal read-only benchmark App for acquirium app-scalability tests.

Works UNCHANGED on both the ``main`` branch (each app runs as a Docker
container running ``python -m acquirium.Apps.worker``) and the
``ums-ray-backend`` branch (each app runs as a Ray actor whose runs execute in
stateless Ray tasks).

Design constraints that make it portable across both execution models:

* Imports ONLY ``acquirium`` + stdlib. On ``main`` the module source is shipped
  to the server, written into the app-storage dir, bind-mounted read-only into
  the worker container and imported there; on ``ums-ray`` it is shipped, written
  to disk and imported inside a Ray actor. Any third-party import would have to
  exist in that runtime, so we avoid them.

* The constructor must be callable with NO arguments. The ``ums-ray`` AppRunner
  actor loads the class and instantiates it as ``cls()`` (see
  ``ray_backend.AppRunner._load_app``); it never forwards constructor kwargs.
  Therefore all *per-run* configuration (which receiver to POST to) is read from
  ``ctx.params`` at run time, NOT from ``__init__``.

* ``build_query`` targets a single, fixed benchmark point (``POINT_URI``). Both
  branches rebuild the query server-side from a no-arg ``cls()`` instance, so a
  per-instance point passed via the constructor would be lost. A read-only
  scalability benchmark only needs *a* cheap valid read, so every app instance
  reads the same shared point. That is intentional and sufficient.

The run logic is deliberately trivial (the "app logic is simple" requirement):
record a start timestamp, issue the cheapest valid read, record an end
timestamp, and POST both to the latency receiver via an ``Output.trigger``.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from acquirium import App, Output
from acquirium import Acquirium, AppContext

# Single shared benchmark point every app instance reads. The benchmark harness
# creates this point (a data node with a hasExternalReference stream + a couple
# of timeseries rows) before registering apps. See bench_setup() in
# scripts/run_bench.py / the module docstring of the harness.
POINT_URI = "urn:acqbench:point:temp0"


def _output_uri(point_uri: str) -> str:
    digest = hashlib.sha1(point_uri.encode("utf-8")).hexdigest()[:12]
    return f"urn:acqbench:trigger:{digest}"


class BenchApp(App):
    name = "bench_app"
    version = "0.1"
    app_type = "soft_sensor"
    # Only consulted on the ``main`` (Docker) path: the command the worker
    # container runs. Ignored by the ums-ray Ray-actor path.
    command = "python -m acquirium.Apps.worker"
    outputs = []

    # Class-level default so the no-arg cls() instance the server builds still
    # has a valid point + declared trigger output.
    point_uri = POINT_URI

    def __init__(self, point_uri: str | None = None) -> None:
        if point_uri is not None:
            self.point_uri = point_uri
        # One declared trigger output; registration turns this into a virtual
        # point + event stream in the graph. Kept minimal.
        self.outputs = [
            {
                "kind": "trigger",
                "point_uri": _output_uri(self.point_uri),
            }
        ]

    def build_query(self, aq: Acquirium):
        # Cheapest valid read: bind the one benchmark point.
        return aq.find_all_data(uri=self.point_uri)

    def run(self, ctx: AppContext) -> list[Output]:
        # time_received: when this run started processing.
        time_received = datetime.now(timezone.utc).isoformat()

        # Trivial, cheap read-only work: fetch the latest value for the bound
        # point. Guarded so a missing/empty point never crashes the run — the
        # benchmark cares that a read executed, not what it returned.
        try:
            ctx.query.latest_data(cast_value="float")
        except Exception:
            pass

        # time_completed: when this run finished processing.
        time_completed = datetime.now(timezone.utc).isoformat()

        params = ctx.params or {}
        # Receiver URL is supplied per-run via params so the harness can point
        # it at 127.0.0.1 (ums-ray, host process) or host.docker.internal
        # (main, inside a container). host:port[/path] is fine — the output
        # emitter prepends http:// when no scheme is present.
        receiver_url = params.get("receiver_url")
        point_uri = params.get("point_uri") or self.point_uri

        message = {
            "app_id": ctx.app_id,
            "time_received": time_received,
            "time_completed": time_completed,
        }
        return [
            Output.trigger(
                url=receiver_url,
                message=message,
                point_uri=point_uri,
            )
        ]
