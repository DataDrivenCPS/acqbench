"""Register / run / stop benchmark apps — driven by the app_scale workload.

Runs INSIDE a ref's venv (it imports acquirium and the branch-specific
`register_app`), exec'd as a subprocess by the harness. Three subcommands:

    setup   create the one shared benchmark point apps read
    start   register + run N keep-alive apps, then exit (apps keep running)
    stop    stop N apps (main: containers auto-remove; ums-ray: actor exits)

Per-branch differences are confined to `register_and_run`: main needs a real
docker_image + shipped source; ums-ray takes `replace=True`. `run_app` /
`stop_app` are identical on both. The receiver URL is passed through so apps
POST to 127.0.0.1 (ums-ray host task) or host.docker.internal (main container).
"""

from __future__ import annotations

import argparse
import inspect
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# run_bench.py lives at <harness>/src/acqbench/run_bench.py; put <harness>/src on
# the path so `acqbench.bench_app` imports without the harness being installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from acqbench.bench_app import BenchApp, POINT_URI  # noqa: E402
from acquirium import Acquirium  # noqa: E402

BENCH_SOURCE_ID = "acqbench"
BENCH_REF_NAME = "temp0"


def bench_setup(aq: Acquirium) -> None:
    """The minimal graph/data an app needs: a datasource, a point with a
    hasExternalReference stream, and a couple of rows so latest_data() is
    non-empty."""
    aq.register_datasource(BENCH_SOURCE_ID)
    aq.register_streams([
        {
            "point_uri": POINT_URI,
            "source_id": BENCH_SOURCE_ID,
            "ref_name": BENCH_REF_NAME,
            "label": "acqbench temperature 0",
        }
    ])
    now = datetime.now(timezone.utc)
    rows = [(now - timedelta(seconds=20), 21.5), (now - timedelta(seconds=10), 22.0)]
    aq.insert_timeseries(BENCH_SOURCE_ID, BENCH_REF_NAME, rows, point_uri=POINT_URI)


def app_names(n: int, offset: int = 0) -> list[str]:
    return [f"bench_app_{offset + i}" for i in range(n)]


def _make(name: str) -> BenchApp:
    app = BenchApp(point_uri=POINT_URI)
    app.name = name
    return app


def register_and_run(
    aq: Acquirium, names: list[str], *, receiver_url: str, interval: float,
    branch: str, docker_image: str | None = None,
) -> None:
    from acqbench import bench_app as _mod
    source = inspect.getsource(_mod)
    for name in names:
        app = _make(name)
        if branch == "main":
            aq.register_app(
                app,
                source_code=source,
                entry_file="bench_app.py",
                command="python -m acquirium.Apps.worker",
                docker_image=docker_image,  # client default is a bogus image
            )
        else:
            aq.register_app(app, replace=True)
        aq.run_app(
            app.name, keep_alive=True, interval=interval,
            params={"point_uri": POINT_URI, "receiver_url": receiver_url},
        )
        print(f"started {name}", flush=True)


def stop(aq: Acquirium, names: list[str]) -> None:
    for name in names:
        try:
            aq.stop_app(app_id=name)
            print(f"stopped {name}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"stop {name} failed: {exc}", flush=True)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["setup", "start", "stop"])
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--n", type=int, default=0)
    p.add_argument("--offset", type=int, default=0, help="first app index")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--receiver-url", default="")
    p.add_argument("--branch", choices=["main", "ums"], default="ums")
    p.add_argument("--docker-image", default=None)
    args = p.parse_args()

    aq = Acquirium(server_url=args.host, server_port=args.port)
    if args.command == "setup":
        bench_setup(aq)
        print("setup ok", flush=True)
    elif args.command == "start":
        t0 = time.time()
        register_and_run(
            aq, app_names(args.n, args.offset), receiver_url=args.receiver_url,
            interval=args.interval, branch=args.branch, docker_image=args.docker_image,
        )
        print(f"started {args.n} apps in {time.time() - t0:.1f}s", flush=True)
    elif args.command == "stop":
        stop(aq, app_names(args.n, args.offset))
    return 0


if __name__ == "__main__":
    sys.exit(main())
