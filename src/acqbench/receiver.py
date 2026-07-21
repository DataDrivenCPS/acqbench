"""In-process HTTP receiver for app-emitted latency triggers.

The app-scalability workload starts one of these on a background thread; the
benchmark apps POST a small JSON trigger each time they run, and the receiver
records the timing. This is the harness-embedded equivalent of the reference
`scripts/benchmark/latency_receiver.py`, but it keeps records in memory (no CSV)
and is start/stoppable from the workload.

Each app POSTs to /alerts with a JSON body carrying at least:
    {"app_id": str, "time_received": iso, "time_completed": iso}
`time_received` is when the app started its run, `time_completed` when it
finished. The receiver stamps `endpoint_receipt` on arrival, so it can report:

    received -> completed   app-run latency (the app's own logic)
    completed -> endpoint   dispatch latency (server -> receiver)

Latencies that cross the app boundary are subject to clock skew if the app runs
in a separate process/container, but here everything is one host, one clock.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@dataclass
class Trigger:
    app_id: str
    time_received: str | None
    time_completed: str | None
    endpoint_receipt: str


@dataclass
class Receiver:
    """A running latency receiver. Use via `with Receiver() as r:`."""

    port: int = 0  # 0 = OS-assigned; read .port after start
    host: str = "127.0.0.1"
    _server: ThreadingHTTPServer | None = field(default=None, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _records: list[Trigger] = field(default_factory=list, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __enter__(self) -> "Receiver":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> "Receiver":
        records, lock = self._records, self._lock

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                stamp = datetime.now(timezone.utc).isoformat()
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b""
                try:
                    payload = json.loads(raw.decode("utf-8")) if raw else {}
                except json.JSONDecodeError:
                    self.send_response(400)
                    self.end_headers()
                    return
                # The reference app nests fields under "message"; accept either.
                msg = payload.get("message", payload)
                with lock:
                    records.append(
                        Trigger(
                            app_id=str(msg.get("app_id", "unknown")),
                            time_received=msg.get("time_received"),
                            time_completed=msg.get("time_completed"),
                            endpoint_receipt=stamp,
                        )
                    )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')

            def log_message(self, *a: object) -> None:  # silence access logs
                return

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def snapshot(self) -> list[Trigger]:
        with self._lock:
            return list(self._records)

    def latencies_ms(self) -> dict[str, list[float]]:
        """Per-stage latency samples across ALL received triggers (unfiltered)."""
        recv_to_done: list[float] = []
        done_to_endpoint: list[float] = []
        per_app: dict[str, int] = {}
        for t in self.snapshot():
            per_app[t.app_id] = per_app.get(t.app_id, 0) + 1
            r, c, e = _parse(t.time_received), _parse(t.time_completed), _parse(t.endpoint_receipt)
            if r and c:
                recv_to_done.append((c - r).total_seconds() * 1000.0)
            if c and e:
                done_to_endpoint.append((e - c).total_seconds() * 1000.0)
        return {
            "received_to_completed": recv_to_done,
            "completed_to_endpoint": done_to_endpoint,
            "_per_app_counts": per_app,  # type: ignore[dict-item]
        }

    def analyze(self, n_apps: int, registration_start: datetime) -> "SteadyStateAnalysis":
        """Split startup from steady state.

        An app is "online" once it emits its first trigger (genuinely
        productive, not merely registered). Startup time is from
        `registration_start` until the LAST app comes online. All per-app
        latency and throughput stats are then computed ONLY from triggers that
        arrived after that moment — the startup ramp, where early apps run under
        a growing load while later apps are still being created, is excluded so
        it cannot contaminate the steady-state numbers.
        """
        triggers = self.snapshot()
        first_seen: dict[str, datetime] = {}
        for t in triggers:
            e = _parse(t.endpoint_receipt)
            if e is None:
                continue
            if t.app_id not in first_seen or e < first_seen[t.app_id]:
                first_seen[t.app_id] = e

        apps_online = len(first_seen)
        all_online_at = max(first_seen.values()) if first_seen else None
        complete = apps_online >= n_apps

        # Startup: time to bring the whole fleet online (only meaningful once all
        # N are up; if fewer came online, report how far it got).
        startup_s = (
            (all_online_at - registration_start).total_seconds()
            if all_online_at is not None
            else None
        )
        per_app_online_s = sorted(
            (t - registration_start).total_seconds() for t in first_seen.values()
        )

        # Steady state: triggers strictly after the last app came online.
        recv_to_done: list[float] = []
        done_to_endpoint: list[float] = []
        steady_apps: set[str] = set()
        steady_window: list[datetime] = []
        for t in triggers:
            e = _parse(t.endpoint_receipt)
            if all_online_at is None or e is None or e <= all_online_at:
                continue
            steady_apps.add(t.app_id)
            steady_window.append(e)
            r, c = _parse(t.time_received), _parse(t.time_completed)
            if r and c:
                recv_to_done.append((c - r).total_seconds() * 1000.0)
            if c and e:
                done_to_endpoint.append((e - c).total_seconds() * 1000.0)

        span_s = (
            (max(steady_window) - min(steady_window)).total_seconds()
            if len(steady_window) > 1
            else 0.0
        )
        return SteadyStateAnalysis(
            n_apps=n_apps,
            apps_online=apps_online,
            complete=complete,
            startup_s=startup_s,
            per_app_online_s=per_app_online_s,
            steady_triggers=len(steady_window),
            steady_apps=len(steady_apps),
            steady_span_s=span_s,
            received_to_completed_ms=recv_to_done,
            completed_to_endpoint_ms=done_to_endpoint,
        )


@dataclass
class SteadyStateAnalysis:
    n_apps: int
    apps_online: int
    complete: bool               # did all N apps come online?
    startup_s: float | None      # time to bring all N online (the user's note #1)
    per_app_online_s: list[float]
    steady_triggers: int         # trigger count in the steady window (note #2)
    steady_apps: int
    steady_span_s: float
    received_to_completed_ms: list[float]
    completed_to_endpoint_ms: list[float]


def _parse(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
