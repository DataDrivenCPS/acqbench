"""Trigger receiver: one record per app run, stamped on arrival.

Every arrival carries a monotonic stamp as well as the wall clock, so
inter-run periods (the tick grid) are measured against a clock that cannot
step. Everything runs on one host, so app-side and receiver-side wall clocks
are the same clock.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@dataclass
class Tick:
    app_id: str
    arrived: float          # perf_counter at receipt
    started_epoch: float | None
    finished_epoch: float | None
    compute_us: float | None
    read_us: float | None


@dataclass
class Receiver:
    port: int = 0
    host: str = "127.0.0.1"
    _server: ThreadingHTTPServer | None = field(default=None, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _records: list[Tick] = field(default_factory=list, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def start(self) -> "Receiver":
        records, lock = self._records, self._lock

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                arrived = time.perf_counter()
                n = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(n) if n else b""
                try:
                    payload = json.loads(raw.decode()) if raw else {}
                except json.JSONDecodeError:
                    self.send_response(400); self.end_headers(); return
                msg = payload.get("message", payload)
                with lock:
                    records.append(Tick(
                        app_id=str(msg.get("app_id", "?")),
                        arrived=arrived,
                        started_epoch=msg.get("started_epoch"),
                        finished_epoch=msg.get("finished_epoch"),
                        compute_us=msg.get("compute_us"),
                        read_us=msg.get("read_us"),
                    ))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *a):  # silence
                pass

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def __enter__(self) -> "Receiver":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    @property
    def url(self) -> str:
        return f"127.0.0.1:{self.port}/alerts"

    def snapshot(self) -> list[Tick]:
        with self._lock:
            return list(self._records)

    def apps_seen(self) -> set[str]:
        with self._lock:
            return {t.app_id for t in self._records}

    def clear(self) -> None:
        with self._lock:
            self._records.clear()
