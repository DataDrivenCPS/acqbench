"""Boot a disposable acquirium server for one ref, on a non-default port.

Port 8000 is never used — the user's own server lives there. Every phase gets
a fresh data_dir copied from a pre-warmed template (a cold boot spends minutes
building embedding indexes; ``recreate = true`` wipes that cache, so templates
are built once per ref and booted with ``recreate = false``).
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

import tomli_w
import urllib.request

HERE = Path(__file__).resolve().parent
# Never 8000: that port belongs to the user's own server.
BASE_PORT = 8811


def free_port(start: int = BASE_PORT) -> int:
    for port in range(start, start + 200):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free port")


def render_config(path: Path, *, data_dir: Path, port: int, recreate: bool) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    doc = {
        "server": {
            "host": "127.0.0.1",
            "port": port,
            "data_dir": str(data_dir),
            "timeseries_backend": "duckdb",
            "duckdb_path": str(data_dir / "timeseries.duckdb"),
            "recreate": recreate,
        }
    }
    path.write_text(tomli_w.dumps(doc))
    return path


def get_json(url: str, timeout: float = 5.0):
    import json
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


class Server:
    """A running `acquirium server` subprocess for one ref."""

    def __init__(self, venv_python: Path, workdir: Path, *, data_dir: Path,
                 port: int, recreate: bool, verbose: bool = False,
                 tag: str | None = None):
        self.venv_python = Path(venv_python)
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(data_dir)
        self.port = port
        self.recreate = recreate
        self.verbose = verbose
        # Ray's plasma socket is an AF_UNIX path capped at 103 bytes, so its
        # temp root cannot live under the (long) scratchpad path.
        self.ray_tmpdir = Path("/tmp/aqb") / (tag or f"p{port}")
        self.proc: subprocess.Popen | None = None
        self.log_path = self.workdir / "server.log"
        self.boot_s: float | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, timeout: float = 900.0) -> "Server":
        cfg = render_config(
            self.workdir / "acquirium.toml",
            data_dir=self.data_dir, port=self.port, recreate=self.recreate,
        )
        env = dict(os.environ)
        env["ACQUIRIUM_CONFIG"] = str(cfg)
        shutil.rmtree(self.ray_tmpdir, ignore_errors=True)
        self.ray_tmpdir.mkdir(parents=True, exist_ok=True)
        env["RAY_TMPDIR"] = str(self.ray_tmpdir)
        cmd = [str(self.venv_python.parent / "acquirium"), "server", "--config", str(cfg)]
        if self.verbose:
            cmd.append("-v")
        log = open(self.log_path, "ab", buffering=0)
        t0 = time.perf_counter()
        self.proc = subprocess.Popen(
            cmd, cwd=self.workdir, env=env, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"server exited with {self.proc.returncode}; see {self.log_path}"
                )
            try:
                if get_json(f"{self.url}/health", timeout=3.0).get("ok"):
                    self.boot_s = time.perf_counter() - t0
                    return self
            except Exception:
                time.sleep(1.0)
        self.stop()
        raise TimeoutError(f"server did not become healthy in {timeout:g}s")

    def stop(self, timeout: float = 30.0) -> None:
        if self.proc is None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc.wait(timeout=10)
        self.proc = None

    def __enter__(self) -> "Server":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def seed_template(venv_python: Path, template_dir: Path, workdir: Path,
                  *, port_start: int = BASE_PORT) -> Path:
    """Build (once) a warm data_dir: boot cold, then snapshot.

    Returns the template path; a no-op if it already exists.
    """
    stamp = template_dir / ".appbench-template"
    if stamp.exists():
        return template_dir
    build_dir = workdir / "template-build"
    shutil.rmtree(build_dir, ignore_errors=True)
    srv = Server(venv_python, build_dir, data_dir=build_dir / "data",
                 port=free_port(port_start), recreate=True,
                 tag=f"tpl-{template_dir.name}")
    srv.start(timeout=1800)
    # Give the embedding indexes a chance to finish building before snapshot.
    deadline = time.perf_counter() + 900
    while time.perf_counter() < deadline:
        try:
            st = get_json(f"{srv.url}/embedding_status", timeout=10)
            # The endpoint reports {"graph": {"state": "ready", ...}, "qudt": ...}
            if all(v.get("state") == "ready"
                   for v in st.values() if isinstance(v, dict)):
                break
        except Exception:
            pass
        time.sleep(5)
    srv.stop()
    # Publish by rename: a build interrupted halfway must never leave a
    # half-copied directory where a finished template used to be.
    staging = template_dir.with_name(template_dir.name + ".staging")
    shutil.rmtree(staging, ignore_errors=True)
    shutil.copytree(build_dir / "data", staging)
    # A stale duckdb from the template must not carry benchmark rows.
    (staging / "timeseries.duckdb").unlink(missing_ok=True)
    (staging / ".appbench-template").write_text("ok")
    shutil.rmtree(template_dir, ignore_errors=True)
    staging.rename(template_dir)
    shutil.rmtree(build_dir, ignore_errors=True)
    return template_dir


def fresh_data_dir(template_dir: Path, dest: Path) -> Path:
    shutil.rmtree(dest, ignore_errors=True)
    shutil.copytree(template_dir, dest)
    # The template is snapshotted with the duckdb removed; a leftover WAL
    # without its database would fail to attach.
    for stale in dest.glob("timeseries.duckdb*"):
        stale.unlink()
    return dest
