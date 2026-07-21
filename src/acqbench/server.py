"""Launch and supervise an acquirium server subprocess."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import httpx
import psutil

from .provision import Install


class ServerError(RuntimeError):
    pass


@dataclass
class ServerHandle:
    proc: subprocess.Popen
    base_url: str
    port: int
    log_path: Path
    startup_seconds: float
    ps: psutil.Process | None = None

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def tail(self, n: int = 60) -> str:
        try:
            return "\n".join(self.log_path.read_text(errors="replace").splitlines()[-n:])
        except OSError:
            return "<no log>"

    def resources(self) -> dict[str, float]:
        """Current RSS/CPU for the server tree (server + Ray workers)."""
        if self.ps is None:
            return {}
        try:
            procs = [self.ps] + self.ps.children(recursive=True)
            rss = 0
            cpu = 0.0
            for p in procs:
                try:
                    rss += p.memory_info().rss
                    cpu += p.cpu_times().user + p.cpu_times().system
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return {"rss_mb": rss / 1e6, "cpu_seconds": cpu, "proc_count": len(procs)}
        except psutil.Error:
            return {}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_health(base_url: str, timeout: float, proc: subprocess.Popen | None = None) -> float:
    """Block until /health answers. Returns seconds waited.

    Ontology load + embedding build make cold starts slow (tens of seconds), so
    callers should pass a generous timeout.
    """
    deadline = time.monotonic() + timeout
    start = time.monotonic()
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise ServerError(f"server exited with code {proc.returncode} before becoming healthy")
        try:
            r = httpx.get(f"{base_url}/health", timeout=5.0)
            if r.status_code == 200:
                return time.monotonic() - start
        except (httpx.HTTPError, OSError) as e:
            last_err = e
        time.sleep(0.25)
    raise ServerError(f"server not healthy within {timeout}s (last error: {last_err})")


@contextmanager
def running_server(
    install: Install,
    config_path: Path,
    workdir: Path,
    *,
    port: int,
    env_extra: dict[str, str] | None = None,
    startup_timeout: float = 300.0,
    verbose: bool = False,
    bind_host: str = "127.0.0.1",
) -> Iterator[ServerHandle]:
    """Run `acquirium server` for the duration of the block.

    The server is started in its own process group so that Ray workers and any
    driver subprocesses die with it rather than leaking into the next cell.
    `bind_host` is the listen interface (0.0.0.0 for app-scale so worker
    containers can reach it); the health/base URL always uses loopback.
    """
    log_path = workdir / "server.log"
    env = {**os.environ, **(env_extra or {})}
    env["ACQUIRIUM_CONFIG"] = str(config_path)

    cmd = [
        str(install.acquirium_bin),
        "server",
        "--config", str(config_path),
        "--host", bind_host,
        "--port", str(port),
    ]
    if verbose:
        cmd.append("-v")

    base_url = f"http://127.0.0.1:{port}"
    handle: ServerHandle | None = None
    with log_path.open("w") as log:
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        try:
            try:
                startup = wait_health(base_url, startup_timeout, proc)
            except ServerError as e:
                tail = "\n".join(log_path.read_text(errors="replace").splitlines()[-40:])
                raise ServerError(f"{e}\n--- server log tail ---\n{tail}") from None

            try:
                ps = psutil.Process(proc.pid)
            except psutil.NoSuchProcess:
                ps = None

            handle = ServerHandle(
                proc=proc,
                base_url=base_url,
                port=port,
                log_path=log_path,
                startup_seconds=startup,
                ps=ps,
            )
            yield handle
        finally:
            _terminate(proc)
            # Ray actors (the ums-ray-backend driver runtime) are spawned by the
            # raylet *outside* the server's process group, so killing that group
            # leaves gcs_server + workers orphaned. Reap any Ray process launched
            # from this install's venv. Strictly scoped to the benchmark venv, so
            # it can never touch the user's own acquirium checkout.
            _reap_ray_workers(install.venv)
            # main runs each app as a Docker container (acquirium_app_*), also
            # outside the server's process group; a workload that dies before its
            # own teardown would leak them. Backstop-reap here. Named containers
            # are unique to app_scale runs (sequential), so this is safe.
            _reap_app_containers()


_RAY_MARKERS = ("ray/core", "raylet", "gcs_server", "ray::", "/ray/", "plasma_store")


def is_ray_worker_of(cmdline: str, venv: str) -> bool:
    """True iff `cmdline` is a Ray process launched from `venv`.

    Both conditions are required: the venv path must appear (so only this
    benchmark venv's processes match, never the user's own acquirium checkout)
    AND a Ray marker must appear (so a non-Ray process using the venv, e.g. a
    pip install, is never killed).
    """
    if not venv or venv not in cmdline:
        return False
    return any(m in cmdline for m in _RAY_MARKERS)


def _reap_ray_workers(venv: Path) -> None:
    """Kill orphaned Ray processes belonging to a specific benchmark venv.
    No-op on the duckdb-only path with no Ray, or on `main` (threads)."""
    venv_str = str(venv)
    victims: list[psutil.Process] = []
    for p in psutil.process_iter(["cmdline"]):
        try:
            if is_ray_worker_of(" ".join(p.info["cmdline"] or ()), venv_str):
                victims.append(p)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    for p in victims:
        try:
            p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if victims:
        psutil.wait_procs(victims, timeout=10)


def _reap_app_containers() -> None:
    """Force-remove any leftover acquirium app worker containers (main branch).
    No-op if Docker is absent or there are none."""
    try:
        ids = subprocess.run(
            ["docker", "ps", "-aq", "--filter", "name=acquirium_app_"],
            capture_output=True, text=True, timeout=30,
        ).stdout.split()
    except (OSError, subprocess.SubprocessError):
        return
    if ids:
        try:
            subprocess.run(["docker", "rm", "-f", *ids], capture_output=True, timeout=120)
        except (OSError, subprocess.SubprocessError):
            pass


def _terminate(proc: subprocess.Popen, grace: float = 15.0) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        proc.terminate()
    try:
        proc.wait(timeout=grace)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        proc.kill()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
