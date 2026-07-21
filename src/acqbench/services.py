"""External services the timescale backend needs.

The duckdb backend is self-contained, so the common path needs nothing here.
Only `timeseries_backend = "timescale"` requires Postgres, and we run our own
container rather than reusing acquirium's compose stack: the benchmark must be
able to wipe the database between cells without disturbing the developer's
environment, and it must not depend on a checkout of the acquirium repo.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass

CONTAINER = "acqbench-timescaledb"
IMAGE = "timescale/timescaledb:latest-pg17"
PG_PORT = 55433  # deliberately not 5432/55432 — avoid clashing with acquirium's own stacks
PG_USER = "acqbench"
PG_PASSWORD = "acqbench"
PG_DB = "acqbench"

DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@127.0.0.1:{PG_PORT}/{PG_DB}"


class ServiceError(RuntimeError):
    pass


@dataclass
class Postgres:
    dsn: str
    container: str


def _docker(args: list[str], timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout
    )
    if check and proc.returncode != 0:
        raise ServiceError(
            f"docker {' '.join(args)} failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    return proc


def docker_available() -> bool:
    try:
        return _docker(["info"], timeout=30, check=False).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _state() -> str | None:
    p = _docker(
        ["inspect", "-f", "{{.State.Status}}", CONTAINER], check=False, timeout=30
    )
    return p.stdout.strip() if p.returncode == 0 else None


def up(*, wait_timeout: float = 120.0) -> Postgres:
    """Start (or reuse) the benchmark Postgres and block until it accepts queries."""
    if not docker_available():
        raise ServiceError(
            "docker is not available/running, but the timescale backend needs it. "
            "Start Docker, or restrict the matrix to the duckdb backend."
        )

    state = _state()
    if state == "running":
        pass
    elif state is not None:
        _docker(["start", CONTAINER])
    else:
        _docker(
            [
                "run", "-d",
                "--name", CONTAINER,
                "-e", f"POSTGRES_USER={PG_USER}",
                "-e", f"POSTGRES_PASSWORD={PG_PASSWORD}",
                "-e", f"POSTGRES_DB={PG_DB}",
                "-p", f"{PG_PORT}:5432",
                # tmpfs: keep disk I/O variance out of the measurement and make
                # teardown instant. Benchmark data is disposable by definition.
                "--tmpfs", "/var/lib/postgresql/data:rw,size=4g",
                IMAGE,
            ],
            timeout=300,
        )

    _wait_ready(wait_timeout)
    return Postgres(dsn=DSN, container=CONTAINER)


def _wait_ready(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        p = _docker(
            ["exec", CONTAINER, "pg_isready", "-U", PG_USER, "-d", PG_DB],
            check=False,
            timeout=30,
        )
        if p.returncode == 0:
            return
        time.sleep(1.0)
    raise ServiceError(f"{CONTAINER} did not become ready within {timeout}s")


def reset_database() -> None:
    """Drop and recreate the database so a cell starts from empty.

    acquirium's own `recreate=true` handles its tables, but a hard reset also
    clears TimescaleDB chunk/compression state that would otherwise carry over
    and skew the next cell's write numbers.
    """
    for sql in (
        f'DROP DATABASE IF EXISTS "{PG_DB}" WITH (FORCE)',
        f'CREATE DATABASE "{PG_DB}" OWNER "{PG_USER}"',
    ):
        _docker(
            ["exec", CONTAINER, "psql", "-U", PG_USER, "-d", "postgres", "-c", sql],
            timeout=120,
        )


def down(*, remove: bool = True) -> None:
    if _state() is None:
        return
    _docker(["stop", CONTAINER], check=False, timeout=60)
    if remove:
        _docker(["rm", "-f", CONTAINER], check=False, timeout=60)
