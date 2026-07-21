"""Provision isolated acquirium installs, one venv per ref.

The harness never imports acquirium. Each ref gets its own uv venv so that
0.1.1, main, and a feature branch can coexist and be benchmarked back to back.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path

from .spec import Ref, RefKind

# acquirium requires >=3.12; pin the interpreter so every ref is compared on the
# same Python. A ref built on a different Python is not a fair comparison.
PYTHON_VERSION = "3.12"


class ProvisionError(RuntimeError):
    pass


@dataclass(frozen=True)
class Install:
    """A provisioned, ready-to-run acquirium."""

    ref_spec: str
    venv: Path
    python: Path
    acquirium_bin: Path
    version: str
    resolved: str  # exact source: version, or commit sha for git refs

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("venv", "python", "acquirium_bin"):
            d[k] = str(d[k])
        return d


def _run(cmd: list[str], cwd: Path | None = None, timeout: int = 900) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout
    )
    if proc.returncode != 0:
        raise ProvisionError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"--- stdout ---\n{proc.stdout[-4000:]}\n--- stderr ---\n{proc.stderr[-4000:]}"
        )
    return proc


def provision(
    ref: Ref,
    venv_root: Path,
    project_root: Path,
    *,
    force: bool = False,
) -> Install:
    """Create (or reuse) a venv with `ref` installed, and return how to run it.

    Provisioning is cached by ref slug. `force` rebuilds from scratch, which is
    what you want for a mutable ref like `git:main` or `pypi:latest` whose
    target moves under you.
    """
    venv = venv_root / ref.slug
    stamp = venv / ".acqbench-install.json"

    if stamp.exists() and not force:
        try:
            cached = json.loads(stamp.read_text())
            install = Install(
                ref_spec=cached["ref_spec"],
                venv=Path(cached["venv"]),
                python=Path(cached["python"]),
                acquirium_bin=Path(cached["acquirium_bin"]),
                version=cached["version"],
                resolved=cached["resolved"],
            )
            if install.python.exists() and install.acquirium_bin.exists():
                return install
        except (json.JSONDecodeError, KeyError):
            pass  # corrupt stamp; fall through and rebuild

    if venv.exists():
        shutil.rmtree(venv)
    venv_root.mkdir(parents=True, exist_ok=True)

    _run(["uv", "venv", "--python", PYTHON_VERSION, str(venv)])

    python = venv / "bin" / "python"
    install_spec = ref.install_spec(project_root)

    cmd = ["uv", "pip", "install", "--python", str(python)]
    # acquirium pins a pre-release dependency (pyontoenv==0.6.0a2). uv refuses
    # pre-releases by default when resolving a *dependency* of a requirement, so
    # without this every `pypi:` install fails to resolve — including the
    # release baselines that comparisons are measured against.
    cmd.append("--prerelease=allow")
    # A moving target must not be served from cache, or you benchmark a stale build.
    if force or _is_mutable(ref):
        cmd.append("--reinstall-package")
        cmd.append("acquirium")
    cmd.append(install_spec)
    _run(cmd, timeout=1800)

    version, resolved = _resolve_identity(python, ref)
    acquirium_bin = venv / "bin" / "acquirium"
    if not acquirium_bin.exists():
        raise ProvisionError(
            f"{ref.spec}: installed but no `acquirium` entrypoint at {acquirium_bin}"
        )

    install = Install(
        ref_spec=ref.spec,
        venv=venv,
        python=python,
        acquirium_bin=acquirium_bin,
        version=version,
        resolved=resolved,
    )
    stamp.write_text(json.dumps(install.to_dict(), indent=2))
    return install


def _is_mutable(ref: Ref) -> bool:
    """Refs whose contents can change without the spec changing."""
    if ref.kind is RefKind.PATH:
        return True
    if ref.kind is RefKind.GIT:
        # A full SHA is immutable; a branch or tag is not.
        return not _looks_like_sha(ref.target)
    return ref.kind is RefKind.PYPI and ref.target == "latest"


def _looks_like_sha(target: str) -> bool:
    return len(target) == 40 and all(c in "0123456789abcdef" for c in target.lower())


def _resolve_identity(python: Path, ref: Ref) -> tuple[str, str]:
    """Pin down exactly what got installed, so results are attributable."""
    proc = _run(
        [
            str(python),
            "-c",
            "import importlib.metadata as m, json;"
            "d=m.distribution('acquirium');"
            "print(json.dumps({'version': d.version,"
            " 'direct_url': (d.read_text('direct_url.json') or '')}))",
        ]
    )
    info = json.loads(proc.stdout.strip())
    version = info["version"]

    resolved = version
    direct = info.get("direct_url") or ""
    if direct:
        try:
            du = json.loads(direct)
            vcs = du.get("vcs_info") or {}
            if vcs.get("commit_id"):
                resolved = f"{version}+{vcs['commit_id'][:12]}"
            elif du.get("url", "").startswith("file://"):
                resolved = f"{version}+local"
        except json.JSONDecodeError:
            pass
    return version, resolved


def which_uv() -> str:
    uv = shutil.which("uv")
    if not uv:
        raise ProvisionError(
            "uv not found on PATH. Install it: curl -LsSf https://astral.sh/uv/install.sh | sh"
        )
    return uv
