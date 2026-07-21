from __future__ import annotations

import json
from pathlib import Path

import pytest

from acqbench import provision as prov
from acqbench.spec import Ref


@pytest.fixture
def fake_uv(monkeypatch, tmp_path):
    """Record uv invocations instead of running them."""
    calls: list[list[str]] = []

    def _run(cmd, cwd=None, timeout=900):
        calls.append(cmd)

        class P:
            stdout = json.dumps({"version": "0.3.1", "direct_url": ""})
            stderr = ""
            returncode = 0

        # `uv venv <path>` is expected to produce a usable venv layout; build it
        # at whatever path the caller actually asked for.
        if cmd[:2] == ["uv", "venv"]:
            venv = Path(cmd[-1])
            (venv / "bin").mkdir(parents=True, exist_ok=True)
            (venv / "bin" / "python").write_text("")
            (venv / "bin" / "acquirium").write_text("")
        return P()

    monkeypatch.setattr(prov, "_run", _run)
    return calls


def test_install_allows_prereleases(fake_uv, tmp_path):
    # Published acquirium pins pyontoenv==0.6.0a2, a pre-release. Without
    # --prerelease=allow, uv refuses to resolve it and every `pypi:` baseline
    # fails to install — which would silently drop the comparison baseline.
    prov.provision(Ref.parse("pypi:0.3.1"), tmp_path / "venvs", tmp_path)
    install = next(c for c in fake_uv if c[:3] == ["uv", "pip", "install"])
    assert "--prerelease=allow" in install


def test_mutable_refs_are_reinstalled_not_served_from_cache(fake_uv, tmp_path):
    # A branch moves under you; a cached wheel would benchmark a stale build.
    prov.provision(Ref.parse("git:main"), tmp_path / "venvs", tmp_path)
    install = next(c for c in fake_uv if c[:3] == ["uv", "pip", "install"])
    assert "--reinstall-package" in install


def test_immutable_refs_may_use_cache(fake_uv, tmp_path):
    prov.provision(Ref.parse("pypi:0.3.1"), tmp_path / "venvs", tmp_path)
    install = next(c for c in fake_uv if c[:3] == ["uv", "pip", "install"])
    assert "--reinstall-package" not in install


def test_pinned_python_version(fake_uv, tmp_path):
    # Every ref must be compared on the same interpreter.
    prov.provision(Ref.parse("pypi:0.3.1"), tmp_path / "venvs", tmp_path)
    venv_cmd = next(c for c in fake_uv if c[:2] == ["uv", "venv"])
    assert "--python" in venv_cmd
    assert prov.PYTHON_VERSION in venv_cmd


def test_mutability_rules():
    assert prov._is_mutable(Ref.parse("git:main"))
    assert prov._is_mutable(Ref.parse("pypi:latest"))
    assert prov._is_mutable(Ref.parse("path:../acquirium"))
    assert not prov._is_mutable(Ref.parse("pypi:0.3.1"))
    # A full SHA is immutable and can be cached.
    assert not prov._is_mutable(Ref.parse("git:" + "a" * 40))
