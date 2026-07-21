from __future__ import annotations

from pathlib import Path

import pytest

from acqbench.spec import Backend, Cell, Matrix, Ref, RefKind, ServerConfig, Topology


def test_parse_pypi_ref():
    r = Ref.parse("pypi:0.3.1")
    assert r.kind is RefKind.PYPI
    assert r.install_spec(Path.cwd()) == "acquirium==0.3.1"


def test_parse_pypi_latest_has_no_pin():
    assert Ref.parse("pypi:latest").install_spec(Path.cwd()) == "acquirium"


def test_parse_git_ref():
    r = Ref.parse("git:ums-ray-backend")
    assert r.kind is RefKind.GIT
    assert r.install_spec(Path.cwd()).endswith("@ums-ray-backend")
    assert "github.com/DataDrivenCPS/acquirium" in r.install_spec(Path.cwd())


def test_extras_ride_along_on_git_refs():
    r = Ref.parse("git:main", extras=("mqtt",))
    spec = r.install_spec(Path.cwd())
    assert spec.startswith("acquirium[mqtt] @ git+")


def test_bad_ref_specs_are_rejected():
    with pytest.raises(ValueError, match="must be"):
        Ref.parse("0.3.1")
    with pytest.raises(ValueError, match="unknown ref kind"):
        Ref.parse("conda:0.3.1")
    with pytest.raises(ValueError, match="empty target"):
        Ref.parse("pypi:")


def test_path_ref_requires_a_project(tmp_path):
    with pytest.raises(ValueError, match="no pyproject.toml"):
        Ref.parse(f"path:{tmp_path}").install_spec(Path.cwd())


def test_slugs_are_filesystem_safe_and_distinct():
    a = Ref.parse("git:feature/some-branch").slug
    b = Ref.parse("pypi:0.3.1").slug
    assert "/" not in a and a != b


def test_cell_id_is_stable_and_distinct():
    c1 = Cell(Ref.parse("pypi:0.3.1"), ServerConfig(), Topology.SERVER)
    c2 = Cell(Ref.parse("pypi:0.3.1"), ServerConfig(), Topology.SERVER)
    c3 = Cell(Ref.parse("git:main"), ServerConfig(), Topology.SERVER)
    assert c1.cell_id == c2.cell_id
    assert c1.cell_id != c3.cell_id


def test_topology_forces_component_counts():
    # A matrix asking for drivers on a server-only topology must not get them,
    # or the marginal-cost comparison is meaningless.
    cfg = ServerConfig(driver_count=4, app_count=2)
    assert Cell(Ref.parse("pypi:0.3.1"), cfg, Topology.SERVER).effective_config().driver_count == 0
    assert Cell(Ref.parse("pypi:0.3.1"), cfg, Topology.SERVER).effective_config().app_count == 0

    with_drivers = Cell(Ref.parse("pypi:0.3.1"), cfg, Topology.SERVER_DRIVERS).effective_config()
    assert with_drivers.driver_count == 4
    assert with_drivers.app_count == 0


def test_topology_with_drivers_defaults_to_at_least_one():
    cfg = ServerConfig(driver_count=0)
    eff = Cell(Ref.parse("pypi:0.3.1"), cfg, Topology.SERVER_DRIVERS).effective_config()
    assert eff.driver_count == 1


def test_timescale_needs_postgres():
    assert ServerConfig(backend=Backend.TIMESCALE).needs_postgres()
    assert not ServerConfig(backend=Backend.DUCKDB).needs_postgres()


def test_matrix_counts():
    m = Matrix(
        refs=[Ref.parse("pypi:0.3.1"), Ref.parse("git:main")],
        configs=[ServerConfig(), ServerConfig(read_batch_size=10_000)],
        topologies=[Topology.SERVER, Topology.SERVER_DRIVERS],
        workloads=["write_arrow", "read_full"],
        repetitions=3,
    )
    assert m.cell_count() == 8
    assert len(list(m.cells())) == 8
    assert m.run_count() == 48


def test_apps_topology_render_refuses_rather_than_duplicating():
    import pytest as _pytest
    from acqbench.render import render_config

    cell = Cell(Ref.parse("pypi:0.3.1"), ServerConfig(), Topology.SERVER_APPS)
    with _pytest.raises(NotImplementedError, match="not implemented"):
        render_config(cell, Path("/tmp/x"), port=8000)
