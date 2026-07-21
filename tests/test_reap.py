"""Ray-worker reaping must be precisely scoped — a bug could kill the user's
own processes, which has happened before in this project."""

from __future__ import annotations

from acqbench.server import is_ray_worker_of

BENCH = "/Users/x/acquirium_benchmark_suite/venvs/git-ums-ray-backend"
USER = "/Users/x/acquirium/.venv"


def test_matches_ray_worker_from_benchmark_venv():
    assert is_ray_worker_of(f"{BENCH}/lib/python3.12/site-packages/ray/core/src/ray/raylet/raylet", BENCH)
    assert is_ray_worker_of(f"{BENCH}/bin/python -m ray::IDLE", BENCH)
    assert is_ray_worker_of(f"{USER}/lib/.../ray/gcs/gcs_server --foo", USER)


def test_never_matches_the_users_dev_environment_when_scoped_to_bench():
    # The exact incident to prevent: reaping bench workers must NOT touch a Ray
    # process running from the user's own acquirium checkout.
    user_gcs = f"{USER}/lib/python3.12/site-packages/ray/core/src/ray/gcs/gcs_server --log_dir=/tmp/ray"
    assert not is_ray_worker_of(user_gcs, BENCH)


def test_never_matches_non_ray_process_in_the_venv():
    # A pip/uv process using the same venv must not be killed.
    assert not is_ray_worker_of(f"uv pip install --python {BENCH}/bin/python acquirium", BENCH)
    assert not is_ray_worker_of(f"{BENCH}/bin/acquirium server --config x.toml", BENCH)


def test_empty_or_missing_venv_matches_nothing():
    assert not is_ray_worker_of("ray/core/raylet", "")
    assert not is_ray_worker_of("", BENCH)


def test_requires_both_venv_and_ray_marker():
    # venv present but no ray marker -> no; ray marker but different venv -> no.
    assert not is_ray_worker_of(f"{BENCH}/bin/python some_script.py", BENCH)
    assert not is_ray_worker_of("/other/venv/ray/core/raylet", BENCH)
