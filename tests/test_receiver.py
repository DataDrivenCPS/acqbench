"""Startup vs steady-state split for the app-scalability receiver.

The whole point of this split (per the user's two notes): report how long it
takes to bring all N apps online, and compute per-app latency ONLY from the
steady-state window once all apps are running — never from the startup ramp,
where early apps run under a growing load while later apps are still starting.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from acqbench.receiver import Receiver, Trigger

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _trigger(app: str, endpoint_off_s: float, run_ms: float) -> Trigger:
    e = BASE + timedelta(seconds=endpoint_off_s)
    c = e - timedelta(milliseconds=2)          # completed 2ms before endpoint
    r = c - timedelta(milliseconds=run_ms)     # run took run_ms
    return Trigger(app, r.isoformat(), c.isoformat(), e.isoformat())


def _receiver_with(records: list[Trigger]) -> Receiver:
    r = Receiver()
    r._records.extend(records)
    return r


def test_startup_time_is_when_the_last_app_comes_online():
    # 3 apps come online staggered; startup = last one's first trigger.
    recs = [_trigger("a1", 0.5, 10), _trigger("a2", 1.5, 10), _trigger("a3", 3.0, 10)]
    a = _receiver_with(recs).analyze(n_apps=3, registration_start=BASE)
    assert a.complete
    assert a.apps_online == 3
    assert a.startup_s == 3.0
    assert a.per_app_online_s == [0.5, 1.5, 3.0]


def test_steady_state_excludes_startup_ramp_measurements():
    # Startup-ramp runs are 10ms; steady-state runs are 12ms. Only the 12ms
    # ones (after all 3 are online at t=3.0) must count.
    recs = [_trigger("a1", 0.5, 10), _trigger("a2", 1.5, 10), _trigger("a3", 3.0, 10)]
    for off in (4.0, 5.0, 6.0):
        for app in ("a1", "a2", "a3"):
            recs.append(_trigger(app, off, 12))
    a = _receiver_with(recs).analyze(n_apps=3, registration_start=BASE)
    assert a.steady_triggers == 9  # 3 apps x 3 rounds, all after t=3.0
    med = sorted(a.received_to_completed_ms)[len(a.received_to_completed_ms) // 2]
    assert med == 12.0  # not 10.0 — the startup-ramp samples are excluded


def test_incomplete_startup_is_flagged():
    # Only 2 of 3 apps ever emit — the fleet never fully came online.
    recs = [_trigger("a1", 0.5, 10), _trigger("a2", 1.5, 10)]
    a = _receiver_with(recs).analyze(n_apps=3, registration_start=BASE)
    assert not a.complete
    assert a.apps_online == 2


def test_no_triggers_yields_no_startup():
    a = _receiver_with([]).analyze(n_apps=5, registration_start=BASE)
    assert a.apps_online == 0
    assert a.startup_s is None
    assert a.steady_triggers == 0


def test_receiver_captures_posted_triggers_over_http():
    import time
    import httpx

    with Receiver() as r:
        t0 = datetime.now(timezone.utc)
        httpx.post(
            f"{r.url}/alerts",
            json={"app_id": "x", "time_received": t0.isoformat(),
                  "time_completed": (t0 + timedelta(milliseconds=5)).isoformat()},
        )
        time.sleep(0.2)
        assert r.count() == 1
