# App path bench — `main` vs `ums-apps-improve`

Measures what the app-supervisor / app-runner rewrite does to registration,
steady-state load and overrun behaviour. Report:
`../reports/apps_branch_bench.html`.

## Why these three phases

The branch changes three things that can be measured from outside:

| | `main` | `ums-apps-improve` |
|---|---|---|
| Supervisor lock | `_lock` held across `ray.get(actor.register/setup)` | `_lock` guards the dict only; builds serialize on `_build_lock`, which no request path takes |
| Graph polling | `graph_status()` once per app **per tick** | floored at `max(interval, 10 s)` |
| Overrun | a run per tick, unbounded | `IntervalScheduler`: skip-and-count, `max_in_flight`, optional `run_timeout` |
| Teardown | unbounded actor calls | 10 s then kill |
| Env | inherit only | `EnvSpec` → persistent overlay venv, `py_executable` |

So: `register` (lock scope), `steady` (poll rate + tick grid), `overrun`
(scheduler), plus `envspec` as a branch-only cost characterization.

## Pipeline

```
build_template.py  boot once per ref, snapshot a warm data_dir  -> <root>/templates/<ref>
run_all.sh         every phase, both refs, one server at a time -> results/*.json
make_report.py     -> ../reports/apps_branch_bench.html
```

```
ROOT=<bench root> WT=<worktrees root> ./run_all.sh
ROOT=<bench root> WT=<worktrees root> ./rerun_50.sh   # 50-app storm only, 2 reps/ref
```

`WT/<ref>/.venv` is a venv built from a detached worktree of `../../acquirium`,
one per ref. Servers bind a free port from 8811 up — **never 8000**, which
belongs to the user's own server.

## Traps this harness works around

**A cold boot is ~7 minutes, and `recreate = true` throws it away.** Most of it
is the QUDT embedding index (~385 s). `build_template.py` pays that once per ref
and every phase copies the snapshot and boots with `recreate = false`.

**Ray's plasma socket is an AF_UNIX path capped at 103 bytes.** The scratchpad
path alone is longer than that, so `RAY_TMPDIR` is forced to a short
`/tmp/aqb/<tag>` per server. Without it every server dies at `ray.init`.

**A wedged server must not be hammered.** A teardown call that times out client-
side does not free the server thread serving it, so retrying only exhausts the
request threadpool faster — the first run spent 50 minutes deleting apps from a
server that had already stopped answering. `stop_and_delete` bounds each call at
20 s, gives up after 3 consecutive timeouts, and records
`teardown_wedged` / `server_alive_after_teardown`; the register phase then stops
escalating.

**This Mac idle-sleeps mid-run.** Two sleeps (980 s and 525 s) corrupted the
first pass — the 50-app count landed inside one of them. Long runs go through
`caffeinate -dimsu`, and every register cell records `clock_gap_s`
(`time.time()` minus `time.perf_counter()` across the count): macOS pauses the
monotonic clock while suspended, so a slept run looks *fast*, not slow, and the
gap is the only thing that gives it away. `machine_slept: true` invalidates the
cell.

**Probe timeouts are latency observations.** A `/apps/list` that times out at 60 s
is recorded as 60 s rather than dropped, so main's p95 does not silently improve
by losing its worst samples. Anything reading 60,000 ms is a floor, not a value.

## Reading the results

`register` at n=50 on `main` is a **categorical** result, not a slow one: 0 of 50
requests returned and the server never answered again. Do not average it with the
other counts.

The branch's HTTP 400s at n=50 (16/50 and 20/50 succeeded on a cold server,
38/50 on one already warm from the smaller counts) are its own bug (the supervisor's in-handler
`Acquirium(...)` health-checks the server it runs inside), not a measurement
artefact — the server-side traceback is in the phase's `server.log`.
