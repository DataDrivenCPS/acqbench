#!/bin/bash
# Launch: fresh timescale container (same image/env as the benchmark, plus pg_stat_statements
# and track_io_timing), then the tracer under py-spy sampling windows.  Survives ssh loss.
set -u
ROOT=${ROOT:-$HOME/acq_trace1}
V=$HOME/acq_bench10/venv_ums-timescale-fix
DATA=${DATA:-$HOME/dpr_1000}
MAXF=${MAXF:-1000}; DEADLINE=${DEADLINE:-2400}; EXPL=${EXPL:-25}; VARIANT=${VARIANT:-baseline}
TSDB=acq_bench_tsdb; IMG=timescale/timescaledb:latest-pg17
mkdir -p "$ROOT"; cd "$ROOT"
say(){ echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }
say "root=$ROOT data=$DATA max_files=$MAXF deadline=${DEADLINE}s explain_every=$EXPL variant=$VARIANT"
docker rm -f -v $TSDB >/dev/null 2>&1
say "public repo head: $(git ls-remote https://github.com/DataDrivenCPS/acquirium.git refs/heads/ums-timescale-fix | cut -c1-12)  clone head: $(git -C $HOME/acq_bench10/repo_ums-timescale-fix rev-parse --short=12 HEAD)"
say "image digest: $(docker inspect --format '{{index .RepoDigests 0}}' $IMG)"
T0=$(date +%s)
docker run -d --name $TSDB -p 127.0.0.1:5433:5432 -e POSTGRES_USER=acquirium -e POSTGRES_PASSWORD=acquirium \
  -e POSTGRES_DB=acquirium -e TS_TUNE_MEMORY=8GB -e TS_TUNE_NUM_CPUS=4 --shm-size=1g $IMG \
  postgres -c shared_preload_libraries=timescaledb,pg_stat_statements -c track_io_timing=on >/dev/null || { say "docker run failed"; exit 1; }
for i in $(seq 1 150); do
  if docker exec $TSDB pg_isready -U acquirium -d acquirium >/dev/null 2>&1; then sleep 3;
    docker exec $TSDB pg_isready -U acquirium -d acquirium >/dev/null 2>&1 && break; fi; sleep 2; done
say "timescale ready in $(( $(date +%s) - T0 ))s: $(docker exec $TSDB psql -U acquirium -d acquirium -tA -c "SELECT version(); SHOW shared_preload_libraries; SHOW track_io_timing; SHOW shared_buffers; SHOW work_mem; SELECT count(*) FROM information_schema.tables WHERE table_schema='public'" | tr '\n' ' ')"
"$V/bin/python" "$ROOT/ingest_trace.py" --data "$DATA" --out "$ROOT/trace" --max-files "$MAXF" --deadline-s "$DEADLINE" --explain-every "$EXPL" --variant "$VARIANT" > "$ROOT/tracer.out" 2>&1 &
PID=$!
say "tracer pid=$PID"
sleep 45
(
  "$V/bin/py-spy" record --pid $PID --duration ${SPY1:-600} --rate 50 --native --format flamegraph -o "$ROOT/flame_cpu.svg" > "$ROOT/spy1.log" 2>&1
  say "py-spy window 1 (cpu, native) done: $(tail -1 $ROOT/spy1.log)"
  sleep 5
  "$V/bin/py-spy" record --pid $PID --duration ${SPY2:-600} --rate 50 --native --idle --threads --format flamegraph -o "$ROOT/flame_wall.svg" > "$ROOT/spy2.log" 2>&1
  say "py-spy window 2 (wall, idle, threads) done: $(tail -1 $ROOT/spy2.log)"
  sleep 5
  "$V/bin/py-spy" record --pid $PID --duration ${SPY3:-900} --rate 50 --idle --threads --format raw -o "$ROOT/stacks_wall.txt" > "$ROOT/spy3.log" 2>&1
  say "py-spy window 3 (raw collapsed stacks) done: $(tail -1 $ROOT/spy3.log)"
) &
wait $PID; RC=$?
say "tracer exited rc=$RC"
docker logs $TSDB > "$ROOT/pg.log" 2>&1
docker exec $TSDB psql -U acquirium -d acquirium -tA -c "SELECT count(*) FROM timeseries; SELECT table_bytes, index_bytes, total_bytes FROM hypertable_detailed_size('timeseries'); SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='timeseries'" > "$ROOT/final_db.txt" 2>&1
say "final db: $(tr '\n' ' ' < $ROOT/final_db.txt)"
say "ALL DONE"
