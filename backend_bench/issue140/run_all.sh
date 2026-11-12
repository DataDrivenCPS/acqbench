#!/usr/bin/env bash
# Three DuckDB variants on ~/data/dpr1000, fully decoupled (run under tmux).
# v1 = main @1b12d64 (UNIQUE(ref_id,ts) + delete/insert), v2 = no unique index, plain insert,
# v3 = staging table + main kept sorted by (ref_id, ts). Results under ~/var_runs/<v>/.
set -u
ROOT=$HOME/var_runs; DATA=$HOME/data/dpr1000; NFILES=1000; PORT=8080; BASE=http://127.0.0.1:$PORT
REPS=10; WARMUP=2; STALL_S=3600
mkdir -p "$ROOT"; MASTER=$ROOT/run_all.log
log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$MASTER"; }
P="docker ps"
rss_sid(){ ps -eo sid=,rss= | awk -v s="$1" '$1==s {t+=$2} END {print t+0}'; }
rss_ray(){ ps -eo rss=,comm= | awk '$2 ~ /^(raylet|gcs_server|ray::)/ {t+=$1} END {print t+0}'; }
wait_health(){ # $1 pid $2 logfile
  local t0=$(date +%s)
  while :; do
    kill -0 "$1" 2>/dev/null || { log "server $1 died (see $2)"; return 1; }
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 $BASE/health)" = 200 ] && { log "healthy after $(( $(date +%s) - t0 ))s"; return 0; }
    [ $(( $(date +%s) - t0 )) -gt 900 ] && { log "server not healthy after 900s"; return 1; }
    sleep 5
  done
}
stop_server(){ # $1 pid
  kill -TERM "$1" 2>/dev/null; local t0=$(date +%s)
  while kill -0 "$1" 2>/dev/null; do [ $(( $(date +%s) - t0 )) -gt 900 ] && { log "server $1 did not exit in 900s, SIGKILL"; kill -9 "$1"; break; }; sleep 2; done
  log "server $1 exited after $(( $(date +%s) - t0 ))s"
  "$2/bin/ray" stop --force >/dev/null 2>&1; sleep 3
}
run_variant(){
  local v=$1 venv=$2 rd=$ROOT/$1
  log "===== $v start (venv $venv)"
  rm -rf "$rd"; mkdir -p "$rd"; cd "$rd" || return 1
  cat > server.toml <<T
[server]
host = "127.0.0.1"
port = $PORT
data_dir = ".acquirium"
timeseries_backend = "duckdb"
duckdb_path = ".acquirium/timeseries.duckdb"
recreate = true
T
  cat > driver.toml <<T
[driver]
server_url  = "127.0.0.1"
server_port = $PORT
use_ssl     = false

[[drivers]]
spec              = "$HOME/bench/dpr_driver.py:DPRDriver"
interval          = 30.0
insert_batch_rows = 50000
source_id         = "dpr-trailer"
watch_dir         = "$DATA"
glob              = "*.csv"
format            = "wide"
date_col          = "Date"
clock_col         = "Time"
date_format       = "%m/%d/%Y %I:%M:%S %p"
header_contains   = ["Date", "Time"]
null_values       = ["NaN"]
T
  setsid "$venv/bin/acquirium" server --config server.toml > server.log 2>&1 < /dev/null &
  local spid=$!; echo "$spid" > server.pid; log "$v server pid $spid"
  wait_health "$spid" server.log || { echo FAIL_BOOT > STATUS; stop_server "$spid" "$venv"; return 1; }
  local db=.acquirium/timeseries.duckdb
  echo "db_empty_bytes $(stat -c %s $db)" > ingest.txt
  local t_start=$(date +%s); echo "t_driver_start $t_start $(date -u -d @$t_start +%FT%TZ)" >> ingest.txt
  "$venv/bin/acquirium" driver start driver.toml > driver_start.log 2>&1; echo "driver_start_rc $? after $(( $(date +%s) - t_start ))s" >> ingest.txt
  echo "# epoch files rows db_bytes wal_bytes server_rss_kb ray_rss_kb merges mem_avail_gb" > progress.log
  local n=0 last_n=0 last_t=$(date +%s) status=OK
  while :; do
    sleep 30
    n=$(grep -c "\.csv — inserted" server.log)
    local rows=$(grep "\.csv — inserted" server.log | sed 's/.*inserted \([0-9]*\).*/\1/' | awk '{s+=$1} END {print s+0}')
    local wal=0; [ -f $db.wal ] && wal=$(stat -c %s $db.wal)
    echo "$(date +%s) $n $rows $(stat -c %s $db) $wal $(rss_sid $spid) $(rss_ray) $(grep -c STAGING_MERGE server.log) $(awk '/MemAvailable/ {printf "%.1f", $2/1048576}' /proc/meminfo)" >> progress.log
    [ "$n" -ge "$NFILES" ] && break
    if [ "$n" -gt "$last_n" ]; then last_n=$n; last_t=$(date +%s); elif [ $(( $(date +%s) - last_t )) -gt $STALL_S ]; then log "$v STALL at $n files"; status=STALL; break; fi
    kill -0 "$spid" 2>/dev/null || { log "$v server died during ingest"; status=SERVER_DIED; break; }
  done
  local t_end=$(date +%s); echo "t_ingest_end $t_end $(date -u -d @$t_end +%FT%TZ) files $n status $status" >> ingest.txt
  log "$v ingest $status: $n files in $(( t_end - t_start ))s"
  curl -s --max-time 120 -X POST $BASE/drivers/stop -H 'Content-Type: application/json' -d '{"name": "DPRDriver"}' >> ingest.txt; echo >> ingest.txt
  sleep 10
  echo "db_bytes_before_shutdown $(stat -c %s $db) wal $( [ -f $db.wal ] && stat -c %s $db.wal || echo 0 )" >> ingest.txt
  local t0=$(date +%s); stop_server "$spid" "$venv"; echo "server_shutdown_s $(( $(date +%s) - t0 ))" >> ingest.txt
  echo "db_bytes_after_shutdown $(stat -c %s $db) wal $( [ -f $db.wal ] && stat -c %s $db.wal || echo 0 )" >> ingest.txt
  sed 's/\x1b\[[0-9;]*m//g' server.log > server_clean.log
  grep -m1 "\.csv — inserted" server_clean.log | cut -c1-120 >> ingest.txt
  grep "\.csv — inserted" server_clean.log | tail -1 | cut -c1-120 >> ingest.txt
  grep STAGING_MERGE server_clean.log >> ingest.txt
  echo "tracebacks $(grep -c Traceback server_clean.log) errors $(grep -c ' ERROR ' server_clean.log)" >> ingest.txt
  if [ "$v" = v3 ]; then
    log "$v final merge"; t0=$(date +%s)
    "$venv/bin/python" "$HOME/bench/final_merge.py" "$db" > final_merge.log 2>&1
    echo "final_merge_wall_s $(( $(date +%s) - t0 )) $(grep FINAL_MERGE final_merge.log)" >> ingest.txt
    echo "db_bytes_after_final_merge $(stat -c %s $db)" >> ingest.txt
  fi
  log "$v storage breakdown"
  ls -l .acquirium/ > storage_ls.txt
  "$venv/bin/python" "$HOME/bench/duck_storage.py" "$db" > storage.txt 2>&1
  "$venv/bin/python" "$HOME/bench/duck_blocks.py" "$db" > blocks.txt 2>&1
  if [ "$v" != v1 ]; then "$venv/bin/python" "$HOME/bench/duck_compact.py" "$db" "$rd/compact.duckdb" > compact.txt 2>&1; rm -f "$rd/compact.duckdb"; fi
  log "$v queries"
  sed -i 's/recreate = true/recreate = false/' server.toml
  setsid "$venv/bin/acquirium" server --config server.toml > server2.log 2>&1 < /dev/null &
  spid=$!; echo "$spid" > server2.pid
  if wait_health "$spid" server2.log; then
    if [ ! -f "$ROOT/queries_1000.json" ]; then
      "$venv/bin/python" "$HOME/bench/queries.py" gen $BASE "$HOME/bench/property_to_column.json" "$ROOT/queries_1000.json" > "$ROOT/queries_gen.log" 2>&1 || log "query gen FAILED (see $ROOT/queries_gen.log)"
    fi
    t0=$(date +%s)
    SERVER_SID=$spid "$venv/bin/python" "$HOME/bench/queries.py" run $BASE "$ROOT/queries_1000.json" $REPS $WARMUP "$rd/queries.json" 2> queries.stderr
    log "$v queries done in $(( $(date +%s) - t0 ))s rc=$?"
    stop_server "$spid" "$venv"
  else
    echo FAIL_REBOOT > STATUS
  fi
  echo "$status" > STATUS; log "===== $v done ($status)"
}
run_variant v1 "$HOME/venv_main"
run_variant v2 "$HOME/venv_v2"
run_variant v3 "$HOME/venv_v3"
log "ALL DONE"; touch "$ROOT/ALL_DONE"
