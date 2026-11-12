#!/usr/bin/env bash
# ums-duckdb-fix-3 on the same protocol as run_all.sh, then continue the same store with the remaining 4,193 files.
# Phase A: fresh store, ~/data/dpr1000 (1000 files), storage breakdown, 50-query replay  -> ~/var_runs/fix3/phaseA_*
# Phase B: same store, recreate=false, ~/data/dpr_rest (4193 files), breakdown, 50-query replay -> ~/var_runs/fix3/phaseB_*
set -u
ROOT=$HOME/var_runs; RD=$ROOT/fix3; VENV=$HOME/venv_fix3; PORT=8080; BASE=http://127.0.0.1:$PORT
REPS=10; WARMUP=2; STALL_S=7200; QSET=$ROOT/queries_1000.json
MASTER=$ROOT/run_fix3.log
log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$MASTER"; }
rss_sid(){ ps -eo sid=,rss= | awk -v s="$1" '$1==s {t+=$2} END {print t+0}'; }
rss_ray(){ ps -eo rss=,comm= | awk '$2 ~ /^(raylet|gcs_server|ray::)/ {t+=$1} END {print t+0}'; }
wait_health(){ local t0=$(date +%s)
  while :; do kill -0 "$1" 2>/dev/null || { log "server $1 died (see $2)"; return 1; }
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 $BASE/health)" = 200 ] && { log "healthy after $(( $(date +%s) - t0 ))s"; return 0; }
    [ $(( $(date +%s) - t0 )) -gt 1800 ] && { log "server not healthy after 1800s"; return 1; }; sleep 5; done; }
stop_server(){ kill -TERM "$1" 2>/dev/null; local t0=$(date +%s)
  while kill -0 "$1" 2>/dev/null; do [ $(( $(date +%s) - t0 )) -gt 1800 ] && { log "server $1 did not exit in 1800s, SIGKILL"; kill -9 "$1"; break; }; sleep 2; done
  log "server $1 exited after $(( $(date +%s) - t0 ))s"; "$VENV/bin/ray" stop --force >/dev/null 2>&1; sleep 3; }
write_driver(){ cat > driver.toml <<T
[driver]
server_url  = "127.0.0.1"
server_port = $PORT
use_ssl     = false

[[drivers]]
spec              = "$HOME/bench/dpr_driver.py:DPRDriver"
interval          = 30.0
insert_batch_rows = 50000
source_id         = "dpr-trailer"
watch_dir         = "$1"
glob              = "*.csv"
format            = "wide"
date_col          = "Date"
clock_col         = "Time"
date_format       = "%m/%d/%Y %I:%M:%S %p"
header_contains   = ["Date", "Time"]
null_values       = ["NaN"]
T
}
phase(){ # $1 tag(A|B) $2 data dir $3 nfiles $4 recreate(true|false)
  local tag=$1 data=$2 nfiles=$3 recreate=$4 p="phase$1" db=.acquirium/timeseries.duckdb
  log "===== fix3 phase $tag start: $nfiles files from $data (recreate=$recreate)"
  cd "$RD" || return 1
  cat > server.toml <<T
[server]
host = "127.0.0.1"
port = $PORT
data_dir = ".acquirium"
timeseries_backend = "duckdb"
duckdb_path = "$db"
recreate = $recreate
T
  write_driver "$data"
  setsid "$VENV/bin/acquirium" server --config server.toml > ${p}_server.log 2>&1 < /dev/null &
  local spid=$!; log "$p server pid $spid"
  wait_health "$spid" ${p}_server.log || { echo FAIL_BOOT > ${p}_STATUS; stop_server "$spid"; return 1; }
  echo "db_bytes_at_start $(stat -c %s $db)" > ${p}_ingest.txt
  local t_start=$(date +%s); echo "t_driver_start $t_start $(date -u -d @$t_start +%FT%TZ)" >> ${p}_ingest.txt
  "$VENV/bin/acquirium" driver start driver.toml > ${p}_driver_start.log 2>&1; echo "driver_start_rc $? after $(( $(date +%s) - t_start ))s" >> ${p}_ingest.txt
  echo "# epoch files rows db_bytes wal_bytes server_rss_kb ray_rss_kb mem_avail_gb" > ${p}_progress.log
  local n=0 last_n=0 last_t=$(date +%s) status=OK
  while :; do sleep 30
    n=$(grep -c "\.csv — inserted" ${p}_server.log)
    local rows=$(grep "\.csv — inserted" ${p}_server.log | sed 's/.*inserted \([0-9]*\).*/\1/' | awk '{s+=$1} END {print s+0}')
    local wal=0; [ -f $db.wal ] && wal=$(stat -c %s $db.wal)
    echo "$(date +%s) $n $rows $(stat -c %s $db) $wal $(rss_sid $spid) $(rss_ray) $(awk '/MemAvailable/ {printf "%.1f", $2/1048576}' /proc/meminfo)" >> ${p}_progress.log
    [ "$n" -ge "$nfiles" ] && break
    if [ "$n" -gt "$last_n" ]; then last_n=$n; last_t=$(date +%s); elif [ $(( $(date +%s) - last_t )) -gt $STALL_S ]; then log "$p STALL at $n files"; status=STALL; break; fi
    kill -0 "$spid" 2>/dev/null || { log "$p server died during ingest"; status=SERVER_DIED; break; }
  done
  local t_end=$(date +%s); echo "t_ingest_end $t_end $(date -u -d @$t_end +%FT%TZ) files $n status $status" >> ${p}_ingest.txt
  log "$p ingest $status: $n files in $(( t_end - t_start ))s"
  curl -s --max-time 300 -X POST $BASE/drivers/stop -H 'Content-Type: application/json' -d '{"name": "DPRDriver"}' >> ${p}_ingest.txt; echo >> ${p}_ingest.txt; sleep 10
  echo "db_bytes_before_shutdown $(stat -c %s $db) wal $( [ -f $db.wal ] && stat -c %s $db.wal || echo 0 )" >> ${p}_ingest.txt
  local t0=$(date +%s); stop_server "$spid"; echo "server_shutdown_s $(( $(date +%s) - t0 ))" >> ${p}_ingest.txt
  echo "db_bytes_after_shutdown $(stat -c %s $db) wal $( [ -f $db.wal ] && stat -c %s $db.wal || echo 0 )" >> ${p}_ingest.txt
  sed 's/\x1b\[[0-9;]*m//g' ${p}_server.log > ${p}_server_clean.log
  grep -m1 "\.csv — inserted" ${p}_server_clean.log | cut -c1-120 >> ${p}_ingest.txt
  grep "\.csv — inserted" ${p}_server_clean.log | tail -1 | cut -c1-120 >> ${p}_ingest.txt
  echo "tracebacks $(grep -c Traceback ${p}_server_clean.log) errors $(grep -c ' ERROR ' ${p}_server_clean.log)" >> ${p}_ingest.txt
  log "$p storage breakdown"
  ls -l .acquirium/ > ${p}_storage_ls.txt
  "$VENV/bin/python" "$HOME/bench/duck_storage.py" "$db" > ${p}_storage.txt 2>&1
  "$VENV/bin/python" "$HOME/bench/duck_blocks.py" "$db" > ${p}_blocks.txt 2>&1
  "$VENV/bin/python" "$HOME/bench/duck_compact.py" "$db" "$RD/compact.duckdb" > ${p}_compact.txt 2>&1; rm -f "$RD/compact.duckdb"
  log "$p queries"
  sed -i 's/recreate = true/recreate = false/' server.toml
  setsid "$VENV/bin/acquirium" server --config server.toml > ${p}_server2.log 2>&1 < /dev/null &
  spid=$!
  if wait_health "$spid" ${p}_server2.log; then
    t0=$(date +%s)
    SERVER_SID=$spid "$VENV/bin/python" "$HOME/bench/queries.py" run $BASE "$QSET" $REPS $WARMUP "$RD/${p}_queries.json" 2> ${p}_queries.stderr
    log "$p queries done in $(( $(date +%s) - t0 ))s rc=$?"
    stop_server "$spid"
  else echo FAIL_REBOOT > ${p}_STATUS; fi
  echo "$status" > ${p}_STATUS; log "===== fix3 phase $tag done ($status)"
}
rm -rf "$RD"; mkdir -p "$RD"
phase A "$HOME/data/dpr1000" 1000 true
phase B "$HOME/data/dpr_rest" 4193 false
log "FIX3 ALL DONE"; touch "$RD/ALL_DONE"
