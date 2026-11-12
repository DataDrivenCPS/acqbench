#!/usr/bin/env bash
set -u
ROOT=$HOME/var_runs; RD=$ROOT/fix3; VENV=$HOME/venv_fix3x; PORT=8080; BASE=http://127.0.0.1:$PORT; MASTER=$ROOT/run_fix3.log
log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$MASTER"; }
cd "$RD"; sed -i 's/recreate = true/recreate = false/' server.toml
log "===== fix3 phase C: same full store, partitioned_execution disabled on read connections (venv_fix3x)"
setsid "$VENV/bin/acquirium" server --config server.toml > phaseC_server.log 2>&1 < /dev/null &
spid=$!; t0=$(date +%s)
while :; do kill -0 $spid 2>/dev/null || { log "server died"; exit 1; }; [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 $BASE/health)" = 200 ] && break; [ $(( $(date +%s) - t0 )) -gt 1800 ] && { log "not healthy"; exit 1; }; sleep 5; done
log "healthy after $(( $(date +%s) - t0 ))s"; t0=$(date +%s)
SERVER_SID=$spid "$VENV/bin/python" "$HOME/bench/queries.py" run $BASE "$ROOT/queries_1000.json" 10 2 "$RD/phaseC_queries.json" 2> phaseC_queries.stderr
log "phase C queries done in $(( $(date +%s) - t0 ))s rc=$?"
kill -TERM $spid; while kill -0 $spid 2>/dev/null; do sleep 2; done; "$VENV/bin/ray" stop --force >/dev/null 2>&1
log "===== fix3 phase C done"; touch "$RD/PHASEC_DONE"
