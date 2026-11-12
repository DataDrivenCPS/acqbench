#!/usr/bin/env bash
# Query-only pass on the existing full store in ~/var_runs/fix3. usage: run_queries.sh TAG VENV QSET [gen]
set -u
TAG=$1; VENV=$2; QSET=$3; GEN=${4:-}
ROOT=$HOME/var_runs; RD=$ROOT/fix3; PORT=8080; BASE=http://127.0.0.1:$PORT; MASTER=$ROOT/run_fix3.log
log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$MASTER"; }
cd "$RD"; sed -i 's/recreate = true/recreate = false/' server.toml
log "===== $TAG: query-only pass, venv $VENV ($(git -C ~/acq_fix3 rev-parse --short HEAD)), set $(basename $QSET)"
setsid "$VENV/bin/acquirium" server --config server.toml > ${TAG}_server.log 2>&1 < /dev/null &
spid=$!; t0=$(date +%s)
while :; do kill -0 $spid 2>/dev/null || { log "server died"; exit 1; }; [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 $BASE/health)" = 200 ] && break; [ $(( $(date +%s) - t0 )) -gt 1800 ] && { log "not healthy"; exit 1; }; sleep 5; done
log "healthy after $(( $(date +%s) - t0 ))s"
if [ -n "$GEN" ] && [ ! -f "$QSET" ]; then "$VENV/bin/python" "$HOME/bench/queries100b.py" $BASE "$HOME/bench/property_to_column.json" "$QSET" > ${TAG}_gen.log 2>&1; log "gen: $(tail -1 ${TAG}_gen.log)"; fi
t0=$(date +%s)
SERVER_SID=$spid "$VENV/bin/python" "$HOME/bench/queries.py" run $BASE "$QSET" 10 2 "$RD/${TAG}_queries.json" 2> ${TAG}_queries.stderr
log "$TAG queries done in $(( $(date +%s) - t0 ))s rc=$?"
kill -TERM $spid; while kill -0 $spid 2>/dev/null; do sleep 2; done; "$VENV/bin/ray" stop --force >/dev/null 2>&1
log "===== $TAG done"; touch "$RD/${TAG}_DONE"
