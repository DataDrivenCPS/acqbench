#!/usr/bin/env bash
# Generate queries_100b.json on the current head, then replay it at d0cc2c4 (phaseH) and 0a8eb80 (phaseI).
set -u
ROOT=$HOME/var_runs; QSET=$ROOT/queries_100b.json; MASTER=$ROOT/run_fix3.log
log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$MASTER"; }
switch(){ (cd ~/acq_fix3 && git checkout -q --detach "$1" && ~/.local/bin/uv pip install -q --python ~/venv_fix3/bin/python -e ~/acq_fix3 requests pyarrow && git rev-parse --short HEAD); }
log "switch to d0cc2c4: $(switch d0cc2c4)"
"$HOME/bench/run_queries_b.sh" phaseH "$HOME/venv_fix3" "$QSET" gen
log "switch to 0a8eb80: $(switch 0a8eb80)"
"$HOME/bench/run_queries_b.sh" phaseI "$HOME/venv_fix3" "$QSET"
log "TWO HEADS DONE"; touch "$ROOT/fix3/TWOHEADS_DONE"
