#!/bin/bash
# Paired lockstep pass, driven from the workstation over ssh.
# Each job starts on startbench-1 (main) and startbench-5 (local-runtime-init)
# at the same moment; the next job starts only after both finish.
HOST=mete@10.167.210.155
R=/home/vmuser/acq_startup
while read -r name rep exact tpl mode; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  args="--name $name --rep $rep --exact $exact --mode $mode"
  [[ "$tpl" != "-" ]] && args="$args --template $R/templates/$tpl"
  echo "$(date +%T) START $name $mode-$rep"
  ssh -n -o BatchMode=yes $HOST "
    docker exec -e RESULTS_ROOT=$R/bench/results_pair startbench-1 python3 $R/bench/run_one.py $args > /tmp/pair_main.log 2>&1 &
    docker exec -e RESULTS_ROOT=$R/bench/results_pair -e VENV_DIR=$R/venv_lri startbench-5 python3 $R/bench/run_one.py $args > /tmp/pair_lri.log 2>&1 &
    wait
    tail -1 /tmp/pair_main.log; tail -1 /tmp/pair_lri.log
  " 2>&1 | grep -v Emulate
  sleep 5
done < "${1:-pair.jobs}"
echo "$(date +%T) ALLDONE"
