#!/bin/bash
# usage: jobs.sh <jobfile>   each line: name rep exact template(-|path) mode [keep]
cd ~/acq_startup/bench
T=~/acq_startup/templates
while read -r name rep exact tpl mode keep; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  args=(--name "$name" --rep "$rep" --exact "$exact" --mode "$mode")
  [[ "$tpl" != "-" ]] && args+=(--template "$T/$tpl")
  [[ -n "$keep" ]] && args+=(--keep "$T/$keep")
  echo "$(date +%T) START $name $mode-$rep"
  python3 run_one.py "${args[@]}" || echo "$(date +%T) FAILED $name $mode-$rep"
  sleep 5
done < "$1"
echo "$(date +%T) ALLDONE"
