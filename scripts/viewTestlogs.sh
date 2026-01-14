#!/usr/bin/env bash
cd $(dirname $0)/..
files=""
for j in $(for i in test.log/slot_*/slot.log test.log/*.log; do
  n=${i%%/slot.log}
  n=${n##*_}
  f=$(tail -n1 $i | grep -v "completed" | cut -f 2 -d: | cut -f3 -d' ')
  if [ -z "$f" ]; then
    continue
  fi
  echo "$i"
done); do
  files="$files $j"
done
multitail -s 2 $(eval echo $files)
