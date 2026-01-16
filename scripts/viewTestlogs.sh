#!/usr/bin/env bash
cd $(dirname $0)/..
files=""
for j in $(for i in test.log/slot_*/slot.log test.log/slot_*/*.log test.log/*.log; do
  n=${i%%/slot.log}
  n=${n##*_}
  if echo "$i" | grep "slot.*/slot.log" >/dev/null; then
    continue
  fi
  if [ ! -e "$i" ]; then
    continue
  fi
  f=$(tail -n1 $i | grep -v "completed" | grep -v "Help 1" | cut -f 2 -d: | cut -f3 -d' ')
  if [ -z "$f" ]; then
    continue
  fi
  echo "$i"
done); do
  files="$files $j"
done
multitail -s 2 $(eval echo $files)
