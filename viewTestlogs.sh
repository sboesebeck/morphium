#!/usr/bin/env bash

files=""
for j in $(for i in test.log/slot_*/slot.log; do
  n=${i%%/slot.log}
  n=${n##*_}
  f=$(tail -n1 $i | grep -v "completed" | cut -f 2 -d: | cut -f3 -d' ')
  if [ -z "$f" ]; then
    continue
  fi
  echo "test.log/slot_$n/$f.log"
done); do
  files="$files $j"
done
multitail -s 2 $(eval echo $files)
