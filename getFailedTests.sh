#!/usr/bin/env bash

echo "Looking for failed tests..."
for i in test.log/*.log; do
  pkg=${i%.log}

  pkg=${pkg%_slot*}
  #last element is class name
  pkg=${pkg%.*}
  pkg=${pkg#test.log/}
  for m in $(cat $i | grep "\\[ERROR\\]" | grep -A 10 "\\[ERROR\\] Failures:" | grep "\\[ERROR\\]   " | cut -f1 -d: | cut -c11- | cut -f1 -d"»" | cut -f1 -d"("); do
    echo "$pkg.$m"
  done
  for m in $(cat $i | grep "\\[ERROR\\]" | grep -A 10 "\\[ERROR\\] Errors:" | grep "\\[ERROR\\]   " | cut -f1 -d: | cut -c11- | cut -f1 -d"»" | cut -f1 -d"("); do
    echo "$pkg.$m"
  done
done | sort -u
