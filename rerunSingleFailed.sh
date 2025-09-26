#!/usr/bin/env bash

cd $(dirname $0)
m=""

echo "Choose failed test"

tst=$(for i in test.log/*.log; do
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
done | fzf)
#tst=$(cat test.log/*.log | grep "\\[ERROR\\]" | grep -A 10 "\\[ERROR\\] Failures:" | grep "\\[ERROR\\]   " | cut -f1 -d: | cut -c11- | sort | fzf)
echo "Chose $tst"

cls=${tst%.*}
m=${tst##*.}

echo "Method $m in Class $cls"
# f=$(./runtests.sh --stats --noreason --nosum | grep -v "Calculating" | fzf)

mvn -Dtest=$cls#$m "$@" compile test-compile test
