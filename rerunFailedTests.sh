#!/bin/bash

failed=$(./getFailedTests.sh)


for f in $failed; do
  cls=${f%#*}
  m=${f#*#}
  m=$(echo "$m" | sed -e 's/\/.*$' )
  echo "Re-Running tests in $cls Method $m"
  ./runtests.sh --nodel $f $m
done

