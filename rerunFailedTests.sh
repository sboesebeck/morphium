#!/bin/bash

failed=$(./getFailedTests.sh --noreason)


for f in $failed; do
  echo "F: $f"
  cls=${f%#*}
  m=${f#*#}
  m=${m/(*/}
  # m=$(echo "$m" | sed -e 's/\\(.*$//' )
  echo "Re-Running tests in $cls Method $m"
  ./runtests.sh --nodel $cls $m
done

