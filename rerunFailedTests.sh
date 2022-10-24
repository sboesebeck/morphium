#!/bin/bash

failed=$(./getFailedTests.sh)


for f in $failed; do
  echo "Re-Running tests in $f"
  ./runtests.sh --nodel $f
done

