#!/usr/bin/env bash

class=$(fd .java src/test | fzf)
if [ -z $class ]; then
  exit
fi
tstClass=$(echo $class | sed -e 's!src/test/java/!!' | tr '/' '.' | sed -e 's/.java$//')
# echo "You chose $class -> $tstClass"
test=$(egrep -A3 '@Parameterized|@Test' $class | grep "public void" | cut -f1 -d'(' | sed -e 's/public void//' | tr -d ' ' | fzf)

echo "Running in class $tstClass"
if [ -z $test ]; then
  echo "Running all tests"
  echo "mvn \"$@\" -Dtest=$tstClass test" | pbcopy
  mvn "$@" -Dtest=$tstClass test
else
  echo "line to execute: (in clipboard)"
  echo "mvn $@ -Dtest=$tstClass#$test test"
  echo "mvn $@ -Dtest=$tstClass#$test test" | pbcopy
  echo "Continue?"
  read
  mvn "$@" -Dtest=$tstClass#$test test
fi
