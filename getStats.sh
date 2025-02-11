#!/bin/bash
RD='\033[0;31m'
GN='\033[0;32m'
BL='\033[0;34m'
YL='\033[0;33m'
MG='\033[0;35m'
CN='\033[0;36m'
CL='\033[0m'

getFailure() {
  cls=$1
  type=$2
  # echo "Checking $cls for type $type"
  #sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml
  failures=$(cat ./target/surefire-reports/TEST-"$cls".xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep 'testcase\[[0-9]*\].'$type'\[".*@type"\]' | cut -f 2 -d '[' | cut -f1 -d ']')
  for id in $(echo $failures); do
    # echo "ID: '$id'"
    # xq . ./target/surefire-reports/TEST-$cls.xml | gron | grep "testcase\\[$id\\]\\[\"@name\"\\]" | cut -f2 -d= | tr -d '"; '
    m="$(cat ./target/surefire-reports/TEST-$cls.xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep "testcase\\[$id\\]\\[\".*@name\"\\]" | cut -f2 -d= | tr -d '"; ')"
    if [ $noreason -eq 1 ]; then
      echo "$cls#$m"
    else
      err="$(cat ./target/surefire-reports/TEST-$cls.xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep "testcase\\[$id\\].$type\\[\".*@type\"\\]" | cut -f2 -d= | tr -d '"; ')"
      msg="$(cat ./target/surefire-reports/TEST-$cls.xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep "testcase\\[$id\\].$type\\[\".*@message\"\\]" | cut -f2 -d= | tr -d '"; ')"
      echo "$cls#$m - $err ($msg)"
    fi
  done
  if cat ./target/surefire-reports/TEST-"$cls".xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep 'testcase.'$type'\[".*@type"\]' >/dev/null; then
    #found single test
    m=$(cat ./target/surefire-reports/TEST-"$cls".xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep 'testcase\[".*@name"\]' | cut -f2 -d= | tr -d "!; ')")
    if [ $noreason -eq 1 ]; then
      echo "$cls#$m"
    else
      err=$(cat ./target/surefire-reports/TEST-"$cls".xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep 'testcase.'$type'\[".*@type"\]' | cut -f2 -d= | tr -d "!; ')")
      msg=$(cat ./target/surefire-reports/TEST-"$cls".xml | sed -e 's/#27...../;/g' | sed -e 's/< *$//g' | yq -o json -p xml | gron | grep 'testcase.'$type'\[".*@message"\]' | cut -f2 -d= | tr -d "!; ')")

      echo "$cls#$m - $err ($msg)"
    fi
  fi

}

noreason=0
nosum=0

while [ $# -ne 0 ]; do
  case "$1" in
  --noreason)
    noreason=1
    shift
    ;;
  --nosum)
    nosum=1
    shift
    ;;
  *)
    echo "Error - only --noreason or --noshift allowed! $1 unknown"
    exit 1
    ;;
  esac
done
fail=0
for i in $(grep -a "Tests run: .*in " test.log/*.log | cut -f4 -d: | cut -f1 -d,); do
  ((fail = fail + i))
done
run=0
for i in test.log/*; do
  fn=$(basename $i)
  fn=src/test/java/$(echo "${fn%%.log}" | tr "." "/").java
  testMethods=$(grep -E "@Test" $fn | cut -f2 -d: | grep -vc '^ *//')
  testMethods3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $fn | cut -f2 -d: | grep -vc '^ *//')
  testMethods2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $fn | cut -f2 -d: | grep -vc '^ *//')
  testMethods1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $fn | cut -f2 -d: | grep -vc '^ *//')
  # testMethodsP=$(grep -E "@ParameterizedTest" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
  ((testMethods = testMethods + 3 * testMethods3 + testMethods2 * 2 + testMethods1))
  ((run = run + testMethods))
done
# for i in $(grep -a "Tests run: .*in " test.log/*.log | cut -f3 -d: | cut -f1 -d,); do
# 	((run = run + i))
# done
err=0
for i in $(grep -a "Tests run: .*in " test.log/*.log | cut -f5 -d: | cut -f1 -d,); do
  ((err = err + i))
done
((sum = fail + err))
if [ "$nosum" -eq 0 ]; then
  echo "Total tests run    : $run"
  echo "Total unsuccessful : $sum"
  echo "Tests failed       : $fail"
  echo "Tests with errors  : $err"
fi
if [ "$fail" -eq 0 ] && [ "$err" -eq 0 ]; then
  exit
else
  echo ""
  for cls in $(grep -E "Tests run: " test.log/* | grep FAILURE | cut -f2 -d! | cut -f4 -d' '); do
    # for cls in $(grep -E "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' | grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'); do
    # echo "Getting failures in $cls"
    if [ ! -e ./target/surefire-reports/TEST-$cls.xml ]; then
      # if [ "$noreason" -eq 1 ]; then
      echo "$cls"
      # else
      #     echo "$cls (no more details available)"
      # fi
      continue
    fi
    getFailure $cls "failure"
    getFailure $cls "error"
  done
fi
