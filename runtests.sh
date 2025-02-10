#!/bin/bash
RD='\033[0;31m'
GN='\033[0;32m'
BL='\033[0;34m'
YL='\033[0;33m'
MG='\033[0;35m'
CN='\033[0;36m'
CL='\033[0m'

function createFileList() {
  rg -l "@Test" | grep ".java" >files.lst
  rg -l "@ParameterizedTest" | grep ".java" >>files.lst

  sort -u files.lst | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >files.txt
  sort -u files.lst | grep "$p" >files.tmp && mv -f files.tmp files.lst
  rg -A2 "^ *@Disabled" | grep -B2 "public class" | grep : | cut -f1 -d: >disabled.lst
  cat files.lst | while read l; do
    if grep $l disabled.lst; then
      echo "$l disabled"
    else
      echo "$l" >>files.tmp
    fi
  done
  mv files.tmp files.lst

}
function quitting() {
  echo -e "${RD}Shutting down...${CL} current test $t"

  if [ -e test.pid ]; then
    kill -9 $(<test.pid) >/dev/null 2>&1
  fi
  if [ -e fail.pid ]; then
    kill -9 $(<fail.pid) >/dev/null 2>&1
  fi
  rm -f test.pid fail.pid >/dev/null 2>&1
  echo "Removing unfinished test $t"
  rm -f test.log/$t.log
  ./getFailedTests.sh >failed.txt
  echo "List of failed tests in failed.txt"
  cat failed.txt
  rm -f run.lck disabled.lst
  exit
}

nodel=0
skip=0
refresh=5
logLength=15
numRetries=1
totalRetries=0

while [ "q$1" != "q" ]; do

  if [ "q$1" == "q--nodel" ]; then
    nodel=1
    shift
  elif [ "q$1" == "q--skip" ]; then
    skip=1
    shift
  elif [ "q$1" == "q--restart" ]; then
    rm -rf test.log
    rm -f startTS
    mkdir test.log
    shift
  elif [ "q$1" == "q--logs" ]; then
    shift
    logLength=$1
    shift
  elif [ "q$1" == "q--retry" ]; then
    shift
    numRetries=$1
    shift
  elif [ "q$1" == "q--refresh" ]; then
    shift
    refresh=$1
    shift
  else
    echo "Do not know option $1 - assuming it is a testname"
    break
  fi
done
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
  if [ -z "$(ls -A 'test.log')" ]; then
    count=0
  else
    count=$(ls -1 test.log/* | wc -l)
  fi
  if [ "$count" -gt 0 ]; then
    echo -e "There are restults from old tests there - ${BL}c${CL}ontinue tests (c), erase old logs and ${BL}r${CL}estart all (r) or ${RD}a${CL}bort (CTRL-C / a)?"
    read q
    case $q in
    c)
      echo "Will continue where we left of..."
      skip=1
      ;;
    r)
      echo -e "${YL}ATTENTION: ${CL}Erase all and restart..."
      skip=0
      ;;
    a)
      echo "Aborting..."
      exit
      ;;
    *)
      echo "${RD}Unknown answer${CL} - aborting"
      exit 1
      ;;
    esac
  fi
fi
#trap quitting EXIT
trap quitting SIGINT
trap quitting SIGHUP

p=$1
if [ "q$p" == "q" ]; then
  # echo "No pattern given, running all tests"
  p="."
# else
# 	echo "Checking for tests whose classes match $p"
fi

m=$2
if [ "q$2" == "q" ]; then
  # echo "No pattern for methods given"
  m="."
# else
# 	echo "Checking for test-methods matching $m"
fi

createFileList
if [ "$skip" -ne 0 ]; then
  echo -e "${BL}Info:${CL} Skipping tests already run"
  if [ -z "$(ls -A 'test.log')" ]; then
    echo "Nothing to skip"
  else
    for i in test.log/*; do
      i=$(basename $i)
      i=${i%%.log}
      echo -e "${BL}info: ${CL}not rerunning $i"
      grep -v $i files.txt >files.tmp
      mv files.tmp files.txt
    done
  fi
fi
# read
cnt=$(wc -l <files.txt | tr -d ' ')
if [ "$cnt" -eq 0 ]; then
  echo "no matching class found for $p"
  exit 1
fi
disabled=$(rg -C1 "^ *@Disabled" | grep -C1 "@Test" | grep : | cut -f1 -d: | wc -l)
disabled3=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances"\)' | grep : | cut -f1 -d: | wc -l)
disabled2=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstancesNo.*"\)' | grep : | cut -f1 -d: | wc -l)
disabled1=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances.*Only"\)' | grep : | cut -f1 -d: | wc -l)
testMethods=$(grep -E "@Test" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
# testMethodsP=$(grep -E "@ParameterizedTest" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
((testMethods = testMethods + 3 * testMethods3 + testMethods2 * 2 + testMethods1 - disabled - disabled3 * 3 - disabled2 * 2 - disabled1))
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
  echo -e "${BL}Info:${CL} Cleaning up - cleansing logs..."
  rm -rf test.log >/dev/null 2>&1
  rm -f startTS >/dev/null 2>&1
  mkdir test.log
fi
if [ "$nodel" -eq 0 ]; then
  echo -e "${BL}Info:${CL} Cleaning up - mvn clean..."
  mvn clean >/dev/null
fi
echo -e "${BL}Info:${CL} Compiling..."
mvn compile test-compile >/dev/null || {
  echo -e "${RD}Error:${CL} Compilation failed!"
  exit 1
}

tst=0
echo -e "${GN}Starting tests..${CL}" >failed.txt
# running getfailedTests in background
{
  touch run.lck
  while [ -e run.lck ]; do
    ./getFailedTests.sh >failed.tmp
    mv failed.tmp failed.txt
    sleep 4
  done >/dev/null 2>&1

} &

echo $! >fail.pid
if [ -e startTS ]; then
  start=$(<startTS)
else
  start=$(date +%s)
  echo "$start" >startTS
fi
testsRun=0
unsuc=0
fail=0
err=0
for t in $(<files.txt); do
  ((tst = tst + 1))
  tm=$(date +%s)
  if [ "$m" == "." ]; then
    mvn -Dsurefire.useFile=false test -Dtest="$t" >test.log/"$t".log 2>&1 &
    echo $! >test.pid
  else
    mvn -Dsurefire.useFile=false test -Dtest="$t#$m" >"test.log/$t.log" 2>&1 &
    echo $! >test.pid
  fi
  while true; do
    testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
    unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
    fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
    err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
    ((d = $(date +%s) - start))
    lmeth=$(grep -E "@Test|@ParameterizedTest" $(grep "$t" files.lst) | cut -f2 -d: | grep -vc '^ *//')
    clear

    if [ ! -z "$testsRun" ] && [ "$testsRun" -ne 0 ] && [ "$m" == "." ] && [ "$p" == "." ]; then
      ((spt = d / testsRun))
      ((etl = spt * testMethods - d))
      ((etlm = etl / 60))
      echo -e "Date: $(date) - Running ${MG}$d${CL}sec - Tests run: ${BL}$testsRun${CL} ~ ${YL}$spt${CL} sec per test - ETL: ${MG}$etl${CL} =~ $etlm minutes"
    elif [ "$m" != "." ]; then
      echo -e "Date: $(date) - Running ${MG}$d${CL}sec - Runing Tests matching ${BL}$m$CL  in ${YL}$t$CL ($lmeth methods)"
    else
      echo -e "Date: $(date) - ${BL}Startup...$CL"

    fi
    echo "---------------------------------------------------------------------------------------------------------"
    echo -e "Running tests in ${YL}$t${CL}  - #${MG}$tst${CL}/${BL}$cnt$CL"
    echo -e "Total number methods to run in matching classes ${CN}$testMethods$CL"
    echo -e "Number of test methods in ${YL}$t${CL}: ${GN}$lmeth$CL"
    if [ "$totalRetries" -ne 0 ]; then
      echo -e "Had to retry ${YL}$totalRetries${CL} times"
    fi
    if [ "$m" != "." ]; then
      echo -e " Tests matching: ${BL}$m${CL}"
    fi
    echo "---------------------------------------------------------------------------------------------------------"

    C1="$GN"
    C2="$GN"
    C3="$GN"

    ((dur = $(date +%s) - tm))
    if [ -z "$testsRun" ]; then
      echo -e "....."
    elif [ "$m" != "." ] || [ "$p" != "." ]; then
      ((testsRun = testsRun - lmeth))

      echo -e "Total Tests run           :  ${BL}$testsRun${CL} done + ${CN}$lmeth$CL in progress / ${GN}$testMethods$CL"
      echo -e "Tests fails / errors      : ${C2}$fail${CL} /${C3}$err$CL"
      echo -e "Total Tests unsuccessful  :  ${C1}$unsuc${CL}"
      echo -e "Duration: ${MG}${dur}s${CL}"
      if [ "$unsuc" -gt 0 ]; then
        echo -e "----------${RD} Failed Tests: $CL---------------------------------------------------------------------------------"
        tail -n+5 failed.txt
      fi
      echo -e "---------- ${CN}LOG:$CL--------------------------------------------------------------------------------------"
      tail -n $logLength test.log/"$t".log
      echo "---------------------------------------------------------------------------------------------------------"
    else
      if [ "$unsuc" -gt 0 ]; then
        C1=$RD
      fi
      if [ "$fail" -gt 0 ]; then
        C2=$RD
      fi
      if [ "$err" -gt 0 ]; then
        C3=$RD
      fi
      ((prc = (testsRun + lmeth) * 100 / testMethods))
      echo -e "Total Tests run           :  ${BL}$testsRun${CL} done + ${CN}$lmeth$CL in progress / ${GN}$testMethods$CL ~ ${MB}$prc %${CL}"
      echo -e "Tests fails / errors      : ${C2}$fail${CL} /${C3}$err$CL"
      echo -e "Total Tests unsuccessful  :  ${C1}$unsuc${CL}"
      echo -e "Duration: ${MG}${dur}s${CL}"
      if [ "$unsuc" -gt 0 ]; then
        echo -e "----------${RD} Failed Tests: $CL---------------------------------------------------------------------------------"
        tail -n+5 failed.txt
      fi
      echo -e "---------- ${CN}LOG:$CL--------------------------------------------------------------------------------------"
      tail -n $logLength test.log/"$t".log
      echo "---------------------------------------------------------------------------------------------------------"
    fi
    # egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--' | pr -l1 -3 -t -w 280 || echo "none"

    # egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'  || echo "none"
    # egrep "] Running |Tests run:" test.log/* | grep -B1 FAILURE | cut -f2 -d']' || echo "none"
    jobs >/dev/null
    j=$(jobs | grep -E '\[[0-9]+\]' | wc -l)
    if [ "$j" -lt 2 ]; then
      break
    fi
    if [ $dur -gt 600 ]; then
      echo -e "${RD}Error:${CL} Test class runs longer than 10 minutes - killing it!"
      echo "TERMINATED DUE TO TIMEOUT" >>test.log/$t.log
      kill $(<test.pid)
    fi
    sleep $refresh
  done
  ./getFailedTests.sh >failed.txt
  testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
  unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
  fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
  err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
  num=$numRetries
  if [ "$unsuc" -gt 0 ] && [ "$num" -gt 0 ]; then
    while [ "$num" -gt 0 ]; do
      echo -e "${YL}Some tests failed$CL - retrying...."
      ./rerunFailedTests.sh $t
      ((num = num - 1))
      ((totalRetries = totalRetries + 1))
      ./getFailedTests.sh >failed.txt
      unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
      if [ "$unsuc" -eq 0 ]; then
        break
      fi
    done
    createFileList

  fi
done
./getFailedTests.sh >failed.txt

testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
rm -f run.lck
sleep 5
# kill $(<fail.pid) >/dev/null 2>&1
rm -f fail.pid >/dev/null 2>&1
echo -e "${GN}Finished!${CL} - total run: $testsRun - total unsuccessful: $unsuc"

if [ -z "$unsuc" ] || [ "$unsuc" -eq 0 ]; then
  echo -e "${GN}no errors recorded$CL"
  rm -f failed.txt
else
  echo -e "${RD}There were errors$CL: fails $fail + errors $err = $unsuc - List of failed tests in ./failed.txt "
  exit 1
fi
