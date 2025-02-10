#!/bin/bash
RD='\033[0;31m'
GN='\033[0;32m'
BL='\033[0;34m'
YL='\033[0;33m'
MG='\033[0;35m'
CN='\033[0;36m'
CL='\033[0m'

PID=$$

filesList=files_$PID.lst
classList=classes_$PID.txt
disabledList=disabled_$PID.txt
runLock=run_$PID.lck
testPid=test_$PID.pid
failPid=fail_$PID.pid

function createFileList() {
  rg -l "@Test" | grep ".java" >$filesList
  rg -l "@ParameterizedTest" | grep ".java" >>$filesList

  sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
  sort -u $filesList | grep "$p" >files_$PID.tmp && mv -f files_$PID.tmp $filesList
  rg -A2 "^ *@Disabled" | grep -B2 "public class" | grep : | cut -f1 -d: >$disabledList
  cat $filesList | while read l; do
    if grep $l $disabledList; then
      echo "$l disabled"
    else
      echo "$l" >>files_$PID.tmp
    fi
  done
  mv files_$PID.tmp $filesList

}
function quitting() {
  echo -e "${RD}Shutting down...${CL} current test $t"

  if [ -e $testPid ]; then
    kill -9 $(<$testPid) >/dev/null 2>&1
  fi
  if [ -e $failPid ]; then
    kill -9 $(<$failPid) >/dev/null 2>&1
  fi
  rm -f $testPid $failPid >/dev/null 2>&1
  echo "Removing unfinished test $t"
  rm -f test.log/$t.log
  ./getFailedTests.sh >failed.txt
  echo "List of failed tests in failed.txt"
  cat failed.txt
  rm -f $runLock $disabledList
  rm -f $filesList $classList files_$PID.tmp
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
      grep -v $i $classList >files_$PID.tmp
      mv files_$PID.tmp $classList
    done
  fi
fi
# read
cnt=$(wc -l <$classList | tr -d ' ')
if [ "$cnt" -eq 0 ]; then
  echo "no matching class found for $p"
  exit 1
fi
disabled=$(rg -C1 "^ *@Disabled" | grep -C1 "@Test" | grep : | cut -f1 -d: | wc -l)
disabled3=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances"\)' | grep : | cut -f1 -d: | wc -l)
disabled2=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstancesNo.*"\)' | grep : | cut -f1 -d: | wc -l)
disabled1=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances.*Only"\)' | grep : | cut -f1 -d: | wc -l)
testMethods=$(grep -E "@Test" $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
testMethods3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
testMethods2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
testMethods1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
# testMethodsP=$(grep -E "@ParameterizedTest" $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
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
  touch $runLock
  while [ -e $runLock ]; do
    ./getFailedTests.sh >failed.tmp
    mv failed.tmp failed.txt
    sleep $refresh
  done >/dev/null 2>&1

} &

echo $! >$failPid
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
##################################################################################################################
#######MAIN LOOP
for t in $(<$classList); do
  if grep "$t" $disabledList; then
    continue
  fi
  ((tst = tst + 1))
  tm=$(date +%s)
  if [ "$m" == }"." ]; then
    mvn -Dsurefire.useFile=false test -Dtest="$t" >test.log/"$t".log 2>&1 &
    echo $! >$testPid
  else
    mvn -Dsurefire.useFile=false test -Dtest="$t#$m" >"test.log/$t.log" 2>&1 &
    echo $! >$testPid
  fi
  while true; do
    testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
    unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
    fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
    err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
    ((d = $(date +%s) - start))
    # echo "Checking $fn"
    fn=$(echo "$t" | tr "." "/")
    lmeth=$(grep -E "@Test|@ParameterizedTest" $(grep "$fn" $filesList) | cut -f2 -d: | grep -vc '^ *//')
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
      kill $(<$testPid)
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
rm -f $runLock
sleep 5
# kill $(<$failPid) >/dev/null 2>&1
rm -f $failPid >/dev/null 2>&1
echo -e "${GN}Finished!${CL} - total run: $testsRun - total unsuccessful: $unsuc"

if [ -z "$unsuc" ] || [ "$unsuc" -eq 0 ]; then
  echo -e "${GN}no errors recorded$CL"
  rm -f failed.txt
else
  echo -e "${RD}There were errors$CL: fails $fail + errors $err = $unsuc - List of failed tests in ./failed.txt "
  exit 1
fi
