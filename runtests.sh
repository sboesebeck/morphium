#!/bin/bash
RD='\033[0;31m'
GN='\033[0;32m'
BL='\033[0;34m'
YL='\033[0;33m'
MG='\033[0;35m'
CN='\033[0;36m'
CL='\033[0m'

function quitting() {
  echo -e "${RD}Shutting down...${CL} current test $t"

  kill -9 $(<test.pid) >/dev/null 2>&1
  kill -9 $(<fail.pid) >/dev/null 2>&1
  rm -f test.pid fail.pid >/dev/null 2>&1
  echo "Removing unfinished test $t"
  rm -f test.log/$t.log
  ./getFailedTests.sh >failed.txt
  echo "List of failed tests in failed.txt"
  cat failed.txt
  exit
}

nodel=0
skip=0
refresh=5
logLength=15

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
  elif [ "q$1" == "q--refresh" ]; then
    shift
    refresh=$1
    shift
  fi
done
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
  count=$(ls -1 test.log/* | wc -l)
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

rg -l "@Test" | grep ".java" >files.lst
rg -l "@ParameterizedTest" | grep ".java" >>files.lst

sort -u files.lst | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >files.txt
sort -u files.lst | grep "$p" >files.tmp && mv -f files.tmp files.lst
if [ "$skip" -ne 0 ]; then
  echo -e "${BL}Info:${CL} Skipping tests already run"
  for i in $(ls -1 test.log); do
    i=$(basename $i)
    i=${i%%.log}
    echo -e "${BL}info: ${CL}not rerunning $i"
    grep -v $i files.txt >files.tmp
    mv files.tmp files.txt
  done
fi
# read
cnt=$(wc -l <files.txt | tr -d ' ')
if [ "$cnt" -eq 0 ]; then
  echo "no matching class found for $p"
  exit 1
fi
testMethods=$(grep -E "@Test" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
# testMethodsP=$(grep -E "@ParameterizedTest" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
((testMethods = testMethods + 3 * testMethods3 + testMethods2 * 2 + testMethods1))
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
  rm -rf test.log
  rm startTS
  mkdir test.log
fi
if [ "$nodel" -eq 0 ]; then
  echo -e "${BL}Info:${CL} Cleaning up..."
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
  while true; do
    # date >failed.tmp
    # echo >>failed.tmp
    ./getFailedTests.sh >failed.tmp
    mv failed.tmp failed.txt
    sleep 8
  done

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

    if [ ! -z "$testsRun" ] && [ "$testsRun" -ne 0 ]; then
      ((spt = d / testsRun))
      ((etl = spt * testMethods - d))
      ((etlm = etl / 60))
      echo -e "Date: $(date) - Running ${MG}$d${CL}sec - Tests run: ${BL}$testsRun${CL} ~ ${YL}$spt${CL} sec per test - ETL: ${MG}$etl${CL} =~ $etlm minutes"
    else
      echo -e "Date: $(date) - ${BL}Startup...$CL"

    fi
    echo "---------------------------------------------------------------------------------------------------------"
    echo -e "Running tests in ${YL}$t${CL}  - #${MG}$tst${CL}/${BL}$cnt$CL"
    echo -e "Total number methods to run in matching classes ${CN}$testMethods$CL"
    echo -e "Number of test methods in ${YL}$t${CL}: ${GN}$lmeth$CL"
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

done
./getFailedTests.sh >failed.txt

testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
kill $(<fail.pid) >/dev/null 2>&1
rm -f fail.pid >/dev/null 2>&1
echo -e "${GN}Finished!${CL} - total run $testsRun - total unsuccessful $unsuc"
if [ "$unsuc" -gt 0 ]; then
  echo -e "${RD}There were errors$CL: fails $fail + errors $err = $unsuc - List of failed tests in ./failed.txt "
  exit 1
else
  echo -e "${GN}no errors recorded$CL"
  rm -f failed.txt
fi
