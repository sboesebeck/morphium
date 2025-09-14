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
    if grep $l $disabledList >/dev/null; then
      echo "$l disabled"
    else
      echo "$l" >>files_$PID.tmp
    fi
  done
  mv files_$PID.tmp $filesList

}
function quitting() {

  if [ -e $testPid ]; then
    kill -9 $(<$testPid) >/dev/null 2>&1
  fi
  if [ -e $failPid ]; then
    kill -9 $(<$failPid) >/dev/null 2>&1
  fi
  rm -f $testPid $failPid >/dev/null 2>&1
  if [ -n "$t" ]; then
    echo -e "${RD}Shutting down...${CL} current test $t"
    echo "Removing unfinished test $t"
    rm -f test.log/$t.log
    ./getStats.sh >failed.txt
    echo "List of failed tests in failed.txt"
    cat failed.txt
  else
    echo -e "${YL} Shutting down...$CL"
  fi
  rm -f $runLock $disabledList
  rm -f $filesList $classList files_$PID.tmp
  exit
}

nodel=0
skip=0
refresh=5
logLength=15
numRetries=1
retried=""
totalRetries=0
includeTags=""
excludeTags=""
driver=""
uri=""
verbose=0
useExternal=0

while [ "q$1" != "q" ]; do

  if [ "q$1" == "q--nodel" ]; then
    nodel=1
    shift
  elif [ "q$1" == "q--help" ] || [ "q$1" == "-h" ]; then
    echo -e "Usage ${BL}$0$CL [--OPTION...] [TESTNAME] [METHOD]"
    echo -e "${BL}--skip$CL        - if presen, allready run tests will be skipped"
    echo -e "${BL}--restart$CL     - forget about existing test logs, restart"
    echo -e "${BL}--logs$CL ${GN}NUM$CL    - number of log lines to show"
    echo -e "${BL}--refresh$CL ${GN}NUM$CL - refresh view every NUM secs"
    echo -e "${BL}--retry$CL ${GN}NUM$CL   - number of retries on error in tests - default $YL$numRetries$CL"
    echo -e "${BL}--tags$CL ${GN}LIST$CL   - include JUnit5 tags (comma-separated)"
    echo -e "                   Available: core,messaging,driver,inmemory,aggregation,cache,admin,performance,encryption,jms,geo,util"
    echo -e "${BL}--exclude-tags$CL ${GN}LIST$CL - exclude JUnit5 tags (comma-separated)"
    echo -e "${BL}--driver$CL ${GN}NAME$CL - morphium driver: pooled|single|inmem"
    echo -e "${BL}--uri$CL ${GN}URI$CL     - mongodb connection string (or use MONGODB_URI env)"
    echo -e "${BL}--verbose$CL     - enable verbose test logs"
    echo -e "${BL}--external$CL    - enable external-tagged tests (activates -Pexternal)"
    echo -e "if neither ${BL}--restart${CL} nor ${BL}--skip${CL} are set, you will be asked, what to do"
    echo "Test name is the classname to run, and method is method name in that class"
    echo
    echo -e "${YL}Tag Examples:${CL}"
    echo -e "  ${BL}./runtests.sh --tags core${CL}                    # Run only core functionality tests"
    echo -e "  ${BL}./runtests.sh --tags messaging,cache${CL}         # Run messaging and cache tests"
    echo -e "  ${BL}./runtests.sh --exclude-tags performance${CL}     # Skip slow performance tests"
    echo -e "  ${BL}./runtests.sh --tags inmemory${CL}                # Fast offline testing"
    echo -e "  ${BL}./runtests.sh --tags core,messaging --exclude-tags admin${CL} # Combined filters"
    echo
    exit 0
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
  elif [ "q$1" == "q--tags" ]; then
    shift
    includeTags=$1
    shift
  elif [ "q$1" == "q--exclude-tags" ]; then
    shift
    excludeTags=$1
    shift
  elif [ "q$1" == "q--driver" ]; then
    shift
    driver=$1
    shift
  elif [ "q$1" == "q--uri" ]; then
    shift
    uri=$1
    shift
  elif [ "q$1" == "q--verbose" ]; then
    verbose=1
    shift
  elif [ "q$1" == "q--external" ]; then
    useExternal=1
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
# trap quitting EXIT
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
m=$(echo "$m" | tr -d '"')

createFileList
# If include tags specified, filter classes/files to only those tagged
if [ -n "$includeTags" ]; then
  tagPattern=$(echo "$includeTags" | sed 's/,/|/g')
  tmpTagged=tmp_tag_files_$PID.txt
  : > $tmpTagged
  # 1) Collect files explicitly annotated with any requested tag
  rg -l "@Tag\\(\\\"($tagPattern)\\\"\\)|@Tags\\(.*($tagPattern).*\\)" src/test/java >> $tmpTagged || true
  # 2) Directory-based helpers to map common tags to suites (backup for missing annotations)
  IFS=',' read -r -a tagArr <<< "$includeTags"
  for tg in "${tagArr[@]}"; do
    case "$tg" in
      core)
        # Find core functionality tests (primarily in suite/base)
        find src/test/java/de/caluga/test/mongo/suite/base -name "*Test*.java" -type f >> $tmpTagged || true
        ;;
      messaging)
        # Messaging tests in multiple locations
        if [ -d src/test/java/de/caluga/test/morphium/messaging ]; then
          find src/test/java/de/caluga/test/morphium/messaging -name "*.java" >> $tmpTagged
        fi
        if [ -d src/test/java/de/caluga/test/mongo/suite/ncmessaging ]; then
          find src/test/java/de/caluga/test/mongo/suite/ncmessaging -name "*.java" >> $tmpTagged
        fi
        ;;
      driver)
        # Driver layer tests
        if [ -d src/test/java/de/caluga/test/morphium/driver ]; then
          find src/test/java/de/caluga/test/morphium/driver -name "*.java" >> $tmpTagged
        fi
        find src/test/java -name "*DriverTest*.java" -o -name "*ConnectionTest*.java" >> $tmpTagged || true
        ;;
      inmemory)
        # InMemory driver specific tests
        if [ -d src/test/java/de/caluga/test/mongo/suite/inmem ]; then
          find src/test/java/de/caluga/test/mongo/suite/inmem -name "*.java" >> $tmpTagged
        fi
        find src/test/java -name "*InMem*.java" >> $tmpTagged || true
        ;;
      aggregation)
        # Aggregation pipeline tests
        if [ -d src/test/java/de/caluga/test/mongo/suite/aggregationStages ]; then
          find src/test/java/de/caluga/test/mongo/suite/aggregationStages -name "*.java" >> $tmpTagged
        fi
        find src/test/java -name "*Aggregation*.java" -o -name "*MapReduce*.java" >> $tmpTagged || true
        ;;
      cache)
        # Caching functionality tests
        find src/test/java -name "*Cache*.java" >> $tmpTagged || true
        ;;
      admin)
        # Administrative and infrastructure tests
        find src/test/java -name "*Index*.java" -o -name "*Transaction*.java" -o -name "*Admin*.java" >> $tmpTagged || true
        find src/test/java -name "*ChangeStream*.java" -o -name "*Stats*.java" -o -name "*Config*.java" >> $tmpTagged || true
        ;;
      performance)
        # Performance and bulk operation tests
        find src/test/java -name "*Bulk*.java" -o -name "*Buffer*.java" -o -name "*Async*.java" >> $tmpTagged || true
        find src/test/java -name "*Speed*.java" -o -name "*Performance*.java" >> $tmpTagged || true
        ;;
      encryption)
        # Encryption and security tests
        if [ -d src/test/java/de/caluga/test/mongo/suite/encrypt ]; then
          find src/test/java/de/caluga/test/mongo/suite/encrypt -name "*.java" >> $tmpTagged
        fi
        ;;
      jms)
        # JMS integration tests
        if [ -d src/test/java/de/caluga/test/mongo/suite/jms ]; then
          find src/test/java/de/caluga/test/mongo/suite/jms -name "*.java" >> $tmpTagged
        fi
        ;;
      geo)
        # Geospatial functionality tests
        find src/test/java -name "*Geo*.java" >> $tmpTagged || true
        ;;
      util)
        # Utility and helper tests
        find src/test/java -name "*Collator*.java" -o -name "*ObjectMapper*.java" >> $tmpTagged || true
        find src/test/java/de/caluga/test/objectmapping -name "*.java" >> $tmpTagged || true
        find src/test/java/de/caluga/test/morphium/query -name "*.java" >> $tmpTagged || true
        ;;
    esac
  done
  sort -u $tmpTagged -o $tmpTagged
  if [ -s $tmpTagged ]; then
    # Intersect with current files list (only keep files we already identified as tests)
    grep -F -f $tmpTagged $filesList > files_$PID.tmp || true
    mv files_$PID.tmp $filesList
    # Rebuild class list from filtered files
    sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
  fi
  rm -f $tmpTagged
fi

# Handle exclude tags
if [ -n "$excludeTags" ]; then
  excludePattern=$(echo "$excludeTags" | sed 's/,/|/g')
  tmpExcluded=tmp_exclude_files_$PID.txt
  : > $tmpExcluded
  # 1) Collect files explicitly annotated with any excluded tag
  rg -l "@Tag\\(\\\"($excludePattern)\\\"\\)|@Tags\\(.*($excludePattern).*\\)" src/test/java >> $tmpExcluded || true
  # 2) Directory-based patterns for excluded tags
  IFS=',' read -r -a excludeArr <<< "$excludeTags"
  for tg in "${excludeArr[@]}"; do
    case "$tg" in
      performance)
        find src/test/java -name "*Bulk*.java" -o -name "*Buffer*.java" -o -name "*Async*.java" >> $tmpExcluded || true
        find src/test/java -name "*Speed*.java" -o -name "*Performance*.java" >> $tmpExcluded || true
        ;;
      admin)
        find src/test/java -name "*Index*.java" -o -name "*Transaction*.java" -o -name "*Admin*.java" >> $tmpExcluded || true
        find src/test/java -name "*ChangeStream*.java" -o -name "*Stats*.java" -o -name "*Config*.java" >> $tmpExcluded || true
        ;;
      encryption)
        if [ -d src/test/java/de/caluga/test/mongo/suite/encrypt ]; then
          find src/test/java/de/caluga/test/mongo/suite/encrypt -name "*.java" >> $tmpExcluded
        fi
        ;;
      inmemory)
        if [ -d src/test/java/de/caluga/test/mongo/suite/inmem ]; then
          find src/test/java/de/caluga/test/mongo/suite/inmem -name "*.java" >> $tmpExcluded
        fi
        find src/test/java -name "*InMem*.java" >> $tmpExcluded || true
        ;;
    esac
  done
  sort -u $tmpExcluded -o $tmpExcluded
  if [ -s $tmpExcluded ]; then
    # Remove excluded files from the files list
    grep -v -F -f $tmpExcluded $filesList > files_$PID.tmp || cp $filesList files_$PID.tmp
    mv files_$PID.tmp $filesList
    # Rebuild class list from filtered files
    sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
  fi
  rm -f $tmpExcluded
fi
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
disabled=$(rg -C1 "^ *@Disabled" | grep -C1 "@Test" | grep : | cut -f1 -d: | grep "$p" | wc -l)
disabled3=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances"\)' | grep : | cut -f1 -d: | grep "$p" | wc -l)
disabled2=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstancesNo.*"\)' | grep : | cut -f1 -d: | grep "$p" | wc -l)
disabled1=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances.*Only"\)' | grep : | cut -f1 -d: | grep "$p" | wc -l)
testMethods=$(grep -E "@Test" $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
testMethods3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
testMethods2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
testMethods1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
# testMethodsP=$(grep -E "@ParameterizedTest" $(grep "$p" $filesList) | cut -f2 -d: | grep -vc '^ *//')
((testMethods = testMethods + 2 * testMethods3 + testMethods2 * 2 + testMethods1 - disabled - disabled3 * 3 - disabled2 * 2 - disabled1))
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
MVN_PROPS=""
if [ -n "$includeTags" ]; then MVN_PROPS="$MVN_PROPS -Dtest.includeTags=$includeTags"; fi
if [ -n "$excludeTags" ]; then MVN_PROPS="$MVN_PROPS -Dtest.excludeTags=$excludeTags"; fi
if [ -n "$driver" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.driver=$driver"; fi
if [ -n "$uri" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.uri=$uri"; fi
if [ -z "$uri" ] && [ -n "$MONGODB_URI" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.uri=$MONGODB_URI"; fi
if [ "$verbose" -eq 1 ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.tests.verbose=true"; fi
if [ "$useExternal" -eq 1 ]; then MVN_PROPS="$MVN_PROPS -Pexternal"; fi

mvn $MVN_PROPS compile test-compile >/dev/null || {
  echo -e "${RD}Error:${CL} Compilation failed!"
  exit 1
}

tst=0
echo -e "${GN}Starting tests..${CL}" >failed.txt
# running getfailedTests in background
{
  touch $runLock
  while [ -e $runLock ]; do
    ./getStats.sh >failed.tmp
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
  while true; do
    tm=$(date +%s)
  if [ "$m" == "." ]; then
      echo "Running Tests in $t" >"test.log/$t.log"
      mvn -Dsurefire.useFile=false $MVN_PROPS test -Dtest="$t" >>"test.log/$t".log 2>&1 &
      echo $! >$testPid
    else
      echo "Running $m in $t" >"test.log/$t.log"
      mvn -Dsurefire.useFile=false $MVN_PROPS test -Dtest="$t#$m" >>"test.log/$t.log" 2>&1 &
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
      lmeth=$(grep -E "@Test" $(grep "$fn" $filesList) | cut -f2 -d: | grep -vc '^ *//')
      lmeth3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $(grep "$fn" $filesList) | cut -f2 -d: | grep -vc '^ *//')
      lmeth2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $(grep "$fn" $filesList) | cut -f2 -d: | grep -vc '^ *//')
      lmeth1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $(grep "$fn" $filesList) | cut -f2 -d: | grep -vc '^ *//')
      ((lmeth = lmeth + lmeth3 * 2 + lmeth2 * 2 + lmeth1))

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
        echo -e "--------------------------${MG}Retries${CL}-----------------------------------------------------------------------"
        echo -e "Had to retry ${YL}$totalRetries${CL} times:"
        echo -e "Classes retried:$retried" | sort -u
      fi
      if [ "$m" != "." ]; then
        echo -e " Tests matching: ${BL}$m${CL}"
      fi

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
    if grep -A3 "Results:" test.log/$t.log | grep "Tests run: 0,"; then
      echo -e "${RD}Error:$CL No tests run in $t - enter to retry"
      read </dev/tty
      mvn clean compile test-compile
    else
      break
    fi
  done
done
##### Mainloop end
######################################################
./getStats.sh >failed.txt

testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
num=$numRetries
if [ "$unsuc" -gt 0 ] && [ "$num" -gt 0 ]; then
  while [ "$num" -gt 0 ]; do
    echo -e "${YL}Some tests failed$CL - retrying...."
    ./rerunFailedTests.sh
    ((num = num - 1))
    ((totalRetries = totalRetries + 1))
    retried="$retried\n$t"
    ./getStats.sh >failed.txt
    unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
    if [ "$unsuc" -eq 0 ]; then
      break
    fi
  done

fi
./getStats.sh >failed.txt

testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
rm -f $runLock
sleep 5
t=""
# kill $(<$failPid) >/dev/null 2>&1
rm -f $failPid >/dev/null 2>&1
echo -e "${GN}Finished!${CL} - total run: $testsRun - total unsuccessful: $unsuc - Retries: $totalRetries"

if [ -z "$unsuc" ] || [ "$unsuc" -eq 0 ]; then
  echo -e "${GN}no errors recorded$CL"
  rm -f failed.txt
  quitting
else
  echo -e "${RD}There were errors$CL: fails $fail + errors $err = $unsuc - List of failed tests in ./failed.txt "
  quitting
  exit 1
fi
