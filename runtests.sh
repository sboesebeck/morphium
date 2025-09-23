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
    get_test_stats >failed.txt 2>/dev/null
    echo "List of failed tests in failed.txt"
    cat failed.txt
  else
    echo -e "${YL} Shutting down...$CL"
  fi
  rm -f $runLock $disabledList
  rm -f $filesList $classList files_$PID.tmp
  exit
}

function get_test_stats() {
  local noreason=0
  local nosum=0

  # Parse arguments
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
      echo "Error - only --noreason or --nosum allowed! $1 unknown"
      return 1
      ;;
    esac
  done

  local total_run=0
  local total_fail=0
  local total_err=0
  local total_build_failures=0
  local failed_tests=""

  # Handle both sequential and parallel log structures
  local log_pattern=""
  if [ -d "test.log" ]; then
    # Check if we have slot directories (parallel execution)
    if ls test.log/slot_* >/dev/null 2>&1; then
      # Parallel execution - prioritize copied logs in main directory, fallback to slot logs
      if ls test.log/*.log >/dev/null 2>&1; then
        log_pattern="test.log/*.log" # Use copied logs if available
      else
        log_pattern="test.log/slot_*/*.log" # Fallback to slot logs
      fi
    else
      # Sequential execution - standard pattern
      log_pattern="test.log/*.log"
    fi

    # Use a simple deduplication approach: collect unique test classes and process newest first
    # This handles parallel execution where same test runs in multiple slots

    # Create a temporary file to store results per test class
    local temp_results=$(mktemp)

    # First pass: collect all log files with their timestamps and test classes
    for pattern in $log_pattern; do
      for logfile in $pattern; do
        if [ -f "$logfile" ] && [[ "$logfile" =~ \.log$ ]] && [[ ! "$logfile" =~ slot\.log$ ]]; then
          local test_class=$(basename "$logfile" .log | sed 's/_slot[0-9]*$//')
          local timestamp=$(stat -f "%m" "$logfile" 2>/dev/null || stat -c "%Y" "$logfile" 2>/dev/null || echo "0")

          # Normalize test class names: if we have both short and full names, prefer full names
          # This handles cases where tests are run as "TestClass" vs "com.package.TestClass"
          if [[ "$test_class" != *.* ]]; then
            # Short name - check if there's a corresponding full name in logs
            local full_name_pattern="*.$test_class"
            for other_log in $log_pattern; do
              local other_class=$(basename "$other_log" .log | sed 's/_slot[0-9]*$//')
              if [[ "$other_class" == $full_name_pattern ]]; then
                test_class="$other_class"
                break
              fi
            done
          fi

          echo "$timestamp:$test_class:$logfile" >>"$temp_results"
        fi
      done
    done

    # Process logs by timestamp (newest first) to ensure most recent result is used
    # This fixes the issue where old failed tests still appear as failed after successful reruns
    local processed_classes=""

    # Function to process a single log file
    process_logfile() {
      local test_class="$1"
      local logfile="$2"

      local has_build_failure=false
      local has_test_failure=false
      local run_count=0
      local fail_count=0
      local err_count=0

      # Check if tests actually ran and executed properly
      if grep -q "Tests run: .*in " "$logfile"; then
        local test_result_line=$(grep -a "Tests run: .*in " "$logfile" | tail -1)
        local run_count=$(echo "$test_result_line" | sed -n 's/.*Tests run: \([0-9]*\).*/\1/p')
        [[ ! "$run_count" =~ ^[0-9]+$ ]] && run_count=0

        # Only treat as test execution if tests actually ran (run_count > 0)
        if [ "$run_count" -gt 0 ]; then
          # Tests ran, so build succeeded. Any failures are test failures, not build failures
          fail_count=$(echo "$test_result_line" | sed -n 's/.*Failures: \([0-9]*\).*/\1/p')
          err_count=$(echo "$test_result_line" | sed -n 's/.*Errors: \([0-9]*\).*/\1/p')

          # Handle missing values and ensure they're numeric
          [[ ! "$fail_count" =~ ^[0-9]+$ ]] && fail_count=0
          [[ ! "$err_count" =~ ^[0-9]+$ ]] && err_count=0

          # Add to totals
          ((total_run += run_count))
          if [ "$fail_count" -gt 0 ]; then
            ((total_fail += fail_count))
            has_test_failure=true
          fi
          if [ "$err_count" -gt 0 ]; then
            ((total_err += err_count))
            has_test_failure=true
          fi

          # Add to failed tests if there were actual test failures
          if [ "$has_test_failure" == "true" ]; then
            if [ $noreason -eq 1 ]; then
              failed_tests="$failed_tests$test_class"$'\n'
            else
              failed_tests="$failed_tests$test_class - Tests failed: $fail_count, Errors: $err_count"$'\n'
            fi
          fi
        else
          # Tests run: 0 - this means the test class failed to execute (build/setup issue)
          # Check for Maven BUILD FAILURE - only if it's the final result with "Total time"
          if grep -A5 -B5 "BUILD FAILURE" "$logfile" 2>/dev/null | grep -q "Total time:" && grep -q "BUILD FAILURE" "$logfile"; then
            has_build_failure=true
            ((total_build_failures += 1))
            if [ $noreason -eq 1 ]; then
              failed_tests="$failed_tests$test_class"$'\n'
            else
              failed_tests="$failed_tests$test_class - BUILD FAILURE (no tests executed)"$'\n'
            fi
          fi
        fi
      else
        # No "Tests run:" line at all - this is a true build failure (compilation error, dependency issue, etc.)
        # Check for Maven BUILD FAILURE - only if it's the final result with "Total time"
        if grep -A5 -B5 "BUILD FAILURE" "$logfile" 2>/dev/null | grep -q "Total time:" && grep -q "BUILD FAILURE" "$logfile"; then
          has_build_failure=true
          ((total_build_failures += 1))
          if [ $noreason -eq 1 ]; then
            failed_tests="$failed_tests$test_class"$'\n'
          else
            failed_tests="$failed_tests$test_class - BUILD FAILURE"$'\n'
          fi
        fi
      fi
    }

    # Single pass: Process newest log for each test class (sorted by timestamp descending)
    while IFS=: read -r timestamp test_class logfile; do
      if [[ "$processed_classes" == *"|$test_class|"* ]]; then
        continue # Already processed the newest log for this test class
      fi

      # Mark this test class as processed and process its newest log
      processed_classes="$processed_classes|$test_class|"
      process_logfile "$test_class" "$logfile"
    done < <(sort -nr "$temp_results")

    rm -f "$temp_results"
  fi

  # Output summary
  if [ "$nosum" -eq 0 ]; then
    echo "Total tests run    : $total_run"
    echo "Total unsuccessful : $((total_fail + total_err + total_build_failures))"
    echo "Tests failed       : $total_fail"
    echo "Tests with errors  : $total_err"
    if [ "$total_build_failures" -gt 0 ]; then
      echo "Build failures     : $total_build_failures"
    fi
  fi

  # Output failed tests
  if [ -n "$failed_tests" ] && [ "$total_fail" -gt 0 -o "$total_err" -gt 0 -o "$total_build_failures" -gt 0 ]; then
    if [ "$nosum" -eq 0 ]; then
      echo ""
    fi
    # Use printf to handle newline-separated entries properly
    printf "%s" "$failed_tests"
  fi
}

nodel=0
skip=0
refresh=5
logLength=15
numRetries=0
retried=""
totalRetries=0
includeTags=""
excludeTags=""
driver=""
uri=""
user=""
pass=""
authdb=""
verbose=0
useExternal=0
rerunfailed=0
explicitRestart=0
showStats=0

# Save original arguments for stats processing
original_args=("$@")

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
    echo -e "                   Available: core,messaging,driver,inmemory,aggregation,cache,admin,performance,encryption,jms,geo,util,external"
    echo -e "${BL}--exclude-tags$CL ${GN}LIST$CL - exclude JUnit5 tags (comma-separated)"
    echo -e "${BL}--driver$CL ${GN}NAME$CL - morphium driver: pooled|single|inmem"
    echo -e "${BL}--uri$CL ${GN}URI$CL     - mongodb connection string (or use MONGODB_URI env)"
    echo -e "${BL}--verbose$CL     - enable verbose test logs"
    echo -e "${BL}--user$CL ${GN}USESRNAME$CL     - authenticate as user"
    echo -e "${BL}--pass$CL ${GN}password$CL     - authenticate with password"
    echo -e "${BL}--authdb$CL ${GN}DATABASE$CL     - authentication DB"
    echo -e "${BL}--external$CL    - enable external MongoDB tests (activates -Pexternal)"
    echo -e "                     ${YL}NOTE:${CL} Conflicts with --driver inmem and --tags inmemory"
    echo -e "${BL}--parallel$CL ${GN}N$CL    - run tests in N parallel slots (1-16, each with unique DB)"
    echo -e "${BL}--rerunfailed$CL   - rerun only previously failed tests (uses integrated stats)"
    echo -e "                     ${YL}NOTE:${CL} Conflicts with --restart (which cleans logs)"
    echo -e "${BL}--stats$CL         - show test statistics and failed tests (replaces getStats.sh)"
    echo -e "if neither ${BL}--restart${CL} nor ${BL}--skip${CL} are set, you will be asked, what to do"
    echo "Test name is the classname to run, and method is method name in that class"
    echo
    echo -e "${YL}Tag Examples:${CL}"
    echo -e "  ${BL}./runtests.sh --tags core${CL}                    # Run only core functionality tests"
    echo -e "  ${BL}./runtests.sh --tags messaging,cache${CL}         # Run messaging and cache tests"
    echo -e "  ${BL}./runtests.sh --exclude-tags performance${CL}     # Skip slow performance tests"
    echo -e "  ${BL}./runtests.sh --tags inmemory${CL}                # Fast offline testing (local only)"
    echo -e "  ${BL}./runtests.sh --external --tags driver${CL}       # External MongoDB driver tests"
    echo -e "  ${BL}./runtests.sh --tags core,messaging --exclude-tags admin${CL} # Combined filters"
    echo
    echo -e "${YL}Driver/External Examples:${CL}"
    echo -e "  ${BL}./runtests.sh --driver inmem --tags core${CL}     # Local testing with InMemory driver"
    echo -e "  ${BL}./runtests.sh --external --driver pooled${CL}     # External MongoDB with pooled driver"
    echo -e "  ${RD}./runtests.sh --external --driver inmem${CL}      # ERROR: Conflicting options!"
    echo
    echo -e "${YL}Parallel Examples:${CL}"
    echo -e "  ${BL}./runtests.sh --parallel 4 --driver inmem${CL}    # 4 parallel slots with InMemory driver"
    echo -e "  ${BL}./runtests.sh --parallel 8 --tags core${CL}       # 8 parallel slots, core tests only"
    echo
    echo -e "${YL}Rerun Examples:${CL}"
    echo -e "  ${BL}./runtests.sh --rerunfailed${CL}                  # Rerun all previously failed tests"
    echo -e "  ${BL}./runtests.sh --rerunfailed --retry 3${CL}        # Rerun with 3 retries per test"
    echo -e "  ${BL}./runtests.sh --rerunfailed --parallel 4${CL}     # Rerun failed tests in parallel"
    echo -e "  ${RD}./runtests.sh --rerunfailed --restart${CL}        # ERROR: Conflicting options!"
    echo
    echo -e "${YL}Stats Examples:${CL}"
    echo -e "  ${BL}./runtests.sh --stats${CL}                       # Show test statistics and failed tests"
    echo -e "  ${BL}./runtests.sh --stats --noreason${CL}             # Show only failed test names"
    echo -e "  ${BL}./runtests.sh --stats --nosum${CL}                # Show only failed tests (no summary)"
    echo
    exit 0
  elif [ "q$1" == "q--skip" ]; then
    skip=1
    shift
  elif [ "q$1" == "q--restart" ]; then
    explicitRestart=1
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
  elif [ "q$1" == "q--pass" ]; then
    shift
    pass=$1
    shift
  elif [ "q$1" == "q--user" ]; then
    shift
    user=$1
    shift
  elif [ "q$1" == "q--authdb" ]; then
    shift
    authdb=$1
    shift
  elif [ "q$1" == "q--parallel" ]; then
    shift
    parallel=$1
    shift
    if ! [[ "$parallel" =~ ^[0-9]+$ ]] || [ "$parallel" -lt 1 ] || [ "$parallel" -gt 16 ]; then
      echo -e "${RD}Error: --parallel must be a number between 1 and 16${CL}"
      exit 1
    fi
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
  elif [ "q$1" == "q--rerunfailed" ]; then
    rerunfailed=1
    shift
  elif [ "q$1" == "q--stats" ]; then
    showStats=1
    shift
  elif [ "q$1" == "q--noreason" ] || [ "q$1" == "q--nosum" ]; then
    # These are stats-specific options, only meaningful with --stats
    # Skip them in main parsing, they'll be handled in stats processing
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

# Set defaults
if [ -z "$parallel" ]; then
  parallel=1
fi

# Set default driver to inmem if none specified and no external mode

# Conflict detection
if [ "$useExternal" -eq 1 ] && [ "$driver" == "inmem" ]; then
  echo -e "${RD}Error:${CL} --external and --driver inmem are conflicting options!"
  echo -e "  --external: Enables external MongoDB connection tests"
  echo -e "  --driver inmem: Forces InMemory driver (local only)"
  echo -e "  ${YL}Suggestion:${CL} Use --external with --driver pooled or --driver single"
  exit 1
fi

if [ "$useExternal" -eq 1 ] && [[ "$includeTags" == *"inmemory"* ]]; then
  echo -e "${YL}Warning:${CL} --external and --tags inmemory may conflict!"
  echo -e "  --external: Enables external MongoDB tests"
  echo -e "  --tags inmemory: Runs InMemory driver tests (local only)"
  echo -e "  ${BL}Continuing...${CL} but results may be inconsistent"
fi

if [ "$driver" == "inmem" ] && [[ "$includeTags" == *"external"* ]]; then
  echo -e "${RD}Error:${CL} --driver inmem and --tags external are conflicting!"
  echo -e "  --driver inmem: Forces InMemory driver (local only)"
  echo -e "  --tags external: Runs tests requiring external MongoDB"
  echo -e "  ${YL}Suggestion:${CL} Use --tags external with --driver pooled or remove --tags external"
  exit 1
fi

if [ "$rerunfailed" -eq 1 ] && [ "$explicitRestart" -eq 1 ]; then
  echo -e "${RD}Error:${CL} --rerunfailed and --restart are conflicting!"
  echo -e "  --rerunfailed: Needs existing test logs to identify failed tests"
  echo -e "  --restart: Cleans all existing test logs"
  echo -e "  ${YL}Suggestion:${CL} Use --rerunfailed alone or use --restart for a fresh start"
  exit 1
fi

# Handle --stats option early to just show statistics and exit
if [ "$showStats" -eq 1 ]; then
  # Extract only the stats-specific arguments and pass them to get_test_stats
  echo -e "Calculating stats... ${GN}please wait$CL"
  stats_args=""
  for arg in "${original_args[@]}"; do
    case "$arg" in
    --noreason | --nosum)
      stats_args="$stats_args $arg"
      ;;
    esac
  done
  get_test_stats $stats_args
  exit 0
fi

if [ -z "$driver" ] && [ "$useExternal" -eq 0 ]; then
  driver="inmem"
  echo -e "${YL}Info:${CL} No driver specified, defaulting to --driver inmem (use --external for external MongoDB drivers)"
fi
# Handle --rerunfailed option early to bypass interactive prompts
if [ "$rerunfailed" -eq 1 ]; then
  echo -e "${MG}Rerunning${CL} ${CN}failed tests...${CL}"

  # For rerunfailed, we want to use existing logs to determine what failed
  # Set nodel=1 to preserve existing logs for analysis and skip=1 to bypass prompts
  nodel=1
  skip=1

  # Get failed tests using integrated stats function
  failed=$(get_test_stats --noreason --nosum 2>/dev/null || echo "")

  if [ -z "$failed" ]; then
    echo -e "${GN}No failed tests found to rerun!${CL}"
    exit 0
  fi

  # Convert failed tests to class list format
  failed_classes=""
  for f in $failed; do
    cls=${f%#*}
    # Add to our class list for later processing
    if [ -z "$failed_classes" ]; then
      failed_classes="$cls"
    else
      failed_classes="$failed_classes\n$cls"
    fi
  done

  # Create a temporary class list with just the failed tests
  echo -e "$failed_classes" | sort -u >$classList
  echo -e "${BL}Found $(wc -l <$classList | tr -d ' ') failed test classes to rerun${CL}"

  # Create file list based on the failed classes
  : >$filesList
  while IFS= read -r cls; do
    # Convert class name back to file path
    file_path=$(echo "$cls" | sed 's/\./\//g')
    file_path="src/test/java/${file_path}.java"
    if [ -f "$file_path" ]; then
      echo "$file_path" >>$filesList
    fi
  done <$classList

  # Create disabled list (empty for failed test reruns)
  : >$disabledList

  # Skip the normal file creation logic
  skip_file_creation=1
fi
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

if [ "$skip_file_creation" != "1" ]; then
  createFileList
fi
# If include tags specified, filter classes/files to only those tagged
if [ -n "$includeTags" ]; then
  tagPattern=$(echo "$includeTags" | sed 's/,/|/g')
  tmpTagged=tmp_tag_files_$PID.txt
  : >$tmpTagged
  # 1) Collect files explicitly annotated with any requested tag
  rg -l "@Tag\\(\\\"($tagPattern)\\\"\\)|@Tags\\(.*($tagPattern).*\\)" src/test/java >>$tmpTagged || true
  # 2) Directory-based helpers to map common tags to suites (backup for missing annotations)
  IFS=',' read -r -a tagArr <<<"$includeTags"
  for tg in "${tagArr[@]}"; do
    case "$tg" in
    core)
      # Find core functionality tests (primarily in suite/base)
      find src/test/java/de/caluga/test/mongo/suite/base -name "*Test*.java" -type f >>$tmpTagged || true
      ;;
    messaging)
      # Messaging tests in multiple locations
      if [ -d src/test/java/de/caluga/test/morphium/messaging ]; then
        find src/test/java/de/caluga/test/morphium/messaging -name "*.java" >>$tmpTagged
      fi
      if [ -d src/test/java/de/caluga/test/mongo/suite/ncmessaging ]; then
        find src/test/java/de/caluga/test/mongo/suite/ncmessaging -name "*.java" >>$tmpTagged
      fi
      ;;
    driver)
      # Driver layer tests
      if [ -d src/test/java/de/caluga/test/morphium/driver ]; then
        find src/test/java/de/caluga/test/morphium/driver -name "*.java" >>$tmpTagged
      fi
      find src/test/java -name "*DriverTest*.java" -o -name "*ConnectionTest*.java" >>$tmpTagged || true
      ;;
    inmemory)
      # InMemory driver specific tests
      if [ -d src/test/java/de/caluga/test/mongo/suite/inmem ]; then
        find src/test/java/de/caluga/test/mongo/suite/inmem -name "*.java" >>$tmpTagged
      fi
      find src/test/java -name "*InMem*.java" >>$tmpTagged || true
      ;;
    aggregation)
      # Aggregation pipeline tests
      if [ -d src/test/java/de/caluga/test/mongo/suite/aggregationStages ]; then
        find src/test/java/de/caluga/test/mongo/suite/aggregationStages -name "*.java" >>$tmpTagged
      fi
      find src/test/java -name "*Aggregation*.java" -o -name "*MapReduce*.java" >>$tmpTagged || true
      ;;
    cache)
      # Caching functionality tests
      find src/test/java -name "*Cache*.java" >>$tmpTagged || true
      ;;
    admin)
      # Administrative and infrastructure tests
      find src/test/java -name "*Index*.java" -o -name "*Transaction*.java" -o -name "*Admin*.java" >>$tmpTagged || true
      find src/test/java -name "*ChangeStream*.java" -o -name "*Stats*.java" -o -name "*Config*.java" >>$tmpTagged || true
      ;;
    performance)
      # Performance and bulk operation tests
      find src/test/java -name "*Bulk*.java" -o -name "*Buffer*.java" -o -name "*Async*.java" >>$tmpTagged || true
      find src/test/java -name "*Speed*.java" -o -name "*Performance*.java" >>$tmpTagged || true
      ;;
    encryption)
      # Encryption and security tests
      if [ -d src/test/java/de/caluga/test/mongo/suite/encrypt ]; then
        find src/test/java/de/caluga/test/mongo/suite/encrypt -name "*.java" >>$tmpTagged
      fi
      ;;
    jms)
      # JMS integration tests
      if [ -d src/test/java/de/caluga/test/mongo/suite/jms ]; then
        find src/test/java/de/caluga/test/mongo/suite/jms -name "*.java" >>$tmpTagged
      fi
      ;;
    geo)
      # Geospatial functionality tests
      find src/test/java -name "*Geo*.java" >>$tmpTagged || true
      ;;
    util)
      # Utility and helper tests
      find src/test/java -name "*Collator*.java" -o -name "*ObjectMapper*.java" >>$tmpTagged || true
      find src/test/java/de/caluga/test/objectmapping -name "*.java" >>$tmpTagged || true
      find src/test/java/de/caluga/test/morphium/query -name "*.java" >>$tmpTagged || true
      ;;
    external)
      # External MongoDB connection tests (failover, etc.)
      find src/test/java -name "*Failover*.java" >>$tmpTagged || true
      ;;
    esac
  done
  sort -u $tmpTagged -o $tmpTagged
  if [ -s $tmpTagged ]; then
    # Intersect with current files list (only keep files we already identified as tests)
    grep -F -f $tmpTagged $filesList >files_$PID.tmp || true
    mv files_$PID.tmp $filesList
    # Rebuild class list from filtered files
    sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
  fi
  rm -f $tmpTagged
fi

if [ "q$1" = "q" ]; then
  # no test to run specified
  # Handle exclude tags
  if [ -n "$excludeTags" ]; then
    excludePattern=$(echo "$excludeTags" | sed 's/,/|/g')
    tmpExcluded=tmp_exclude_files_$PID.txt
    : >$tmpExcluded
    # 1) Collect files explicitly annotated with any excluded tag
    rg -l "@Tag\\(\\\"($excludePattern)\\\"\\)|@Tags\\(.*($excludePattern).*\\)" src/test/java >>$tmpExcluded || true
    # 2) Directory-based patterns for excluded tags
    IFS=',' read -r -a excludeArr <<<"$excludeTags"
    for tg in "${excludeArr[@]}"; do
      case "$tg" in
      performance)
        find src/test/java -name "*Bulk*.java" -o -name "*Buffer*.java" -o -name "*Async*.java" >>$tmpExcluded || true
        find src/test/java -name "*Speed*.java" -o -name "*Performance*.java" >>$tmpExcluded || true
        ;;
      admin)
        find src/test/java -name "*Index*.java" -o -name "*Transaction*.java" -o -name "*Admin*.java" >>$tmpExcluded || true
        find src/test/java -name "*ChangeStream*.java" -o -name "*Stats*.java" -o -name "*Config*.java" >>$tmpExcluded || true
        ;;
      encryption)
        if [ -d src/test/java/de/caluga/test/mongo/suite/encrypt ]; then
          find src/test/java/de/caluga/test/mongo/suite/encrypt -name "*.java" >>$tmpExcluded
        fi
        ;;
      inmemory)
        if [ -d src/test/java/de/caluga/test/mongo/suite/inmem ]; then
          find src/test/java/de/caluga/test/mongo/suite/inmem -name "*.java" >>$tmpExcluded
        fi
        find src/test/java -name "*InMem*.java" >>$tmpExcluded || true
        ;;
      external)
        # External MongoDB connection tests (excluded by default)
        find src/test/java -name "*Failover*.java" >>$tmpExcluded || true
        ;;
      esac
    done
    sort -u $tmpExcluded -o $tmpExcluded
    if [ -s $tmpExcluded ]; then
      # Remove excluded files from the files list
      grep -v -F -f $tmpExcluded $filesList >files_$PID.tmp || cp $filesList files_$PID.tmp
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
else
  echo "Limiting to test $1"
  echo "$1" >$classList
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
if [ -n "$user" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.user=$user"; fi
if [ -n "$pass" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.pass=$pass"; fi
if [ -n "$authdb" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.authdb=$authdb"; fi
if [ -n "$uri" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.uri=$uri"; fi
if [ -z "$uri" ] && [ -n "$MONGODB_URI" ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.uri=$MONGODB_URI"; fi
if [ "$verbose" -eq 1 ]; then MVN_PROPS="$MVN_PROPS -Dmorphium.tests.verbose=true"; fi
if [ "$useExternal" -eq 1 ]; then MVN_PROPS="$MVN_PROPS -Pexternal"; fi

mvn $MVN_PROPS compile test-compile >/dev/null || {
  echo -e "${RD}Error:${CL} Compilation failed!"
  exit 1
}

TEST_MVN_PROPS="$MVN_PROPS -Dmaven.compiler.skip=true"

tst=0
echo -e "${GN}Starting tests..${CL}" >failed.txt
# running getfailedTests in background
{
  touch $runLock
  while [ -e $runLock ]; do
    get_test_stats >failed.tmp 2>/dev/null
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
#######PARALLEL EXECUTION FUNCTIONS
function run_test_slot() {
  local slot_id=$1
  local test_chunk_file=$2
  local slot_mvn_props="$TEST_MVN_PROPS -Dmorphium.database=morphium_test_$slot_id -DtempDir=surefire_slot_$slot_id -DreportsDirectory=target/surefire-reports-$slot_id"

  mkdir -p "test.log/slot_$slot_id"
  mkdir -p "target/surefire_slot_$slot_id"

  # Calculate actual test methods for this slot's test classes (consistent with serial mode)
  local total_test_classes=$(wc -l <"$test_chunk_file" | tr -d ' ')
  local slot_testMethods=0
  while IFS= read -r test_class; do
    if ! grep "$test_class" $disabledList >/dev/null; then
      local fn=$(echo "$test_class" | tr "." "/")
      local test_file=$(grep "$fn" $filesList | head -1 2>/dev/null)
      if [ -n "$test_file" ] && [ -f "$test_file" ]; then
        local lmeth=$(grep -E "@Test" "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
        local lmeth3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
        local lmeth2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
        local lmeth1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
        ((slot_testMethods += lmeth + lmeth3 * 2 + lmeth2 * 2 + lmeth1))
      fi
    fi
  done <"$test_chunk_file"

  local current_test_methods=0
  local current_test_classes=0
  local failed_tests=0

  while IFS= read -r t; do
    if grep "$t" $disabledList >/dev/null; then
      continue
    fi

    # Calculate test methods for this class
    local fn=$(echo "$t" | tr "." "/")
    local test_file=$(grep "$fn" $filesList | head -1 2>/dev/null)
    local class_test_methods=0
    if [ -n "$test_file" ] && [ -f "$test_file" ]; then
      local class_lmeth=$(grep -E "@Test" "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
      local class_lmeth3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
      local class_lmeth2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
      local class_lmeth1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
      class_test_methods=$((class_lmeth + class_lmeth3 * 2 + class_lmeth2 * 2 + class_lmeth1))
    fi

    ((current_test_classes++))

    # Update progress before starting test
    echo "RUNNING:$t:$current_test_methods:$slot_testMethods:$failed_tests" >"test.log/slot_$slot_id/progress"

    if [ "$m" == "." ]; then
      echo "Slot $slot_id: Running $t ($current_test_classes/$total_test_classes, $current_test_methods methods)" >>"test.log/slot_$slot_id/slot.log"
      mvn -Dsurefire.useFile=false $slot_mvn_props test -Dtest="$t" >"test.log/slot_$slot_id/$t.log" 2>&1
      exit_code=$?
    else
      echo "Slot $slot_id: Running $t#$m ($current_test_classes/$total_test_classes, $current_test_methods methods)" >>"test.log/slot_$slot_id/slot.log"
      mvn -Dsurefire.useFile=false $slot_mvn_props test -Dtest="$t#$m" >"test.log/slot_$slot_id/$t.log" 2>&1
      exit_code=$?
    fi

    # Add the class's methods to our running total after completion
    ((current_test_methods += class_test_methods))

    if [ $exit_code -ne 0 ]; then
      ((failed_tests++))
      echo "FAILED:$t" >>"test.log/slot_$slot_id/failed_tests"
    fi

    # Update progress after completing test
    echo "COMPLETED:$t:$current_test_methods:$slot_testMethods:$failed_tests" >"test.log/slot_$slot_id/progress"

  done <"$test_chunk_file"

  echo "DONE::$current_test_methods:$slot_testMethods:$failed_tests" >"test.log/slot_$slot_id/progress"
  echo "Slot $slot_id completed: $current_test_classes classes ($current_test_methods test methods), $failed_tests failed" >>"test.log/slot_$slot_id/slot.log"
}

function monitor_parallel_progress() {
  echo -e "${GN}Monitoring parallel execution...${CL}"

  while true; do
    all_done=true
    clear
    echo -e "Date: $(date) - ${BL}Parallel Test Execution Status${CL}"
    echo "=========================================================================================================="

    local total_running=0
    local total_completed=0
    local total_tests=0
    local total_failed=0

    for ((slot = 1; slot <= parallel; slot++)); do
      if [ -e "slot_${slot}.pid" ] && kill -0 $(<"slot_${slot}.pid") 2>/dev/null; then
        all_done=false
        if [ -e "test.log/slot_$slot/progress" ]; then
          progress_line=$(cat "test.log/slot_$slot/progress")
          IFS=':' read -r status test_name current_test total_tests_slot failed_tests <<<"$progress_line"

          ((total_tests += total_tests_slot))
          ((total_completed += current_test))
          ((total_failed += failed_tests))

          if [ "$status" == "RUNNING" ]; then
            ((total_running++))
            echo -e "Slot ${YL}$slot${CL}: ${GN}RUNNING${CL} $test_name (${BL}$current_test${CL}/${MG}$total_tests_slot${CL}) Failed: ${RD}$failed_tests${CL}"
          elif [ "$status" == "COMPLETED" ]; then
            echo -e "Slot ${YL}$slot${CL}: ${BL}READY${CL} Last: $test_name (${BL}$current_test${CL}/${MG}$total_tests_slot${CL}) Failed: ${RD}$failed_tests${CL}"
          elif [ "$status" == "DONE" ]; then
            echo -e "Slot ${YL}$slot${CL}: ${GN}DONE${CL} (${BL}$current_test${CL}/${MG}$total_tests_slot${CL}) Failed: ${RD}$failed_tests${CL}"
          fi
        else
          echo -e "Slot ${YL}$slot${CL}: ${YL}STARTING${CL}..."
        fi
      else
        if [ -e "test.log/slot_$slot/progress" ]; then
          progress_line=$(cat "test.log/slot_$slot/progress")
          IFS=':' read -r status test_name current_test total_tests_slot failed_tests <<<"$progress_line"
          echo -e "Slot ${YL}$slot${CL}: ${GN}FINISHED${CL} (${BL}$current_test${CL}/${MG}$total_tests_slot${CL}) Failed: ${RD}$failed_tests${CL}"
          ((total_tests += total_tests_slot))
          ((total_completed += current_test))
          ((total_failed += failed_tests))
        fi
      fi
    done

    echo "=========================================================================================================="
    echo -e "Overall: ${BL}$total_completed${CL}/${MG}$total_test_methods${CL} test methods completed, ${GN}$total_running${CL} slots running, ${RD}$total_failed${CL} failed"

    # Show failed tests in real-time
    if [ $total_failed -gt 0 ]; then
      echo -e "\n${RD}Failed tests so far:${CL}"
      for ((slot = 1; slot <= parallel; slot++)); do
        if [ -e "test.log/slot_$slot/failed_tests" ]; then
          while IFS= read -r failed_test; do
            if [[ "$failed_test" =~ ^FAILED:(.+)$ ]]; then
              test_name="${BASH_REMATCH[1]}"
              echo -e "  ${RD}•${CL} $test_name (slot $slot)"
            fi
          done <"test.log/slot_$slot/failed_tests"
        fi
      done
    fi

    if [ "$all_done" == "true" ]; then
      echo -e "${GN}All parallel slots completed!${CL}"
      break
    fi

    sleep 2
  done
}

function cleanup_parallel_execution() {
  echo -e "\n${YL}Received interrupt signal - cleaning up parallel execution...${CL}"

  # Stop monitoring
  if [ ! -z "$monitor_pid" ]; then
    kill $monitor_pid 2>/dev/null
    wait $monitor_pid 2>/dev/null
  fi

  # Kill all test slots
  for ((slot = 1; slot <= parallel; slot++)); do
    if [ -e "slot_${slot}.pid" ]; then
      slot_pid=$(<"slot_${slot}.pid")
      if kill -0 $slot_pid 2>/dev/null; then
        echo "Terminating slot $slot (PID: $slot_pid)"
        # Kill the slot process
        kill -TERM $slot_pid 2>/dev/null
        sleep 1
        # Force kill if still running
        if kill -0 $slot_pid 2>/dev/null; then
          kill -KILL $slot_pid 2>/dev/null
        fi
        # Also try to kill any maven processes that might be running
        pkill -f "morphium_test_$slot" 2>/dev/null
      fi
      rm -f "slot_${slot}.pid"
    fi
  done

  # Show failed tests summary before cleanup
  local total_failed=0
  local failed_tests_list=()

  echo -e "\n${YL}Test Results Summary:${CL}"
  echo "=========================================================================================================="

  for ((slot = 1; slot <= parallel; slot++)); do
    if [ -e "test.log/slot_$slot/failed_tests" ]; then
      while IFS= read -r failed_test; do
        if [[ "$failed_test" =~ ^FAILED:(.+)$ ]]; then
          test_name="${BASH_REMATCH[1]}"
          failed_tests_list+=("$test_name (slot $slot)")
          ((total_failed++))
        fi
      done <"test.log/slot_$slot/failed_tests"
    fi
  done

  if [ $total_failed -eq 0 ]; then
    echo -e "${GN}No test failures detected before abort${CL}"
  else
    echo -e "${RD}✗ $total_failed test(s) failed before abort:${CL}"
    echo
    for failed_test in "${failed_tests_list[@]}"; do
      echo -e "  ${RD}•${CL} $failed_test"
    done
    echo
    echo -e "${YL}Failed test logs can be found in:${CL}"
    for failed_test in "${failed_tests_list[@]}"; do
      test_name=$(echo "$failed_test" | cut -d' ' -f1)
      slot_num=$(echo "$failed_test" | grep -o "slot [0-9]*" | cut -d' ' -f2)
      echo -e "  ${BL}test.log/slot_$slot_num/${test_name}.log${CL}"
    done
  fi
  echo "=========================================================================================================="

  # Copy completed logs to main directory for getStats.sh compatibility
  echo -e "${BL}Preserving completed test logs...${CL}"
  for ((slot = 1; slot <= parallel; slot++)); do
    if [ -d "test.log/slot_$slot" ]; then
      for log_file in test.log/slot_$slot/*.log; do
        if [ -e "$log_file" ] && [[ ! "$log_file" =~ slot\.log$ ]]; then
          base_name=$(basename "$log_file" .log)
          # Copy to main directory with slot suffix to avoid conflicts
          cp "$log_file" "test.log/${base_name}_slot${slot}.log"
        fi
      done
    fi
  done

  # Cleanup temporary files
  rm -f test_chunk_*.txt

  echo -e "${RD}Parallel execution aborted by user${CL}"
  exit 130
}

function run_parallel_tests() {
  echo -e "${GN}Starting parallel execution with $parallel slots${CL}"

  # Set up signal handling for clean shutdown
  trap cleanup_parallel_execution SIGINT SIGTERM

  # Calculate total test methods (consistent with serial mode)
  total_test_classes=$(wc -l <$classList | tr -d ' ')
  disabled=$(rg -C1 "^ *@Disabled" | grep -C1 "@Test" | cut -f1 -d: | wc -l)
  disabled3=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances"\)' | cut -f1 -d: | wc -l)
  disabled2=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstancesNo.*"\)' | cut -f1 -d: | wc -l)
  disabled1=$(rg -C1 "^ *@Disabled" | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances.*Only"\)' | cut -f1 -d: | wc -l)
  testMethods=$(cat $classList | xargs -I {} grep {} $filesList | xargs grep -E "@Test" | grep -vc '^ *//' | tr -d '\n' || echo "0")
  testMethods3=$(cat $classList | xargs -I {} grep {} $filesList | xargs grep -E '@MethodSource\("getMorphiumInstances"\)' | grep -vc '^ *//' | tr -d '\n' || echo "0")
  testMethods2=$(cat $classList | xargs -I {} grep {} $filesList | xargs grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' | grep -vc '^ *//' | tr -d '\n' || echo "0")
  testMethods1=$(cat $classList | xargs -I {} grep {} $filesList | xargs grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' | grep -vc '^ *//' | tr -d '\n' || echo "0")
  ((total_test_methods = testMethods + 2 * testMethods3 + testMethods2 * 2 + testMethods1 - disabled - disabled3 * 3 - disabled2 * 2 - disabled1))

  # Split test list into chunks (still use class count for chunk distribution)
  tests_per_slot=$((total_test_classes / parallel))
  remainder=$((total_test_classes % parallel))

  line_start=1
  for ((slot = 1; slot <= parallel; slot++)); do
    chunk_size=$tests_per_slot
    if [ $slot -le $remainder ]; then
      ((chunk_size++))
    fi

    chunk_file="test_chunk_$slot.txt"
    sed -n "${line_start},$((line_start + chunk_size - 1))p" $classList >$chunk_file
    ((line_start += chunk_size))

    echo "Starting slot $slot with $(wc -l <$chunk_file | tr -d ' ') tests, DB: morphium_test_$slot"
    run_test_slot $slot $chunk_file &
    echo $! >"slot_${slot}.pid"
  done

  # Start monitoring in background
  monitor_parallel_progress &
  monitor_pid=$!

  # Wait for all slots
  for ((slot = 1; slot <= parallel; slot++)); do
    if [ -e "slot_${slot}.pid" ]; then
      wait $(<"slot_${slot}.pid") 2>/dev/null
      rm -f "slot_${slot}.pid"
    fi
  done

  # Stop monitoring
  kill $monitor_pid 2>/dev/null
  wait $monitor_pid 2>/dev/null

  # Clear the trap
  trap - SIGINT SIGTERM

  # Aggregate results and show summary
  echo -e "${GN}Aggregating results...${CL}"

  local total_failed=0
  local failed_tests_list=()

  for ((slot = 1; slot <= parallel; slot++)); do
    if [ -d "test.log/slot_$slot" ]; then
      # Copy log files
      for log_file in test.log/slot_$slot/*.log; do
        if [ -e "$log_file" ] && [[ ! "$log_file" =~ slot\.log$ ]]; then
          cp "$log_file" "test.log/$(basename "$log_file" .log)_slot${slot}.log"
        fi
      done

      # Collect failed tests
      if [ -e "test.log/slot_$slot/failed_tests" ]; then
        while IFS= read -r failed_test; do
          if [[ "$failed_test" =~ ^FAILED:(.+)$ ]]; then
            test_name="${BASH_REMATCH[1]}"
            failed_tests_list+=("$test_name (slot $slot)")
            ((total_failed++))
          fi
        done <"test.log/slot_$slot/failed_tests"
      fi
    fi
  done

  # Show results summary
  echo "=========================================================================================================="
  if [ $total_failed -eq 0 ]; then
    echo -e "${GN}✓ All tests passed successfully!${CL}"
  else
    echo -e "${RD}✗ $total_failed test(s) failed:${CL}"
    echo
    for failed_test in "${failed_tests_list[@]}"; do
      echo -e "  ${RD}•${CL} $failed_test"
    done
    echo
    echo -e "${YL}Failed test logs can be found in:${CL}"
    for failed_test in "${failed_tests_list[@]}"; do
      test_name=$(echo "$failed_test" | cut -d' ' -f1)
      slot_num=$(echo "$failed_test" | grep -o "slot [0-9]*" | cut -d' ' -f2)
      echo -e "  ${BL}test.log/${test_name}_slot${slot_num}.log${CL}"
    done
  fi
  echo "=========================================================================================================="

  # Cleanup
  rm -f test_chunk_*.txt
  echo -e "${GN}Parallel execution completed${CL}"
}

##################################################################################################################
#######MAIN LOOP
if [ $parallel -gt 1 ]; then
  run_parallel_tests
else
  # Original sequential logic
  for t in $(<$classList); do
    if grep "$t" $disabledList; then
      continue
    fi
    ((tst = tst + 1))
    while true; do
      tm=$(date +%s)
      if [ "$m" == "." ]; then
        echo "Running Tests in $t" >"test.log/$t.log"
        mvn -Dsurefire.useFile=false $TEST_MVN_PROPS test -Dtest="$t" >>"test.log/$t".log 2>&1 &
        echo $! >$testPid
      else
        echo "Running $m in $t" >"test.log/$t.log"
        mvn -Dsurefire.useFile=false $TEST_MVN_PROPS test -Dtest="$t#$m" >>"test.log/$t.log" 2>&1 &
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
      break
    done
  done
  ##### Mainloop end
  ######################################################
  get_test_stats >failed.txt 2>/dev/null

  testsRun=$(cat failed.txt | grep "Total tests run" | cut -f2 -d:)
  unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
  fail=$(cat failed.txt | grep "Tests failed" | cut -f2 -d:)
  err=$(cat failed.txt | grep "Tests with errors" | cut -f2 -d:)
  num=$numRetries
  if [ "$unsuc" -gt 0 ] && [ "$num" -gt 0 ] && [ "q$1" = "q" ]; then
    while [ "$num" -gt 0 ]; do
      echo -e "${YL}Some tests failed$CL - retrying...."
      ./rerunFailedTests.sh
      ((num = num - 1))
      ((totalRetries = totalRetries + 1))
      retried="$retried\n$t"
      get_test_stats >failed.txt 2>/dev/null
      unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -f2 -d:)
      if [ "$unsuc" -eq 0 ]; then
        break
      fi
    done

  fi
  get_test_stats >failed.txt 2>/dev/null

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
fi # End of sequential execution (else branch)
