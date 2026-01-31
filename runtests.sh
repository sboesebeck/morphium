#!/bin/bash
RD='\033[0;31m'
GN='\033[0;32m'
BL='\033[0;34m'
YL='\033[0;33m'
MG='\033[0;35m'
CN='\033[0;36m'
CL='\033[0m'

PID=$$
TEST_TMP_DIR="/tmp/morphium-runtests-$PID" # Unique temp directory for this run (outside target/ so mvn clean doesn't delete it)

cd $(dirname $0)
# Ensure the temp directory exists
mkdir -p "$TEST_TMP_DIR"

# Cleanup temp directory on exit
cleanup_temp() {
	rm -rf "$TEST_TMP_DIR"
}
trap cleanup_temp EXIT

filesList="$TEST_TMP_DIR/files_$PID.lst"
classList="$TEST_TMP_DIR/classes_$PID.txt"
disabledList="$TEST_TMP_DIR/disabled_$PID.txt"
runLock="$TEST_TMP_DIR/run_$PID.lck"
testPid="$TEST_TMP_DIR/test_$PID.pid"
failPid="$TEST_TMP_DIR/fail_$PID.pid"

# Helper function to safely count patterns in files
# Usage: safe_grep_count "pattern" "file_pattern" "files_list"
# Returns 0 if no files match, otherwise counts matching lines
function safe_grep_count() {
	local pattern="$1"
	local file_pattern="$2"
	local files_list="$3"
	local matching_files
	local count
	matching_files=$(grep "$file_pattern" "$files_list" 2>/dev/null | tr '\n' ' ')
	if [ -z "$matching_files" ]; then
		echo 0
		return
	fi
	# Use eval to expand the file list properly, ensure single numeric output
	count=$(eval "grep -E '$pattern' $matching_files 2>/dev/null" | cut -f2 -d: | grep -vc '^ *//' 2>/dev/null || echo 0)
	# Ensure we return a single number
	echo "${count:-0}" | head -1 | tr -d '\n'
}

function createFileList() {
	rg -l "@Test" . | grep ".java" >$filesList
	rg -l "@ParameterizedTest" . | grep ".java" >>$filesList

	sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
	local tmp_file_list="$TEST_TMP_DIR/files_list_temp_$PID.tmp"
	sort -u "$filesList" | grep "$p" >"$tmp_file_list" && mv -f "$tmp_file_list" "$filesList"
	rg -A2 "^ *@Disabled" . | grep -B2 "public class" | grep : | cut -f1 -d: >"$disabledList"
	local tmp_filtered_list="$TEST_TMP_DIR/filtered_list_temp_$PID.tmp"
	# Initialize empty file in case no matches
	: >"$tmp_filtered_list"
	cat "$filesList" | while read l; do
		if grep "$l" "$disabledList" >/dev/null; then
			echo "$l disabled"
		else
			echo "$l" >>"$tmp_filtered_list"
		fi
	done
	mv -f "$tmp_filtered_list" "$filesList"

}
function cleanup_test_databases() {
	# Drop all test databases matching the pattern from URI to ensure clean state
	# Only runs if not using inmem driver (no MongoDB to connect to)
	local cleanup_uri="${1:-$uri}"
	if [ -z "$cleanup_uri" ] && [ -n "$MONGODB_URI" ]; then
		cleanup_uri="$MONGODB_URI"
	fi
	if [ -z "$cleanup_uri" ]; then
		echo -e "${YL}Warning:${CL} No URI available for database cleanup"
		return 1
	fi
	if [ "$driver" == "inmem" ]; then
		return 0
	fi

	# Extract database name from URI (format: mongodb://host:port/database or mongodb://host1,host2/database)
	local db_from_uri=$(echo "$cleanup_uri" | sed -n 's|.*mongodb://[^/]*/\([^?]*\).*|\1|p')
	if [ -z "$db_from_uri" ]; then
		db_from_uri="morphium_test" # fallback
	fi
	# Remove any trailing options after ?
	db_from_uri="${db_from_uri%%\?*}"

	echo -e "${BL}Info:${CL} Cleaning up ${db_from_uri}* databases..."
	local count=0
	while read db rst; do
		if [[ "$db" == ${db_from_uri}* ]]; then
			echo -e "  Dropping db ${YL}$db${CL}"
			mongosh "$cleanup_uri" --quiet --eval "use $db; db.dropDatabase()" >/dev/null 2>&1
			((count++))
		fi
	done < <(mongosh "$cleanup_uri" --quiet --eval "show databases" 2>/dev/null)
	if [ "$count" -gt 0 ]; then
		echo -e "${GN}Info:${CL} Dropped $count test database(s)"
	fi
}

function quitting() {

	# Stop a locally started MorphiumServer cluster (if any) before tearing down logs/state.
	if type _ms_local_cleanup >/dev/null 2>&1; then
		_ms_local_cleanup
	fi

	# Clean up test databases on abort (only morphium_test* databases)
	if [ "$driver" != "inmem" ]; then
		cleanup_test_databases
	fi

	if [ -e $testPid ]; then
		kill -9 $(<$testPid) >/dev/null 2>&1
	fi
	if [ -e $failPid ]; then
		kill -9 $(<$failPid) >/dev/null 2>&1
	fi
	rm -f $testPid $failPid >/dev/null 2>&1

	# Preserve any slot logs before cleaning up
	aggregate_slot_logs

	if [ -n "$t" ]; then
		echo -e "${RD}Shutting down...${CL} current test $t"
		echo "Removing unfinished test $t"
		rm -f test.log/$t.log
		get_test_stats >"$TEST_TMP_DIR/failed.txt" 2>/dev/null
		# Copy to test.log/ so it persists after cleanup
		cp "$TEST_TMP_DIR/failed.txt" "test.log/failed.txt" 2>/dev/null
		echo "List of failed tests in test.log/failed.txt"
		cat "test.log/failed.txt"
	else
		echo -e "${YL} Shutting down...$CL"
	fi
	rm -f "$runLock" "$disabledList"
	rm -f "$filesList" "$classList"
	rm -rf "$TEST_TMP_DIR" # Clean up the dedicated temp directory
	exit
}

source "$(dirname "$0")/scripts/stats.sh"

# Aggregate per-slot logs into the shared test.log directory so stats work after interruptions.
function aggregate_slot_logs() {
	# Nothing to do if no slot directories exist
	if ! compgen -G "test.log/slot_*" >/dev/null 2>&1; then
		return 0
	fi

	mkdir -p test.log/temp_aggregate

	local old_nullglob=$(shopt -p nullglob)
	shopt -s nullglob

	for log_file in test.log/slot_*/*.log; do
		[ -f "$log_file" ] || continue
		[[ "$log_file" == */slot.log ]] && continue

		local test_class
		test_class=$(basename "$log_file" .log)
		local dest_file="test.log/temp_aggregate/${test_class}.log"

		if [ ! -f "$dest_file" ]; then
			cp "$log_file" "$dest_file"
		else
			if ! grep -q "BUILD FAILURE" "$log_file" 2>/dev/null && grep -q "BUILD FAILURE" "$dest_file" 2>/dev/null; then
				cp "$log_file" "$dest_file"
			elif grep -q "BUILD SUCCESS" "$log_file" 2>/dev/null && ! grep -q "BUILD SUCCESS" "$dest_file" 2>/dev/null; then
				cp "$log_file" "$dest_file"
			elif [ "$log_file" -nt "$dest_file" ]; then
				cp "$log_file" "$dest_file"
			fi
		fi

	done

	# Restore nullglob to its previous state
	if [ -n "$old_nullglob" ]; then
		eval "$old_nullglob"
	else
		shopt -u nullglob
	fi

	if [ -d "test.log/temp_aggregate" ]; then
		local temp_file
		local old_nullglob_mv=$(shopt -p nullglob)
		shopt -s nullglob
		for temp_file in test.log/temp_aggregate/*.log; do
			[ -f "$temp_file" ] || continue
			mv -f "$temp_file" "test.log/$(basename "$temp_file")"
		done
		if [ -n "$old_nullglob_mv" ]; then
			eval "$old_nullglob_mv"
		else
			shopt -u nullglob
		fi
		rmdir test.log/temp_aggregate 2>/dev/null
	fi

	return 0
}

source "$(dirname "$0")/scripts/morphium_server.sh"

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
morphiumserverLocalMode=0
startMorphiumserverLocal=0
morphiumserverSingleNode=0
morphiumserverLocalStarted=0
morphiumserverLocalPidDir=".morphiumserver-local"
# Connection management for MorphiumServer (auto-calculated from --parallel if not set)
morphiumserverMaxConnections=""
morphiumserverSocketTimeout=""
testname=""    # Stores the class pattern from --test
methodname="." # Stores the method pattern from --test (defaults to all methods)

# Save original arguments for stats processing
original_args=("$@")

while [ "q$1" != "q" ]; do

	if [ "q$1" == "q--nodel" ]; then
		nodel=1
		shift
	elif [ "q$1" == "q--help" ] || [ "q$1" == "-h" ]; then
		echo -e "Usage ${BL}$0$CL [--OPTION...]"
		echo -e "${BL}--skip | --continue$CL        - if present, already run tests will be skipped"
		echo -e "${BL}--restart$CL     - forget about existing test logs, restart"
		echo -e "${BL}--logs$CL ${GN}NUM$CL    - number of log lines to show"
		echo -e "${BL}--refresh$CL ${GN}NUM$CL - refresh view every NUM secs"
		echo -e "${BL}--retry$CL ${GN}NUM$CL   - number of retries on error in tests - default $YL$numRetries$CL"
		echo -e "${BL}--tags$CL ${GN}LIST$CL   - include JUnit5 tags (comma-separated)"
		echo -e "                   Available: ${GN}core, messaging, driver, inmemory, aggregation, cache, admin, performance, encryption, jms, geo, util, external${CL}"
		echo -e "${BL}--exclude-tags$CL ${GN}LIST$CL - exclude JUnit5 tags (comma-separated)"
		echo -e "${BL}--driver$CL ${GN}NAME$CL - morphium driver: ${GN}pooled | single | inmem | all${CL}"
		echo -e "${BL}--uri$CL ${GN}URI$CL     - mongodb connection string (or use MONGODB_URI env)"
		echo -e "${BL}--verbose$CL     - enable verbose test logs"
		echo -e "${BL}--user$CL ${GN}USERNAME$CL - authenticate as user"
		echo -e "${BL}--pass$CL ${GN}password$CL - authenticate with password"
		echo -e "${BL}--authdb$CL ${GN}DATABASE$CL - authentication DB"
		echo -e "${BL}--external$CL    - enable external MongoDB tests (activates -Pexternal profile)"
		echo -e "                     ${YL}NOTE:${CL} This option is for tests that require a real MongoDB instance."
		echo -e "${BL}--morphium-server$CL    - start single MorphiumServer on localhost:17017"
		echo -e "${BL}--morphium-server-replicaset$CL - start 3-node MorphiumServer replica set (17017-17019)"
		echo -e "                     ${YL}NOTE:${CL} Replica set mode has limited data sync - prefer single node for testing"
		echo -e "${BL}--morphiumserver-local | --localhost-rs$CL - (deprecated) alias for --morphium-server-replicaset"
		echo -e "${BL}--parallel$CL ${GN}N$CL    - run tests in N parallel slots (1-16, each with unique DB)"
		echo -e "${BL}--max-connections$CL ${GN}N$CL - MorphiumServer max connections (default: auto from --parallel)"
		echo -e "${BL}--socket-timeout$CL ${GN}N$CL  - MorphiumServer socket timeout in seconds (default: 30)"
		echo -e "${BL}--rerunfailed$CL   - rerun only previously failed tests (uses integrated stats)"
		echo -e "                     ${YL}NOTE:${CL} Conflicts with --restart (which cleans logs)"
		echo -e "${BL}--test$CL ${GN}PATTERN$CL - Specify a pattern to run only matching test classes or methods (e.g., 'CacheTests', 'CacheTests#testCacheEntry')"
		echo
		echo -e "${BL}--watchlogs$CL     - show logs of all running tests side by side, restart if one finishes to update"
		echo -e "${BL}--showlog$CL          - let's you choose a log to show"
		echo -e "${BL}--taillog$CL          - tails a specific log"
		echo -e "${BL}--showfailed$CL       - let's you choose a log from failed test classes to view"
		echo -e "${BL}--stats$CL         - show test statistics and failed tests (replaces getStats.sh)"
		echo -e "if neither ${BL}--restart${CL} nor ${BL}--skip${CL} are set, you will be asked what to do"
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
		echo -e "  ${BL}./runtests.sh --driver all${CL}                   # Run tests with all available drivers"
		echo -e "  ${RD}./runtests.sh --external --driver inmem${CL}      # ERROR: Conflicting options!"
		echo -e "  ${BL}./runtests.sh --morphium-server${CL}             # Single-node MorphiumServer (recommended)"
		echo -e "  ${BL}./runtests.sh --morphium-server-replicaset${CL}  # 3-node MorphiumServer replica set"
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
		echo -e "  ${BL}./runtests.sh --stats --noreason${CL}             # Show only failed test names (no failure reason)"
		echo -e "  ${BL}./runtests.sh --stats --nosum${CL}                # Show only failed tests (no summary)"
		echo
		exit 0
	elif [ "q$1" == "q--showfailed" ]; then
		./scripts/showlog.sh --failed
		exit 0
	elif [ "q$1" == "q--taillog" ]; then
		./scripts/showlog.sh --tail
		exit 0

	elif [ "q$1" == "q--showlog" ]; then
		./scripts/showlog.sh
		exit 0

	elif [ "q$1" == "q--watchlogs" ]; then
		./scripts/viewTestlogs.sh
		exit 0

	elif [ "q$1" == "q--skip" ]; then
		skip=1
		shift
	elif [ "q$1" == "q--restart" ]; then
		explicitRestart=1
		rm -rf test.log
		rm -f "$TEST_TMP_DIR/startTS"
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
	elif [ "q$1" == "q--max-connections" ]; then
		shift
		morphiumserverMaxConnections=$1
		shift
		if ! [[ "$morphiumserverMaxConnections" =~ ^[0-9]+$ ]] || [ "$morphiumserverMaxConnections" -lt 10 ]; then
			echo -e "${RD}Error: --max-connections must be a number >= 10${CL}"
			exit 1
		fi
	elif [ "q$1" == "q--socket-timeout" ]; then
		shift
		morphiumserverSocketTimeout=$1
		shift
		if ! [[ "$morphiumserverSocketTimeout" =~ ^[0-9]+$ ]] || [ "$morphiumserverSocketTimeout" -lt 5 ]; then
			echo -e "${RD}Error: --socket-timeout must be a number >= 5 (seconds)${CL}"
			exit 1
		fi
	elif [ "q$1" == "q--uri" ]; then
		shift
		uri=$1
		useExternal=1 # URI implies external MongoDB
		shift
	elif [ "q$1" == "q--verbose" ]; then
		verbose=1
		shift
	elif [ "q$1" == "q--external" ]; then
		useExternal=1
		shift
	elif [ "q$1" == "q--morphium-server" ]; then
		# Single-node MorphiumServer on localhost:17017 (recommended for testing)
		startMorphiumserverLocal=1
		morphiumserverSingleNode=1
		useExternal=1
		if [ -z "$uri" ]; then
			uri="mongodb://localhost:17017/morphium_tests"
		fi
		if [ -z "$driver" ]; then
			driver="pooled"
		fi
		shift
	elif [ "q$1" == "q--morphium-server-replicaset" ] || [ "q$1" == "q--start-morphiumserver-local" ]; then
		# 3-node MorphiumServer replica set on 17017-27019
		# Note: Each node has isolated InMemoryDriver - limited data sync between nodes
		startMorphiumserverLocal=1
		morphiumserverSingleNode=0
		useExternal=1
		if [ -z "$uri" ]; then
			uri="mongodb://localhost:17017,localhost:17018,localhost:17019/morphium_tests"
		fi
		if [ -z "$driver" ]; then
			driver="pooled"
		fi
		shift
	elif [ "q$1" == "q--localhost-rs" ] || [ "q$1" == "q--morphiumserver-local" ]; then
		# Deprecated aliases for --morphium-server-replicaset
		echo -e "${YL}Warning:${CL} $1 is deprecated, use --morphium-server or --morphium-server-replicaset instead"
		startMorphiumserverLocal=1
		morphiumserverSingleNode=0
		useExternal=1
		if [ -z "$uri" ]; then
			uri="mongodb://localhost:17017,localhost:17018,localhost:17019/morphium_tests"
		fi
		if [ -z "$driver" ]; then
			driver="pooled"
		fi
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
	elif [ "q$1" == "q--test" ]; then
		shift
		test_pattern="$1"
		if [ -z "$test_pattern" ]; then
			echo -e "${RD}Error:${CL} --test requires a class or class#method pattern"
			exit 1
		fi
		if [[ "$test_pattern" == *"#"* ]]; then
			testname="${test_pattern%#*}"
			methodname="${test_pattern#*#}"
		else
			testname="$test_pattern"
			methodname="." # Default to all methods if only class is specified
		fi
		skip=1 # Implies skipping confirmation if --test is used
		shift
	else
		echo "Unknown option $1"
		exit 1
		# echo "Do not know option $1 - assuming it is a testname"
		# break
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

if [ -z "$driver" ]; then
	if [ "$useExternal" -eq 1 ]; then
		driver="pooled"
		echo -e "${BL}Info:${CL} External mode detected - defaulting to --driver pooled"
	else
		driver="inmem"
		echo -e "${YL}Info:${CL} No driver specified, defaulting to --driver inmem (use --external for external MongoDB drivers)"
	fi
fi

# Pooled/single driver implies external mode (needs MongoDB connection)
if [ "$driver" == "pooled" ] || [ "$driver" == "single" ]; then
	if [ "$useExternal" -eq 0 ]; then
		useExternal=1
		echo -e "${BL}Info:${CL} Driver '$driver' requires external MongoDB - enabling external mode"
	fi
fi

# Auto-exclude external tests when using inmem driver
if [ "$driver" == "inmem" ]; then
	if [ -z "$excludeTags" ]; then
		excludeTags="external"
	elif [[ ! "$excludeTags" == *"external"* ]]; then
		excludeTags="$excludeTags,external"
	fi
	echo -e "${BL}Info:${CL} InMemory driver selected - automatically excluding 'external' tagged tests"
fi

# If requested, ensure a local MorphiumServer cluster is reachable (and optionally start it).
if [ "$startMorphiumserverLocal" -eq 1 ]; then
	useExternal=1
fi

if [ "$startMorphiumserverLocal" -eq 1 ] && [ -z "$uri" ]; then
	uri="mongodb://localhost:17017,localhost:17018,localhost:17019/morphium_tests"
fi

if [ "$startMorphiumserverLocal" -eq 1 ] && [ -z "$driver" ]; then
	driver="pooled"
fi

if [ "$morphiumserverLocalMode" -eq 1 ] || [ "$startMorphiumserverLocal" -eq 1 ]; then
	_ms_local_ensure_cluster "$uri"
fi

# Auto-exclude failover tests when using MorphiumServer
# (MorphiumServer doesn't support StepDownCommand for failover testing)
if [ "$startMorphiumserverLocal" -eq 1 ]; then
	if [ -z "$excludeTags" ]; then
		excludeTags="failover"
	else
		if [[ ! "$excludeTags" == *"failover"* ]]; then
			excludeTags="$excludeTags,failover"
		fi
	fi
	echo -e "${BL}Info:${CL} MorphiumServer selected - automatically excluding 'failover' tagged tests"
fi

# Handle --rerunfailed option early to bypass interactive prompts
if [ "$rerunfailed" -eq 1 ]; then
	echo -e "${MG}Rerunning${CL} ${CN}failed tests...${CL}"

	# For rerunfailed, we want to use existing logs to determine what failed
	# Set nodel=1 to preserve existing logs for analysis
	# DO NOT set skip=1 because we WANT to rerun the failed tests
	nodel=1

	# Use getFailedTests.sh to get method-level failures
	if [ ! -f "$(dirname "$0")/scripts/getFailedTests.sh" ]; then
		echo -e "${RD}Error:${CL} scripts/getFailedTests.sh not found!"
		exit 1
	fi

	# Get failed tests in ClassName.methodName format (skip the "Looking for" message)
	failed_tests=$("$(dirname "$0")/scripts/getFailedTests.sh" 2>/dev/null | grep -v "Looking for")

	# Filter by class pattern if provided (e.g., ./runtests.sh --rerunfailed DataTypeTests)
	# Note: $1 contains the class pattern since we're processing this before p=$1 assignment
	if [ "q$testname" != "q" ] && [ "$testname" != "." ]; then
		failed_tests=$(echo "$failed_tests" | grep "$testname")
		echo "Filtering failed tests by pattern: $testname"
	fi

	if [ -z "$failed_tests" ]; then
		echo -e "${GN}No failed tests found to rerun!${CL}"
		exit 0
	fi

	# Convert to ClassName#methodName format and write to classList
	# Use process substitution to avoid subshell (which would lose the file writes)
	: >$classList
	while IFS= read -r failed_test; do
		# Skip empty lines
		[ -z "$failed_test" ] && continue

		# Handle method source failures (contain '>') - these are initialization errors
		# e.g., "de.caluga.test.Foo>MultiDriverTestBase.getMorphiumInstances"
		# For these, we run the entire class, not a specific method
		if [[ "$failed_test" == *">"* ]]; then
			# Extract just the class name (everything before '>'), then remove method source part
			class_part=$(echo "$failed_test" | cut -d'>' -f1)
			# Run all methods in the class
			echo "$class_part#." >>$classList
		else
			# Normal test method failure: ClassName.methodName
			# Split ClassName.methodName into ClassName#methodName*
			# Note: We add * suffix to match parameterized test variants (e.g., testFoo[1], testFoo[2])
			class_part=$(echo "$failed_test" | sed 's/\.[^.]*$//')
			method_part=$(echo "$failed_test" | sed 's/^.*\.//')
			echo "$class_part#$method_part*" >>$classList
		fi
	done < <(echo "$failed_tests")

	# Count unique classes and total methods
	num_methods=$(wc -l <$classList | tr -d ' ')
	num_classes=$(cut -d'#' -f1 <$classList | sort -u | wc -l | tr -d ' ')
	echo -e "${BL}Found $num_methods failed test methods in $num_classes test classes${CL}"

	# Delete old log files for failed tests so we only see current results
	# This prevents stale failures from appearing when tests don't run (e.g., different driver)
	echo -e "${YL}Removing old log files for failed tests...${CL}"
	while IFS= read -r cls; do
		old_log="test.log/${cls}.log"
		if [ -f "$old_log" ]; then
			rm -f "$old_log"
		fi
	done < <(cut -d'#' -f1 <$classList | sort -u)

	# Create file list based on unique classes
	: >$filesList
	while IFS= read -r cls; do
		# Convert class name to file path
		file_path=$(echo "$cls" | sed 's/\./\//g')
		file_path="src/test/java/${file_path}.java"
		if [ -f "$file_path" ]; then
			echo "$file_path" >>$filesList
		fi
	done < <(cut -d'#' -f1 <$classList | sort -u)

	# Create disabled list (empty for failed test reruns)
	: >$disabledList

	# Skip the normal file creation logic
	skip_file_creation=1
fi
if [ "$explicitRestart" -eq 0 ] && [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
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
trap _ms_local_cleanup EXIT

# After argument parsing, ensure p and m are set correctly based on testname and methodname
if [ -n "$testname" ]; then
	p="$testname"
else
	p="." # Default to all classes if --test was not used
fi

if [ -n "$methodname" ]; then
	m="$methodname"
else
	m="." # Default to all methods if --test was not used or no method was specified
fi
m=$(echo "$m" | tr -d '"')

if [ "$skip_file_creation" != "1" ]; then
	createFileList
fi
# If include tags specified, filter classes/files to only those tagged
if [ -n "$includeTags" ]; then
	tagPattern=$(echo "$includeTags" | sed 's/,/|/g')
	tmpTagged="$TEST_TMP_DIR/tmp_tag_files_$PID.txt"
	: >"$tmpTagged"
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
	sort -u "$tmpTagged" -o "$tmpTagged"
	if [ -s "$tmpTagged" ]; then
		# Intersect with current files list (only keep files we already identified as tests)
		grep -F -f "$tmpTagged" "$filesList" >"$TEST_TMP_DIR/files_temp_filter_$PID.tmp" || true
		mv "$TEST_TMP_DIR/files_temp_filter_$PID.tmp" "$filesList"
		# Rebuild class list from filtered files
		sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
	fi
	rm -f "$tmpTagged"
fi

if [ "q$testname" = "q" ]; then
	# no test to run specified
	# Handle exclude tags
	if [ -n "$excludeTags" ]; then
		excludePattern=$(echo "$excludeTags" | sed 's/,/|/g')
		tmpExcluded="$TEST_TMP_DIR/tmp_exclude_files_$PID.txt"
		: >"$tmpExcluded"
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
		sort -u "$tmpExcluded" -o "$tmpExcluded"
		if [ -s "$tmpExcluded" ]; then
			# Remove excluded files from the files list
			grep -v -F -f "$tmpExcluded" "$filesList" >"$TEST_TMP_DIR/files_temp_filter_exclude_$PID.tmp" || cp "$filesList" "$TEST_TMP_DIR/files_temp_filter_exclude_$PID.tmp"
			mv "$TEST_TMP_DIR/files_temp_filter_exclude_$PID.tmp" "$filesList"
			# Rebuild class list from filtered files (but NOT for --rerunfailed which already has classList set)
			if [ "$rerunfailed" -ne 1 ]; then
				sort -u $filesList | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >$classList
			fi
		fi
		rm -f "$tmpExcluded"
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
	echo "Limiting to test $testname"
	echo "$testname" >$classList
fi
# read
cnt=$(wc -l <$classList | tr -d ' ')
if [ "$cnt" -eq 0 ]; then
	echo "no matching class found for $p"
	exit 1
fi

# Skip test method counting for --rerunfailed since we already know exactly which methods to run
if [ "$rerunfailed" -eq 1 ]; then
	# For --rerunfailed, testMethods = number of entries in classList (each is a specific method)
	testMethods=$cnt
else
	disabled=$(rg -C1 "^ *@Disabled" . | grep -C1 "@Test" | grep : | cut -f1 -d: | grep "$p" | wc -l)
	disabled3=$(rg -C1 "^ *@Disabled" . | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances"\)' | grep : | cut -f1 -d: | grep "$p" | wc -l)
	disabled2=$(rg -C1 "^ *@Disabled" . | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstancesNo.*"\)' | grep : | cut -f1 -d: | grep "$p" | wc -l)
	disabled1=$(rg -C1 "^ *@Disabled" . | grep -C2 "@Test" | grep -C2 -E '@MethodSource\("getMorphiumInstances.*Only"\)' | grep : | cut -f1 -d: | grep "$p" | wc -l)
	testMethods=$(safe_grep_count "@Test|@ParameterizedTest" "$p" "$filesList")
	testMethods3=$(safe_grep_count '@MethodSource\("getMorphiumInstances"\)' "$p" "$filesList")
	testMethods2=$(safe_grep_count '@MethodSource\("getMorphiumInstancesNo.*"\)' "$p" "$filesList")
	testMethods1=$(safe_grep_count '@MethodSource\("getMorphiumInstances.*Only"\)' "$p" "$filesList")
	((testMethods = testMethods + 2 * testMethods3 + testMethods2 * 2 + testMethods1 - disabled - disabled3 * 3 - disabled2 * 2 - disabled1))
fi
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
	echo -e "${BL}Info:${CL} Cleaning up - cleansing logs..."
	rm -rf test.log >/dev/null 2>&1
	rm -f startTS >/dev/null 2>&1
	mkdir test.log
	# Clean up test databases before starting fresh
	cleanup_test_databases
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

# Increase connection pool for parallel tests
# MorphiumServer uses NIO so can handle many connections efficiently
# AsyncOperationTest alone uses 1134+ connections, messaging tests need many more for parallel ops
if [ -n "$parallel" ] && [ "$parallel" -gt 1 ]; then
	if [ "$startMorphiumserverLocal" -eq 1 ]; then
		pool_size=$((2000 + parallel * 500)) # Base 2000 + 500 per parallel slot (matches server side)
	else
		# For real MongoDB with replica set, need more connections since all writes go to primary
		# ExclusiveMessageTests creates 5+ Morphium instances with messaging, each with change streams
		pool_size=$((1500 + parallel * 500)) # Base 1500 + 500 per parallel slot
	fi
	MVN_PROPS="$MVN_PROPS -Dmorphium.maxConnections=$pool_size"
	echo -e "${BL}Info:${CL} Increased client pool to ${pool_size} connections for ${parallel} parallel slots"
fi

mvn $MVN_PROPS compile test-compile >/dev/null || {
	echo -e "${RD}Error:${CL} Compilation failed!"
	exit 1
}

TEST_MVN_PROPS="$MVN_PROPS -Dmaven.compiler.skip=true"

tst=0
echo -e "${GN}Starting tests..${CL}" >"$TEST_TMP_DIR/failed.txt"
# running getfailedTests in background
{
	touch $runLock
	while [ -e $runLock ]; do
		get_test_stats >"$TEST_TMP_DIR/failed.tmp" 2>/dev/null
		mv "$TEST_TMP_DIR/failed.tmp" "$TEST_TMP_DIR/failed.txt"
		sleep $refresh
	done >/dev/null 2>&1

} &

echo $! >$failPid
if [ -e "$TEST_TMP_DIR/startTS" ]; then
	start=$(<"$TEST_TMP_DIR/startTS")
else
	start=$(date +%s)
	echo "$start" >"$TEST_TMP_DIR/startTS"
fi
testsRun=0
unsuc=0
fail=0
err=0
##################################################################################################################
#######PARALLEL EXECUTION FUNCTIONS
#
# Best-effort test method counting for progress reporting.
# JUnit5 parameterized tests execute multiple times depending on the @MethodSource and selected drivers.
function _calc_allowed_driver_flags() {
	local external_enabled=0
	if [ "${useExternal:-0}" -eq 1 ]; then external_enabled=1; fi
	if [ -n "${uri:-}" ]; then external_enabled=1; fi
	if [ -z "${uri:-}" ] && [ -n "${MONGODB_URI:-}" ]; then external_enabled=1; fi

	local allow_pooled=0
	local allow_single=0
	local allow_inmem=0

	if [ "$external_enabled" -eq 0 ]; then
		allow_inmem=1
	else
		if [ -z "${driver:-}" ]; then
			allow_pooled=1
			allow_single=1
			allow_inmem=1
		else
			IFS=',' read -r -a drvArr <<<"${driver}"
			for tok in "${drvArr[@]}"; do
				local t
				t="$(echo "$tok" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
				case "$t" in
				pooled | pooleddriver) allow_pooled=1 ;;
				single | singleconnect | singlemongoconnectdriver) allow_single=1 ;;
				inmem | inmemory | inmemorydriver) allow_inmem=1 ;;
				all)
					allow_pooled=1
					allow_single=1
					allow_inmem=1
					;;
				esac
			done
		fi
	fi

	echo "$allow_pooled:$allow_single:$allow_inmem"
}

function _count_test_methods_in_file() {
	local test_file="$1"
	if [ -z "$test_file" ] || [ ! -f "$test_file" ]; then
		echo 0
		return
	fi

	local flags
	flags="$(_calc_allowed_driver_flags)"
	local allow_pooled allow_single allow_inmem
	IFS=':' read -r allow_pooled allow_single allow_inmem <<<"$flags"

	local mult_getInstances=$((allow_pooled + allow_inmem)) # getMorphiumInstances(false, true, true)
	local mult_noInMem=$((allow_pooled + allow_single))     # getMorphiumInstancesNoInMem(true, false, true)
	local mult_pooledOnly=$allow_pooled
	local mult_singleOnly=$allow_single
	local mult_inMemOnly=$allow_inmem

	local base_tests
	base_tests=$(grep -E '@Test\b|@RepeatedTest\b|@TestFactory\b' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
	local param_total
	param_total=$(grep -E '@ParameterizedTest\b' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")

	local ms_getInstances
	ms_getInstances=$(grep -E '@MethodSource\("getMorphiumInstances"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
	local ms_noSingle
	ms_noSingle=$(grep -E '@MethodSource\("getMorphiumInstancesNoSingle"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
	local ms_noInMem
	ms_noInMem=$(grep -E '@MethodSource\("getMorphiumInstancesNoInMem"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
	local ms_pooledOnly
	ms_pooledOnly=$(grep -E '@MethodSource\("getMorphiumInstancesPooledOnly"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
	local ms_singleOnly
	ms_singleOnly=$(grep -E '@MethodSource\("getMorphiumInstancesSingleOnly"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")
	local ms_inMemOnly
	ms_inMemOnly=$(grep -E '@MethodSource\("getMorphiumInstancesInMemOnly"\)' "$test_file" 2>/dev/null | grep -vc '^ *//' 2>/dev/null | tr -d '\n' || echo "0")

	local special_sum=$((ms_getInstances + ms_noSingle + ms_noInMem + ms_pooledOnly + ms_singleOnly + ms_inMemOnly))
	local param_other=$((param_total - special_sum))
	if [ "$param_other" -lt 0 ]; then
		param_other=0
	fi

	echo $((base_tests + param_other + \
	ms_getInstances * mult_getInstances + \
	ms_noSingle * mult_getInstances + \
	ms_noInMem * mult_noInMem + \
	ms_pooledOnly * mult_pooledOnly + \
	ms_singleOnly * mult_singleOnly + \
	ms_inMemOnly * mult_inMemOnly))
}

# Track progress for long running classes by monitoring the Surefire XML.
function monitor_class_progress() {
	local slot_id="$1"
	local test_class="$2"
	local class_methods="$3"
	local slot_total="$4"
	local already_completed="$5"
	local failed_tests="$6"
	local mvn_pid="$7"

	# Nothing to monitor if class method count is unknown
	if [ -z "$class_methods" ] || [ "$class_methods" -le 0 ]; then
		# Wait until mvn process finishes so caller can safely waitpid
		while kill -0 "$mvn_pid" 2>/dev/null; do
			sleep 1
		done
		return 0
	fi

	local progress_file="test.log/slot_${slot_id}/progress"
	local report_file="target/surefire-reports/TEST-${test_class}.xml"
	local last_reported=-1

	while kill -0 "$mvn_pid" 2>/dev/null; do
		if [ -f "$report_file" ]; then
			local completed_in_class
			completed_in_class=$(grep -c "<testcase" "$report_file" 2>/dev/null || echo 0)
			if [ -z "$completed_in_class" ]; then
				completed_in_class=0
			fi

			if [ "$completed_in_class" -gt "$class_methods" ]; then
				completed_in_class="$class_methods"
			fi

			local overall=$((already_completed + completed_in_class))
			if [ "$overall" -ne "$last_reported" ]; then
				echo "RUNNING:$test_class:$overall:$slot_total:$failed_tests" >"$progress_file"
				last_reported="$overall"
			fi
		fi
		sleep 1
	done
}

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
				((slot_testMethods += $(_count_test_methods_in_file "$test_file")))
			fi
		fi
	done <"$test_chunk_file"

	local current_test_methods=0
	local current_test_classes=0
	local failed_tests=0

	while IFS= read -r t; do
		# Parse ClassName#methodName format if present (for --rerunfailed) - do this FIRST
		test_class="$t"
		test_method="$m"
		if [[ "$t" == *"#"* ]]; then
			test_class="${t%#*}"
			test_method="${t#*#}"
		fi

		if grep "$test_class" $disabledList >/dev/null; then
			continue
		fi

		# Calculate test methods for this class (use test_class, not t, to avoid #method suffix)
		local fn=$(echo "$test_class" | tr "." "/")
		local test_file=$(grep "$fn" $filesList | head -1 2>/dev/null)
		local class_test_methods=0
		if [ -n "$test_file" ] && [ -f "$test_file" ]; then
			class_test_methods=$(_count_test_methods_in_file "$test_file")
		fi

		((current_test_classes++))

		# Update progress before starting test
		echo "RUNNING:$test_class:$current_test_methods:$slot_testMethods:$failed_tests" >"test.log/slot_$slot_id/progress"

		# Remove previous Surefire XML to avoid reading stale progress
		local xml_report="target/surefire-reports/TEST-$test_class.xml"
		rm -f "$xml_report"

		local mvn_pid monitor_pid
		if [ "$test_method" == "." ]; then
			echo "Slot $slot_id: Running $test_class ($current_test_classes/$total_test_classes, $current_test_methods methods)" >>"test.log/slot_$slot_id/slot.log"
			mvn -Dsurefire.useFile=false $slot_mvn_props surefire:test -Dtest="$test_class" >"test.log/slot_$slot_id/$test_class.log" 2>&1 &
			mvn_pid=$!
		else
			echo "Slot $slot_id: Running $test_class#$test_method ($current_test_classes/$total_test_classes, $current_test_methods methods)" >>"test.log/slot_$slot_id/slot.log"
			mvn -Dsurefire.useFile=false $slot_mvn_props surefire:test -Dtest="$test_class#$test_method" >"test.log/slot_$slot_id/$test_class.log" 2>&1 &
			mvn_pid=$!
		fi

		monitor_pid=0
		if [ "$class_test_methods" -gt 0 ]; then
			monitor_class_progress "$slot_id" "$test_class" "$class_test_methods" "$slot_testMethods" "$current_test_methods" "$failed_tests" "$mvn_pid" &
			monitor_pid=$!
		fi

		wait $mvn_pid
		exit_code=$?

		if [ "$monitor_pid" -ne 0 ]; then
			# Ensure monitor terminates before proceeding (it exits when mvn pid is gone)
			wait $monitor_pid 2>/dev/null
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
		local total_failed=0
		local total_test_methods_from_slots=0
		local unique_failed_tests=()

		for ((slot = 1; slot <= parallel; slot++)); do
			local slot_status=""
			local slot_current_test=0
			local slot_total_tests=0
			local slot_failed_tests=0
			local slot_test_name=""

			# Check if slot is still running
			local slot_running=false
			if [ -e "slot_${slot}.pid" ] && kill -0 $(<"slot_${slot}.pid") 2>/dev/null; then
				slot_running=true
				all_done=false
			fi

			# Read progress information if available
			if [ -e "test.log/slot_$slot/progress" ]; then
				progress_line=$(cat "test.log/slot_$slot/progress")
				IFS=':' read -r slot_status slot_test_name slot_current_test slot_total_tests slot_failed_tests <<<"$progress_line"

				# Add to totals (avoid double counting by reading each slot's progress only once per iteration)
				((total_completed += slot_current_test))
				((total_failed += slot_failed_tests))
				((total_test_methods_from_slots += slot_total_tests))

				# Display slot status
				if [ "$slot_running" == "true" ]; then
					if [ "$slot_status" == "RUNNING" ]; then
						((total_running++))
						echo -e "Slot ${YL}$slot${CL}: ${GN}RUNNING${CL} $slot_test_name (${BL}$slot_current_test${CL}/${MG}$slot_total_tests${CL}) Failed: ${RD}$slot_failed_tests${CL}"
					else
						echo -e "Slot ${YL}$slot${CL}: ${BL}READY${CL} Last: $slot_test_name (${BL}$slot_current_test${CL}/${MG}$slot_total_tests${CL}) Failed: ${RD}$slot_failed_tests${CL}"
					fi
				else
					echo -e "Slot ${YL}$slot${CL}: ${GN}FINISHED${CL} (${BL}$slot_current_test${CL}/${MG}$slot_total_tests${CL}) Failed: ${RD}$slot_failed_tests${CL}"
				fi

				# Collect unique failed tests for display
				if [ -e "test.log/slot_$slot/failed_tests" ] && [ $slot_failed_tests -gt 0 ]; then
					while IFS= read -r failed_test; do
						if [[ "$failed_test" =~ ^FAILED:(.+)$ ]]; then
							test_name="${BASH_REMATCH[1]}"
							# Check if this test is already in our unique list
							local already_listed=false
							for existing_test in "${unique_failed_tests[@]}"; do
								if [[ "$existing_test" == "$test_name" ]]; then
									already_listed=true
									break
								fi
							done
							if [ "$already_listed" == "false" ]; then
								unique_failed_tests+=("$test_name (slot $slot)")
							fi
						fi
					done <"test.log/slot_$slot/failed_tests"
				fi
			else
				if [ "$slot_running" == "true" ]; then
					echo -e "Slot ${YL}$slot${CL}: ${YL}STARTING${CL}..."
				else
					echo -e "Slot ${YL}$slot${CL}: ${CN}NO PROGRESS DATA${CL}"
				fi
			fi
		done

		echo "=========================================================================================================="
		local total_for_display="$total_test_methods"
		if [ "$total_test_methods_from_slots" -gt 0 ]; then
			total_for_display="$total_test_methods_from_slots"
		fi
		echo -e "Overall: ${BL}$total_completed${CL}/${MG}$total_for_display${CL} test methods completed, ${GN}$total_running${CL} slots running, ${RD}$total_failed${CL} failed"

		# Show unique failed tests in real-time
		if [ ${#unique_failed_tests[@]} -gt 0 ]; then
			echo -e "\n${RD}Failed tests so far:${CL}"
			for failed_test in "${unique_failed_tests[@]}"; do
				echo -e "  ${RD}•${CL} $failed_test"
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
	local unique_failed_tests=()
	local failed_tests_with_slots=()

	echo -e "\n${YL}Test Results Summary:${CL}"
	echo "=========================================================================================================="
	for ((slot = 1; slot <= parallel; slot++)); do
		if [ -e "test.log/slot_$slot/failed_tests" ]; then
			while IFS= read -r failed_test; do
				if [[ "$failed_test" =~ ^FAILED:(.+)$ ]]; then
					test_name="${BASH_REMATCH[1]}"

					# Check if this test is already in our unique list
					local already_listed=false
					for existing_test in "${unique_failed_tests[@]}"; do
						if [[ "$existing_test" == "$test_name" ]]; then
							already_listed=true
							break
						fi
					done

					if [ "$already_listed" == "false" ]; then
						unique_failed_tests+=("$test_name")
						failed_tests_with_slots+=("$test_name (slot $slot)")
					fi
				fi
			done <"test.log/slot_$slot/failed_tests"
		fi
	done

	echo -e "${BL}Preserving completed test logs...${CL}"
	aggregate_slot_logs

	local total_failed=${#unique_failed_tests[@]}

	if [ $total_failed -eq 0 ]; then
		echo -e "${GN}No test failures detected before abort${CL}"
	else
		echo -e "${RD}✗ $total_failed test(s) failed before abort:${CL}"
		echo
		for failed_test in "${failed_tests_with_slots[@]}"; do
			echo -e "  ${RD}•${CL} $failed_test"
		done
		echo
		echo -e "${YL}Failed test logs can be found in:${CL}"
		for failed_test in "${failed_tests_with_slots[@]}"; do
			test_name=$(echo "$failed_test" | cut -d' ' -f1)
			echo -e "  ${BL}test.log/${test_name}.log${CL}"
		done
	fi
	echo "=========================================================================================================="

	# Remove slot directories after copying logs
	echo -e "${BL}Cleaning up slot directories...${CL}"
	rm -f "$TEST_TMP_DIR"/test_chunk_*.txt

	echo -e "${RD}Parallel execution aborted by user${CL}"
	exit 130
}

function run_parallel_tests() {
	echo -e "${GN}Starting parallel execution with $parallel slots${CL}"

	# Set up signal handling for clean shutdown
	trap cleanup_parallel_execution SIGINT SIGTERM

	# Calculate total test methods (use the same calculation as serial mode for consistency)
	total_test_classes=$(wc -l <$classList | tr -d ' ')
	# Use the same calculation as the global testMethods variable from line 751
	total_test_methods=$testMethods

	# Split test list into chunks (still use class count for chunk distribution)
	tests_per_slot=$((total_test_classes / parallel))
	remainder=$((total_test_classes % parallel))

	line_start=1
	for ((slot = 1; slot <= parallel; slot++)); do
		chunk_size=$tests_per_slot
		if [ $slot -le $remainder ]; then
			((chunk_size++))
		fi

		chunk_file="$TEST_TMP_DIR/test_chunk_$slot.txt"
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

	local unique_failed_tests=()
	local failed_tests_with_slots=()

	aggregate_slot_logs

	# Third pass: collect failed tests with deduplication
	for ((slot = 1; slot <= parallel; slot++)); do
		if [ -d "test.log/slot_$slot" ]; then

			# Collect failed tests with deduplication
			if [ -e "test.log/slot_$slot/failed_tests" ]; then
				while IFS= read -r failed_test; do
					if [[ "$failed_test" =~ ^FAILED:(.+)$ ]]; then
						test_name="${BASH_REMATCH[1]}"

						# Check if this test is already in our unique list
						local already_listed=false
						for existing_test in "${unique_failed_tests[@]}"; do
							if [[ "$existing_test" == "$test_name" ]]; then
								already_listed=true
								break
							fi
						done

						if [ "$already_listed" == "false" ]; then
							unique_failed_tests+=("$test_name")
							failed_tests_with_slots+=("$test_name (slot $slot)")
						fi
					fi
				done <"test.log/slot_$slot/failed_tests"
			fi
		fi
	done

	local total_failed=${#unique_failed_tests[@]}

	# Show results summary
	echo "=========================================================================================================="
	if [ $total_failed -eq 0 ]; then
		echo -e "${GN}✓ All tests passed successfully!${CL}"
	else
		echo -e "${RD}✗ $total_failed test(s) failed:${CL}"
		echo
		for failed_test in "${failed_tests_with_slots[@]}"; do
			echo -e "  ${RD}•${CL} $failed_test"
		done
		echo
		echo -e "${YL}Failed test logs can be found in:${CL}"
		for failed_test in "${failed_tests_with_slots[@]}"; do
			test_name=$(echo "$failed_test" | cut -d' ' -f1)
			echo -e "  ${BL}test.log/${test_name}.log${CL}"
		done
	fi
	echo "=========================================================================================================="

	# Clean up slot directories after all processing is complete
	echo -e "${BL}Cleaning up slot directories...${CL}"
	for ((slot = 1; slot <= parallel; slot++)); do
		if [ -d "test.log/slot_$slot" ]; then
			rm -rf "test.log/slot_$slot"
		fi
	done

	# Cleanup temporary files
	rm -f "$TEST_TMP_DIR"/test_chunk_*.txt
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

		# Parse ClassName#methodName format if present (for --rerunfailed)
		test_class="$t"
		test_method="$m"
		if [[ "$t" == *"#"* ]]; then
			test_class="${t%#*}"
			test_method="${t#*#}"
		fi

		while true; do
			tm=$(date +%s)
			if [ "$test_method" == "." ]; then
				echo "Running Tests in $test_class" >"test.log/$test_class.log"
				# Same rationale as in parallel slots: call surefire directly to keep runs isolated from build output churn.
				mvn -Dsurefire.useFile=false $TEST_MVN_PROPS surefire:test -Dtest="$test_class" >>"test.log/$test_class".log 2>&1 &
				echo $! >$testPid
			else
				echo "Running $test_method in $test_class" >"test.log/$test_class.log"
				mvn -Dsurefire.useFile=false $TEST_MVN_PROPS surefire:test -Dtest="$test_class#$test_method" >>"test.log/$test_class.log" 2>&1 &
				echo $! >$testPid
			fi
			while true; do
				testsRun=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Total tests run" | cut -d: -f2 | tr -d ' ' | head -1)
				unsuc=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Total unsuccessful" | cut -d: -f2 | tr -d ' ' | head -1)
				fail=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Tests failed" | cut -d: -f2 | tr -d ' ' | head -1)
				err=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Tests with errors" | cut -d: -f2 | tr -d ' ' | head -1)

				# Ensure numeric values and defaults
				[[ ! "$testsRun" =~ ^[0-9]+$ ]] && testsRun=0
				[[ ! "$unsuc" =~ ^[0-9]+$ ]] && unsuc=0
				[[ ! "$fail" =~ ^[0-9]+$ ]] && fail=0
				[[ ! "$err" =~ ^[0-9]+$ ]] && err=0
				((d = $(date +%s) - start))
				# echo "Checking $fn"
				# Strip #method* suffix before converting to file path (for --rerunfailed)
				fn=$(echo "$test_class" | tr "." "/")
				lmeth=$(safe_grep_count "@Test|@ParameterizedTest" "$fn" "$filesList")
				lmeth3=$(safe_grep_count '@MethodSource\("getMorphiumInstances"\)' "$fn" "$filesList")
				lmeth2=$(safe_grep_count '@MethodSource\("getMorphiumInstancesNo.*"\)' "$fn" "$filesList")
				lmeth1=$(safe_grep_count '@MethodSource\("getMorphiumInstances.*Only"\)' "$fn" "$filesList")
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
				# Display test class (and method if running specific method via --rerunfailed)
				if [ "$test_method" != "." ] && [ "$test_method" != "$m" ]; then
					echo -e "Running ${YL}$test_class${CL}#${CN}$test_method${CL}  - #${MG}$tst${CL}/${BL}$cnt$CL"
				else
					echo -e "Running tests in ${YL}$test_class${CL}  - #${MG}$tst${CL}/${BL}$cnt$CL"
				fi
				echo -e "Total number methods to run in matching classes ${CN}$testMethods$CL"
				echo -e "Number of test methods in ${YL}$test_class${CL}: ${GN}$lmeth$CL"
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
						tail -n+5 "$TEST_TMP_DIR/failed.txt"
					fi
					echo -e "---------- ${CN}LOG:$CL--------------------------------------------------------------------------------------"
					tail -n $logLength test.log/"$test_class".log
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
						tail -n+5 "$TEST_TMP_DIR/failed.txt"
					fi
					echo -e "---------- ${CN}LOG:$CL--------------------------------------------------------------------------------------"
					tail -n $logLength test.log/"$test_class".log
					echo "---------------------------------------------------------------------------------------------------------"
				fi
				# egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--' | pr -l1 -3 -t -w 280 || echo "none"

				# egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'  || echo "none"
				# egrep "] Running |Tests run:" test.log/* | grep -B1 FAILURE | cut -f2 -d']' || echo "none"

				# Check if the test process is still running
				if [ -e "$testPid" ]; then
					pid=$(<$testPid)
					if ! kill -0 $pid 2>/dev/null; then
						# Process finished, wait for it to clean up
						wait $pid 2>/dev/null
						break
					fi
				else
					break
				fi
				if [ $dur -gt 600 ]; then
					echo -e "${RD}Error:${CL} Test class runs longer than 10 minutes - killing it!"
					echo "TERMINATED DUE TO TIMEOUT" >>test.log/$t.log
					kill $(<$testPid)
				fi
				# Use shorter sleep intervals so we can respond faster to process completion
				for ((i = 0; i < refresh; i++)); do
					sleep 1
					# Check again if process finished during sleep
					if [ -e "$testPid" ]; then
						pid=$(<$testPid)
						if ! kill -0 $pid 2>/dev/null; then
							break 3 # Break out of: for loop + inner while + outer while
						fi
					fi
				done
			done
			break
		done
	done
	##### Mainloop end
	get_test_stats >"$TEST_TMP_DIR/failed.txt" 2>/dev/null

	testsRun=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Total tests run" | cut -d: -f2 | tr -d ' ' | head -1)
	unsuc=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Total unsuccessful" | cut -d: -f2 | tr -d ' ' | head -1)
	fail=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Tests failed" | cut -d: -f2 | tr -d ' ' | head -1)
	err=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Tests with errors" | cut -d: -f2 | tr -d ' ' | head -1)

	# Ensure numeric values and defaults
	[[ ! "$testsRun" =~ ^[0-9]+$ ]] && testsRun=0
	[[ ! "$unsuc" =~ ^[0-9]+$ ]] && unsuc=0
	[[ ! "$fail" =~ ^[0-9]+$ ]] && fail=0
	[[ ! "$err" =~ ^[0-9]+$ ]] && err=0
	num=$numRetries
	if [ "$unsuc" -gt 0 ] && [ "$num" -gt 0 ] && [ "q$testname" = "q" ]; then
		while [ "$num" -gt 0 ]; do
			echo -e "${YL}Some tests failed$CL - retrying...."

			# Use built-in rerun failed functionality instead of external script
			failed=$(get_test_stats --noreason --nosum 2>/dev/null || echo "")
			if [ -n "$failed" ]; then
				# Convert failed tests to class list format and rerun them
				failed_classes=""
				for f in $failed; do
					cls=${f%#*}
					if [ -z "$failed_classes" ]; then
						failed_classes="$cls"
					else
						failed_classes="$failed_classes\n$cls"
					fi
				done

				# Create temporary class list with just the failed tests
				echo -e "$failed_classes" | sort -u >"$TEST_TMP_DIR/$(basename "$classList").retry"

				# Rerun failed tests
				for retry_t in $(<"$TEST_TMP_DIR/$(basename "$classList").retry"); do
					if grep "$retry_t" $disabledList >/dev/null; then
						continue
					fi
					echo "Retrying $retry_t"
					if [ "$m" == "." ]; then
						mvn -Dsurefire.useFile=false $TEST_MVN_PROPS surefire:test -Dtest="$retry_t" >"test.log/$retry_t.log" 2>&1
					else
						mvn -Dsurefire.useFile=false $TEST_MVN_PROPS surefire:test -Dtest="$retry_t#$m" >"test.log/$retry_t.log" 2>&1
					fi
				done

				rm -f "$TEST_TMP_DIR/$(basename "$classList").retry"
			fi

			((num = num - 1))
			((totalRetries = totalRetries + 1))
			retried="$retried\n$t"
			get_test_stats >failed.txt 2>/dev/null
			unsuc=$(cat failed.txt | grep "Total unsuccessful" | cut -d: -f2 | tr -d ' ' | head -1)

			# Ensure numeric values and defaults
			[[ ! "$unsuc" =~ ^[0-9]+$ ]] && unsuc=0
			if [ "$unsuc" -eq 0 ]; then
				break
			fi
		done

	fi
	get_test_stats >"$TEST_TMP_DIR/failed.txt" 2>/dev/null

	testsRun=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Total tests run" | cut -d: -f2 | tr -d ' ' | head -1)
	unsuc=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Total unsuccessful" | cut -d: -f2 | tr -d ' ' | head -1)
	fail=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Tests failed" | cut -d: -f2 | tr -d ' ' | head -1)
	err=$(cat "$TEST_TMP_DIR/failed.txt" | grep "Tests with errors" | cut -d: -f2 | tr -d ' ' | head -1)

	# Ensure numeric values and defaults
	[[ ! "$testsRun" =~ ^[0-9]+$ ]] && testsRun=0
	[[ ! "$unsuc" =~ ^[0-9]+$ ]] && unsuc=0
	[[ ! "$fail" =~ ^[0-9]+$ ]] && fail=0
	[[ ! "$err" =~ ^[0-9]+$ ]] && err=0
	rm -f $runLock
	sleep 5
	t=""
	# kill $(<$failPid) >/dev/null 2>&1
	rm -f $failPid >/dev/null 2>&1
	echo -e "${GN}Finished!${CL} - total run: $testsRun - total unsuccessful: $unsuc - Retries: $totalRetries"

	if [ -z "$unsuc" ] || [ "$unsuc" -eq 0 ]; then
		echo -e "${GN}no errors recorded$CL"
		rm -f "$TEST_TMP_DIR/failed.txt"
		quitting
	else
		# Copy failed.txt to test.log/ so it persists after cleanup
		cp "$TEST_TMP_DIR/failed.txt" "test.log/failed.txt" 2>/dev/null
		echo -e "${RD}There were errors$CL: fails $fail + errors $err = $unsuc - List of failed tests in test.log/failed.txt"
		quitting
		exit 1
	fi
fi # End of sequential execution (else branch)
