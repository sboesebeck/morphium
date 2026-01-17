#!/bin/bash

# This script contains the get_test_stats function for parsing test logs
# and generating statistics.
# It is sourced by the main runtests.sh script.
#CHARS=("${RD}>${CL}...." ".${RD}>${CL}..." "..${RD}>${CL}.." "...${RD}>${CL}." "....${RD}>$CL" "....${RD}<$CL" "...${RD}<${CL}." "..${RD}<${CL}.." ".${RD}<${CL}..." "${RD}<${CL}....")

#CHARS=("${RD}░ ▒ ▓ █)

CHARS=("${RD}█${CL}......." "${RD}▓█${CL}......" "${RD}▒▓█${CL}....." "${RD}░▒▓█${CL}...." ".${RD}░▒▓█${CL}..." "..${RD}░▒▓█${CL}.." "...${RD}░▒▓█${CL}." "....${RD}░▒▓█${CL}" ".....${RD}░▒▓${CL}" "......${RD}░▒${CL}" ".......${RD}░${CL}" "........"
	".......${RD}█${CL}" "......${RD}█▓${CL}" ".....${RD}█▓▒${CL}" "....${RD}█▓▒░${CL}" "...${RD}█▓▒░${CL}." "..${RD}█▓▒░${CL}.." ".${RD}█▓▒░${CL}..." "${RD}█▓▒░${CL}...." "${RD}▓▒░${CL}....." "${RD}▒░${CL}......" "${RD}░${CL}......." "........")

function spinner() {
	local c
	while true; do
		for c in "${CHARS[@]}"; do
			echo -ne "$c\\r"
			sleep 0.06
		done
	done

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

	if [ $noreason -eq 0 ] && [ $nosum -eq 0 ] && [ -t 1 ]; then
		# Only show spinner if stdout is a terminal
		spinner 2>/dev/null &
		spinner_pid=$!
	fi
	local total_run=0
	local total_fail=0
	local total_err=0
	local total_build_failures=0
	local failed_tests=""

	# Handle both sequential and parallel log structures
	local log_pattern=""
	if [ -d "test.log" ]; then
		# Sequential execution - standard pattern
		log_pattern="test.log/*.log test.log/slot_*/*.log"

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
	if [ -n "$spinner_pid" ]; then
		kill $spinner_pid >/dev/null 2>&1
		wait $spinner_pid 2>/dev/null
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
