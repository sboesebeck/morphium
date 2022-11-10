#!/bin/bash

function quitting() {
	echo "Shutting down..."
	kill -9 $(<test.pid) >/dev/null 2>&1
	#  rm -f test.pid
	./getFAiledTests.sh >failed.txt
	echo "List of failed tests in failed.txt"
	exit 1
}

trap quitting EXIT
trap quitting SIGINT
trap quitting SIGHUP

nodel=0
skip=0
if [ "q$1" == "q--nodel" ]; then
	nodel=1
	shift
fi

if [ "q$1" == "q--skip" ]; then
	skip=1
	shift
fi
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
if [ "$nodel" -eq 0 ]; then
	echo "Cleaning up..."
	mvn clean >/dev/null
fi

rg -l "@Test" | grep ".java" >files.lst
rg -l "@ParameterizedTest" | grep ".java" >>files.lst

sort -u files.lst | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//' >files.txt
if [ "$skip" -ne 0 ]; then
	echo "Skipping tests already run"
	for i in $(ls -1 test.log); do
		i=$(basename $i)
    i=${i%%.log}
		echo "not rerunning $i"
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
testMethods=$(grep -E "@Test|@ParameterizedTest" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
	rm -rf test.log
	mkdir test.log
fi
echo "Compiling..."
mvn compile >/dev/null || { echo "Compilation failed!"; exit 1; }

tst=0
totalTestsRun=0
totalTestsFailed=0
totalTestsError=0
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
		clear

		echo "Running tests in $t  - #$tst/$cnt"
		echo "Total number methods to run in matching classes $testMethods"
    echo "Number of test methods in $t: $(grep -E "@Test|@ParameterizedTest" $(grep "$t" files.lst) | cut -f2 -d: | grep -vc '^ *//')"
		if [ "$m" != "." ]; then
			echo " Tests matchin: $m"
		fi
		./getFailedTests.sh | pr -t -2 -w280
		((dur = $(date +%s) - tm))
		echo "Duration: $dur"
		echo "---------- LOG: "
		tail -n 15 test.log/"$t".log
		echo "----------"
		# egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--' | pr -l1 -3 -t -w 280 || echo "none"

		# egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'  || echo "none"
		# egrep "] Running |Tests run:" test.log/* | grep -B1 FAILURE | cut -f2 -d']' || echo "none"
		jobs >/dev/null
		j=$(jobs | wc -l)
		if [ "$j" -lt 1 ]; then
			break
		fi
		if [ $dur -gt 600 ]; then
			echo "Test class runs longer than 5 minutes - killing it!"
			echo "TERMINATED DUE TO TIMEOUT" >>test.log/$t.log
			((err = err + 1))
			kill %1
		fi
		sleep 5
	done
	((totalTestsRun = totalTestsRun + run))
	((totalTestsError = totalTestsError + err))
	((totalTestsFailed = totalTestsFailed + fail))

done
./getFAiledTests.sh >failed.txt
echo "Total tests run       : $totalTestsRun"
echo "TotalTests with errors: $totalTestsError"
echo "TotalTests failed     : $totalTestsFailed"
echo "Finished! List of failed tests in ./failed.txt"
