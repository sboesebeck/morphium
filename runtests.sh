#!/bin/bash

function quitting() {
	kill -9 $(<test.pid) >/dev/null 2>&1
	#  rm -f test.pid
	exit 1
}

trap quitting EXIT
trap quitting SIGINT
trap quitting SIGHUP

nodel=0
if [ "q$1" == "q--nodel" ]; then
	nodel=1
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
cnt=$(wc -l <files.txt | tr -d ' ')
if [ "$cnt" -eq 0 ]; then
	echo "no matching class found for $p"
	exit 1
fi
testMethods=$(egrep "@Test|@ParameterizedTest" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
if [ "$nodel" -eq 0 ]; then
	rm -rf test.log
	mkdir test.log
fi
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
		mvn -Dsurefire.useFile=false test -Dtest="$t#$m" >"test.log/$t.log" 2>&1&
		echo $! >test.pid
	fi
	while true; do
		clear

		echo "Running test in $t  - #$tst/$cnt"
		echo "Total methods to in matching classes $testMethods"
		if [ "$m" != "." ]; then
			echo " Tests matchin: $m"
		fi
		echo "Tests    run: $totalTestsRun"
		echo "Tests failed: $totalTestsFailed"
		echo "Tests errors: $totalTestsError"
		((dur = $(date +%s) - tm))
		echo "Duration: $dur"
		run=0
		for i in $(grep -a "Tests run: .*in $t" test.log/$t.log | cut -f2 -d: | cut -f1 -d,); do
			((run = run + i))
		done
		fail=0
		for i in $(grep -a "Tests run: .*in $t" test.log/$t.log | cut -f3 -d: | cut -f1 -d,); do
			((fail = fail + i))
		done
		err=0
		for i in $(grep -a "Tests run: .*in $t" test.log/$t.log | cut -f4 -d: | cut -f1 -d,); do
			((err = err + i))
		done
		echo "---------- LOG: "
		tail -n 10 test.log/"$t".log
		echo "----------"
		echo "Failed tests"
		./getFailedTests.sh | pr -t -2 -w280
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
		sleep 2
	done
	((totalTestsRun = totalTestsRun + run))
	((totalTestsError = totalTestsError + err))
	((totalTestsFailed = totalTestsFailed + fail))

done
