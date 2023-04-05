#!/bin/bash

function quitting() {
	echo "Shutting down... current test $t"

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
if [ "q$1" == "q--nodel" ]; then
	nodel=1
	shift
fi

if [ "q$1" == "q--skip" ]; then
	skip=1
	shift
fi

if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
    count=$(ls -1 test.log/* | wc -l)
    if [ "$count" -gt 0 ]; then
        echo "There are restults from old tests there - continue tests (c), erase old logs and restart all (r) or abort (CTRL-C / a)?"
        read q
        case $q in
            c)
                echo "Will continue where we left of..."
                skip=1;
                ;;
            r)
                echo "Erase all and restart..."
                skip=0;
                ;;
            a)
                echo "Aborting..."
                exit
                ;;
            *)
                echo "Unknown answer - aborting"
                exit 1
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
testMethods=$(grep -E "@Test" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods3=$(grep -E '@MethodSource\("getMorphiumInstances"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods2=$(grep -E '@MethodSource\("getMorphiumInstancesNo.*"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
testMethods1=$(grep -E '@MethodSource\("getMorphiumInstances.*Only"\)' $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
# testMethodsP=$(grep -E "@ParameterizedTest" $(grep "$p" files.lst) | cut -f2 -d: | grep -vc '^ *//')
((testMethods = testMethods + 3 * testMethods3 + testMethods2*2 + testMethods1))
if [ "$nodel" -eq 0 ] && [ "$skip" -eq 0 ]; then
	rm -rf test.log
	mkdir test.log
fi
if [ "$nodel" -eq 0 ]; then
	echo "Cleaning up..."
	mvn clean >/dev/null
fi
echo "Compiling..."
mvn compile test-compile >/dev/null || {
	echo "Compilation failed!"
	exit 1
}


tst=0
echo "Starting..." >failed.txt
# running getfailedTests in background
{
	while true; do
		date >failed.tmp
		./getFailedTests.sh >>failed.tmp
		mv failed.tmp failed.txt
		sleep 8
	done

} &

echo $! >fail.pid

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
		cat failed.txt | pr -t -2 -w280
		((dur = $(date +%s) - tm))
		echo "Duration: $dur"
		echo "---------- LOG: "
		tail -n 15 test.log/"$t".log
		echo "----------"
		# egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--' | pr -l1 -3 -t -w 280 || echo "none"

		# egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' |grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'  || echo "none"
		# egrep "] Running |Tests run:" test.log/* | grep -B1 FAILURE | cut -f2 -d']' || echo "none"
		jobs >/dev/null
		j=$(jobs | grep -E '\[[0-9]+\]' | wc -l)
		if [ "$j" -lt 2 ]; then
			break
		fi
		if [ $dur -gt 900 ]; then
			echo "Test class runs longer than 15 minutes - killing it!"
			echo "TERMINATED DUE TO TIMEOUT" >>test.log/$t.log
			((err = err + 1))
			kill %1
		fi
		sleep 5
	done

done
./getFailedTests.sh >failed.txt
cat failed.txt
echo "Finished! List of failed tests in ./failed.txt"

kill $(<fail.pid) >/dev/null 2>&1
rm -f fail.pid >/dev/null 2>&1
