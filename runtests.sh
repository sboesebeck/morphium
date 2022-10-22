#!/bin/bash
p=$1
if [ "q$p" == "q" ]; then
	echo "No pattern given, running all tests"
	p="."
else
	echo "Checking for tests whose classes match $p"
fi

m=$2
if [ "q$2" == "q" ]; then
	echo "No pattern for methods given"
	m="."
else
	echo "Checking for test-methods matching $m"
fi

rg -l "@Test" | grep ".java" >files.lst
rg -l "@ParameterizedTest" | grep ".java" >>files.lst

sort -u files.lst | grep "$p" | sed -e 's!/!.!g' | sed -e 's/src.test.java//g' | sed -e 's/.java$//' | sed -e 's/^\.//'>files.txt
cnt=$(wc -l < files.txt|tr -d ' ')
echo "Running test classes $cnt"
rm -rf test.log
mkdir test.log
tst=0
totalTestsRun=0
totalTestsFailed=0
totalTestsError=0

for t in $(<files.txt); do
  (( tst = tst +1 ))
	if [ "$m" == "." ]; then
		echo "Running all tests in Class $t"
		mvn test -Dtest="$t" >test.log/"$t".log &
	else
		echo "Running test in class $t matching $m"
		mvn test -Dtest="$t#$m" >"test.log/$t.log" &
	fi

  while true; do
    clear
    echo "Running test in $t  - #$tst/$cnt"
    echo "Tests    run: $totalTestsRun"
    echo "Tests failed: $totalTestsFailed"
    echo "Tests errors: $totalTestsError"
    run=0
    for i in $(grep -a "Tests run: .*in $t" test.log/$t.log |cut -f2 -d: | cut -f1 -d,); do   
      (( run=run+i ))
    done
    fail=0
    for i in $(grep -a "Tests run: .*in $t" test.log/$t.log |cut -f3 -d: | cut -f1 -d,); do 
      (( fail=fail+i ))
    done
    err=0
    for i in $(grep -a "Tests run: .*in $t" test.log/$t.log |cut -f4 -d: | cut -f1 -d,); do 
      (( err=err+i ))
    done
    echo "---------- LOG: "
    tail -n 10 test.log/"$t".log
    echo "----------"
    echo "Failed tests"
    egrep "Running |Tests run:" test.log/* | grep -B1 FAILURE || echo "none"
    jobs >/dev/null
    j=$(jobs | wc -l)
    if [ "$j" -lt 1 ]; then
      break;
    fi
    sleep 2
  done
  (( totalTestsRun = totalTestsRun + run ))
  (( totalTestsError = totalTestsError + err ))
  (( totalTestsFailed = totalTestsFailed + fail ))
  
done
