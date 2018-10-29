#!/bin/bash
function quit {
	echo "Shutting down"
	kill -9 $(ps aux | grep -v grep | grep surefire | cut -c15-24)
	exit 1
}

trap 'quit' ABRT QUIT INT

cd $(dirname $0)

mvn -Dsurefire.skipAfterFailureCount=1 -Dsurefire.rerunFailingTestsCount=1 test >test.log 2>&1 &

end=""
while true; do 
	clear
	date
	grep "Running " test.log | tail -n 1
	a=$(grep "Number: " test.log | tail -n 1); echo "Test number: ${a##*:}"
	run=0
	for i in $(grep -a 'Tests run: ' test.log |cut -f2 -d: | cut -f1 -d,); do 	
		let run=run+i
	done
	echo "Tests run: $run"
	fail=0
	for i in $(grep -a 'Tests run: ' test.log |cut -f3 -d: | cut -f1 -d,); do 
		let fail=fail+i
	done
	echo "Fails: $fail"
	err=0
	for i in $(grep -a 'Tests run: ' test.log |cut -f4 -d: | cut -f1 -d,); do 
		let err=err+i
	done
	echo "Errors: $err"
	echo 
	echo "-------------   Current Failed tests:"
	if [ $fail -gt 0 ] || [ $err -gt 0 ]; then
		egrep "Running |Tests run:" test.log | grep -B1 FAILURE
		echo
	fi
	echo
	echo "-------------   Log output:"
	tail -n 10 test.log

	jobs > /dev/null
	l=$(ls -l test.log)
	sleep 15
	if [ $(jobs | wc -l) -eq 0 ]; then
		echo "Bg job finished... exiting"	
		break
	fi
done
let run=run/2
let fail=fail/2
let err=err/2

end="Run $run Tests, $fail tests failed, $err tests had errors"
curl -X POST -H "Content-type: application/json" --data "{'text':'Morphium integration test just ran: $end'}" https://hooks.slack.com/services/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
