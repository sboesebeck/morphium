#!/bin/bash
function quit {
	echo "Shutting down"
	kill -9 $(ps aux | grep -v grep | grep surefire | cut -c15-24)
	end="Aborted during testrun, but ran $run Tests, $fail tests failed, $err tests had errors"
	curl -X POST -H "Content-type: application/json" --data "{'text':'Morphium $version integration test just ran: $end'}" https://hooks.slack.com/services/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
	exit 1
}

trap 'quit' ABRT QUIT INT

start=$(date +%s)
cd $(dirname $0)
version=$(grep '<version>' pom.xml | head -n1 | tr -d ' a-z<>/')

mvn -Dsurefire.skipAfterFailureCount=2 -Dsurefire.rerunFailingTestsCount=1 test >test.log 2>&1 &

end=""
while true; do 
	clear
	echo "Running tests for version $version"
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
run=$(grep -a 'Tests run: ' test.log |cut -f2 -d: | cut -f1 -d, | tail -n 1)
fail=$(grep -a 'Tests run: ' test.log |cut -f3 -d: | cut -f1 -d,  | tail -n 1)
err=$(grep -a 'Tests run: ' test.log |cut -f4 -d: | cut -f1 -d,  | tail -n 1)

dur=$(date +%s)
let dur=dur-start
let h=dur/3600
let m='(dur-h*3600)/60'
let s='(dur-h*3600-m*60)'
 
let h=dur/3600; let m='(dur-h*3600)/60';let s='(dur-h*3600-m*60)'; 
duration=$(printf "Duration: %02d:%02d:%02d" $h $m $s)
end="$duration - Run $run Tests, $fail tests failed, $err tests had errors"
curl -X POST -H "Content-type: application/json" --data "{'text':'Morphium $version integration test just ran: $end'}" https://hooks.slack.com/services/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
