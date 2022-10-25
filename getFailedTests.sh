#!/bin/bash

for cls in $(egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' | grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'); do
	# echo "Getting failures in $cls"
	if [ ! -e ./target/surefire-reports/TEST-$cls.xml ]; then
		echo "$cls"
		continue
	fi
	failures=$(xq . ./target/surefire-reports/TEST-"$cls".xml | gron | grep 'testcase\[[1-9]*\].failure\["@type"\]' | cut -f 2 -d '[' | cut -f1 -d ']')
	for id in $(echo $failures); do
		# echo "ID: '$id'"
		# xq . ./target/surefire-reports/TEST-$cls.xml | gron | grep "testcase\\[$id\\]\\[\"@name\"\\]" | cut -f2 -d= | tr -d '"; '
		m="$(xq . ./target/surefire-reports/TEST-$cls.xml | gron | grep "testcase\\[$id\\]\\[\"@name\"\\]" | cut -f2 -d= | tr -d '"; ')"
		if [ "q$1" == "q--noreason" ]; then
			echo "$cls#$m"
		else
			err="$(xq . ./target/surefire-reports/TEST-$cls.xml | gron | grep "testcase\\[$id\\].failure\\[\"@type\"\\]" | cut -f2 -d= | tr -d '"; ')"
			echo "$cls#$m - $err"
		fi
	done
done
