#!/bin/bash
run=0
for i in $(grep -a "Tests run: .*in " test.log/*.log | cut -f3 -d: | cut -f1 -d,); do
	((run = run + i))
done
fail=0
for i in $(grep -a "Tests run: .*in " test.log/*.log | cut -f4 -d: | cut -f1 -d,); do
	((fail = fail + i))
done
err=0
for i in $(grep -a "Tests run: .*in " test.log/*.log | cut -f5 -d: | cut -f1 -d,); do
	((err = err + i))
done
echo "Toal tests run   : $run"
(( sum=fail+err ))
echo "Total failed     : $sum"
echo "Tests failed     : $fail"
echo "Tests with errors: $err"
if [ "$fail" -eq 0 ] && [ "$err" -eq 0 ]; then
	exit
else
echo ""
 	for cls in $(grep -E "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' | grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--'); do
		# echo "Getting failures in $cls"
		if [ ! -e ./target/surefire-reports/TEST-$cls.xml ]; then
			echo "$cls"
			continue
		fi
		failures=$(xq . ./target/surefire-reports/TEST-"$cls".xml | gron | grep 'testcase\[[0-9]*\].failure\["@type"\]' | cut -f 2 -d '[' | cut -f1 -d ']')
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
    errors=$(xq . ./target/surefire-reports/TEST-"$cls".xml | gron | grep 'testcase\[[1-9]*\].error\["@type"\]'| cut -f2 -d '[' | cut -f1 -d ']')
    for id in $(echo $errors); do
			m="$(xq . ./target/surefire-reports/TEST-$cls.xml | gron | grep "testcase\\[$id\\]\\[\"@name\"\\]" | cut -f2 -d= | tr -d '"; ')"
			if [ "q$1" == "q--noreason" ]; then
				echo "$cls#$m"
			else
				err="$(xq . ./target/surefire-reports/TEST-$cls.xml | gron | grep "testcase\\[$id\\].error\\[\"@type\"\\]" | cut -f2 -d= | tr -d '"; ')"
				echo "$cls#$m - $err"
			fi
    done
	done
fi
