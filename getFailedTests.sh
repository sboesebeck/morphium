#!/bin/bash

echo $(egrep "] Running |Tests run: " test.log/* | grep -B1 FAILURE | cut -f2 -d']' | grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- '--')

