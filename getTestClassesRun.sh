#!/bin/bash

egrep "] Running " test.log/* | cut -f2 -d']' | grep -v "Tests run: " | sed -e 's/Running //' | grep -v -- "--"
