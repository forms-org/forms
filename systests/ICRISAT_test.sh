#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <enable_optimization>"
    exit 1
fi


$TEST_HOME/run.sh ICRISAT ICRISAT.csv ICRISAT_formula.csv obvdate $1
# $TEST_HOME/run.sh ICRISAT ICRISAT.csv test_formula.csv obvdate $1
