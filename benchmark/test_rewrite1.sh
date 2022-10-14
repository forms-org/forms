#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare FILENAME='weather_10M.csv'
declare FORMULA=formula_for_rewrite_plus.csv
declare ROWS=10000000
declare CORES=16

n=0
sed 1d $FORMULA | while IFS="," read -r formula_str
do
  echo $formula_str
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    FILE_DIR="results/TEST-REWRITE-1/NO-REWRITE/${n}PLUS/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$formula_str" \
              --cores "$CORES" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"

    FILE_DIR="results/TEST-REWRITE-1/REWRITE/${n}PLUS/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$formula_str" \
              --cores "$CORES" \
              --enable_logical_rewriting True \
              --enable_physical_opt True \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"
  done
done

