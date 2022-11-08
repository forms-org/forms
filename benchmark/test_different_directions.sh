#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare FILENAME='weather_10M.csv'
declare FORMULA=formula_for_different_directions.csv
declare ROWS=10000000
declare CORES=16

n=0
sed 1d $FORMULA | while IFS="," read -r FORMULA_STR
do
  echo $FORMULA_STR
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    FILE_DIR="results/TEST-DIRECTION/${n}/ROW-FIRST/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --along_row_first True \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"

    FILE_DIR="results/TEST-DIRECTION/${n}/COL-FIRST/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"
  done
done

