#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a ROWS_OPTIONS=(2000000 4000000 6000000 8000000 10000000)
declare FILENAME='weather_10M.csv'
declare ROWS=10000000
declare CORES=16

for ROWS in "${ROWS_OPTIONS[@]}"
  do
  FORMULA_STR="MEDIAN(E1:F\$${ROWS})"
	for RUN in "${RUN_OPTIONS[@]}"
  do
    FILE_DIR="results/TEST-COSTMODEL/SIMPLE/${ROWS}ROWS/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"

    FILE_DIR="results/TEST-COSTMODEL/LOAD-BALANCE/${ROWS}ROWS/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --cost_model "loadbalance" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"
  done
done

