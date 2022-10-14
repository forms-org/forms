#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a ROWS_OPTIONS=(2000000 4000000 6000000 8000000 10000000)
declare FILENAME='weather_10M.csv'
declare ROWS=10000000
declare CORES=16
declare FORMULA_STR="=MAX(E1, F\$1:F1)"

for RUN in "${RUN_OPTIONS[@]}"
do
  for ROWS in "${ROWS_OPTIONS[@]}"
  do
    FILE_DIR="results/TEST-REWRITE-2/NO-REWRITE/${ROWS}ROWS/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"

    FILE_DIR="results/TEST-REWRITE-2/REWRITE/${ROWS}ROWS/RUN${RUN}"
    mkdir -p $FILE_DIR
    rm -f $FILE_DIR/*
    timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --enable_logical_rewriting True \
              --enable_physical_opt True \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
    echo "finished $FILE_DIR"
  done
done

