#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a ROWS_OPTIONS=(200000 400000 600000 800000 1000000)
declare -a EXECUTORS=("df_pandas_executor" "df_formulas_executor")
declare FILENAME='weather.csv'
declare FORMULA=formula_in_all_types_rows.csv
declare CORES=32

n=0
sed 1d $FORMULA | while IFS="," read -r formula_str
do
  echo $formula_str
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    for ROWS in "${ROWS_OPTIONS[@]}"
    do
      for EXECUTOR in "${EXECUTORS[@]}"
      do
        FILE_DIR="results/TEST-ROWS/${n}/${EXECUTOR}/${ROWS}ROWS/RUN${RUN}"
        mkdir -p $FILE_DIR
        rm -f $FILE_DIR/*
        timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$formula_str" \
              --cores "$CORES" \
              --function_executor "$EXECUTOR" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
        echo "finished $FILE_DIR"
      done
    done
  done
done

