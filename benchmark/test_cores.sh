#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a CORES_OPTIONS=(1 2 4 8 16 32)
declare -a EXECUTORS=("df_pandas_executor" "df_formulas_executor")
declare FILENAME='weather.csv'
declare FORMULA=formula_in_all_types.csv
declare ROWS=1000000

n=0
sed 1d $FORMULA | while IFS="," read -r FORMULA_STR
do
  echo $FORMULA_STR
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    for CORES in "${CORES_OPTIONS[@]}"
    do
      for EXECUTOR in "${EXECUTORS[@]}"
      do
        FILE_DIR="results/TEST-CORES/${n}/${EXECUTOR}/${CORES}CORES/RUN${RUN}"
        mkdir -p $FILE_DIR
        rm -f $FILE_DIR/*
        timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --function_executor "$EXECUTOR" \
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
        echo "finished $FILE_DIR"
      done
    done
  done
done


