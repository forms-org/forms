#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a ROWS_OPTIONS=(10000 2 4 8 16 32)
declare -a EXECUTORS=("df_pandas_executor" "df_formulas_executor")
declare filename='weather90000.csv'

INPUT=spreadsheet_formula.csv
sed 1d $INPUT | while IFS="," read -r formula_str
do
  echo $formula_str
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    for CORES in "${CORES_OPTIONS[@]}"
    do
      for EXECUTOR in "${EXECUTORS[@]}"
      do
        FILE_DIR="results/TEST-ROWS/${n}/${EXECUTOR}/RUN${RUN}/${CORES}CORES"
        mkdir -p $FILE_DIR
        rm -f $FILE_DIR/*
        python3 test_driver.py \
              --filename "$filename" \
              --formula_str "$formula_str" \
              --cores "$CORES" \
              --function_executor= "$EXECUTOR" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
        echo "finished $FILE_DIR"
      done
    done
  done
done

