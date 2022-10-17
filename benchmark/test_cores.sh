#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a CORES_OPTIONS=(1 2 4 8 16 32)
declare -a EXECUTORS=("df_pandas_executor")
declare FILENAME='weather_10M.csv'
declare FORMULA=formula_in_all_types.csv
declare ROWS=10000000

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
        if [ $n == 9 ] || [ $n == 10 ]
        then
          timeout 5m python3 test_driver.py \
              --filename "$FILENAME" \
              --formula_str "$FORMULA_STR" \
              --cores "$CORES" \
              --function_executor "$EXECUTOR" \
              --enable_sumif_opt True\
              --row_num "$ROWS" \
              --output_path "$FILE_DIR" &> $FILE_DIR/run.log
        else
          timeout 5m python3 test_driver.py \
                --filename "$FILENAME" \
                --formula_str "$FORMULA_STR" \
                --cores "$CORES" \
                --function_executor "$EXECUTOR" \
                --row_num "$ROWS" \
                --output_path "$FILE_DIR" &> $FILE_DIR/run.log
        fi
        echo "finished $FILE_DIR"
      done
    done
  done
done


