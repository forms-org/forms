#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a CORES_OPTIONS=(1 2 4 8 16 32)
declare filename='weather90000.csv'

INPUT=spreadsheet_formula.csv
n=0
echo $n
sed 1d $INPUT | while IFS="," read -r formula_str
do
  echo $formula_str
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    for CORES in "${CORES_OPTIONS[@]}"
    do
      FILE_DIR="results/TEST${n}/RUN${RUN}/${CORES}CORES"
      mkdir -p $FILE_DIR
      rm -f $FILE_DIR/*
      python3 test_driver.py \
            --filename "$filename" \
            --formula_str "$formula_str" \
            --cores "$CORES" \
            --output_path "$FILE_DIR" &> $FILE_DIR/run.log
      echo "finished $FILE_DIR"
    done
  done
done

