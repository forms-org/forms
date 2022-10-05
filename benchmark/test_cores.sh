#!/bin/bash

declare -a RUN_OPTIONS=(1 2 3)
declare -a CORES_OPTIONS=(1 2 4 8 16 32)
declare -a ROW_OPTIONS=(10000 20000 40000)

INPUT=spreadsheet_formula.csv
sed 1d $INPUT | while IFS=, read -r filename formula_str
n=0
do
  echo $filename
  echo $formula_str
  n=$(($n+1))
	for RUN in "${RUN_OPTIONS[@]}"
  do
    for ROW_NUM in "${ROW_OPTIONS[@]}"
    do
      for CORES in "${CORES_OPTIONS[@]}"
      do
        FILE_DIR="results/TEST${n}/RUN${RUN}/${ROW_NUM}ROWS/${CORES}CORES/${TYPE}"
        mkdir -p $FILE_DIR
        rm -f $FILE_DIR/*
        python3 test_driver.py \
            --filename "$filename" \
            --formula_str "$formula_str" \
            --cores "$CORES" \
            --row_num "$ROW_NUM" \
            --output_path "$FILE_DIR" &> $FILE_DIR/run.log
        echo "finished $FILE_DIR"
      done
    done
  done
done

