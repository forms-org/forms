#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <table_name> <dataset_file_name> <formula_file_name> <primary_key> <enable_optimization>"
    exit 1
fi

# Assign arguments
TABLE_NAME="$1"
DATASET_FILE_NAME="$2"
FORMULA_FILE_NAME="$3"
PRIMARY_KEY="$4"
OPT="$5"

# Define file paths
DATASET_PATH="$TEST_HOME/datasets/${DATASET_FILE_NAME}"
SCHEMA_PATH="$TEST_HOME/SQLs/${TABLE_NAME}.sql"
FORMULA_FILE_PATH="$TEST_HOME/formulas/${FORMULA_FILE_NAME}"
OUTPUT_FOLDER="$TEST_HOME/output"

# Loop to run the command three times with run values 1, 2, 3
for RUN in {1..3}; do
    echo "Running benchmark for $TABLE_NAME (Run: $RUN)..."
    python benchmark.py \
        --dataset_path "$DATASET_PATH" \
        --schema_path "$SCHEMA_PATH" \
        --table_name "$TABLE_NAME" \
        --primary_key "$PRIMARY_KEY" \
        --formula_file_path "$FORMULA_FILE_PATH" \
        --run "$RUN" \
        --pipeline_optimization $OPT \
        --output_folder "$OUTPUT_FOLDER"
done

echo "Benchmark completed for $TABLE_NAME $FORMULA_FILE_NAME."
