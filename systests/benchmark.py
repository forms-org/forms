#  Copyright 2022-2023 The FormS Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import time
import json
import docker
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2 import Error
from sqlalchemy import create_engine
from docker.errors import DockerException
from docker.models.containers import Container

from forms.core.forms import from_db


def start_postgres_container(postgres_user: str, dbname: str, password: str, port: int) -> Container:
    docker_client = docker.from_env()

    # Start the PostgreSQL container
    try:
        container = docker_client.containers.run(
            "postgres:13",
            environment={
                "POSTGRES_USER": postgres_user,
                "POSTGRES_PASSWORD": password,
                "POSTGRES_DB": dbname,
            },
            ports={"5432/tcp": int(port)},
            detach=True,
        )
    except DockerException as e:
        raise Exception(f"Failed to start PostgreSQL container: {e}")

    return container


def load_table(
    postgres_user: str,
    password: str,
    dbname: str,
    host: str,
    port: str,
    dataset_path: str,
    schema_path: str,
    test_table: str,
):
    timeout = 15
    start_time = time.time()
    conn = None
    cursor = None
    while time.time() - start_time < timeout:
        try:
            conn = psycopg2.connect(
                dbname=dbname, user=postgres_user, password=password, host=host, port=port
            )
            with open(schema_path, "r") as schema_file:
                schema_sql = schema_file.read()
            cursor = conn.cursor()
            cursor.execute(sql.SQL(schema_sql))
            with open(dataset_path, "r", encoding="utf-8") as csv_file:
                cursor.copy_expert(f"COPY {test_table} FROM STDIN WITH CSV", csv_file)
            conn.commit()
            return True
        except Error:
            time.sleep(1)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    raise Exception("PostgreSQL did not start within the given timeout")


def run(
    dataset_path,
    schema_path,
    table_name,
    primary_key,
    formula_file_path,
    run: int,
    pipeline_optimization: bool,
    output_folder,
):

    host = "localhost"
    port = "5432"
    postgres_user = "dt"
    password = "1234"
    dbname = "forms_db"
    order_key = primary_key

    container = start_postgres_container(postgres_user, dbname, password, port)

    try:
        load_table(postgres_user, password, dbname, host, port, dataset_path, schema_path, table_name)

        wb = from_db(
            host=host,
            port=int(port),
            username=postgres_user,
            password=password,
            db_name=dbname,
            table_name=table_name,
            primary_key=[primary_key],
            order_key=[order_key],
            enable_rewriting=True,
            enable_pipelining=pipeline_optimization,
        )

        # Parse the formula file
        formula_string = "formula_string"
        formulas = pd.read_csv(formula_file_path, header=None, names=[formula_string], delimiter="|")
        optimization_str = "subtree" if pipeline_optimization else "function"

        output_data = {}
        for index, row in formulas.iterrows():
            formula_string = row["formula_string"]
            # Execute the formula string
            print(f"Running formula {index+1}: {formula_string}")
            wb.compute_formula(formula_string)
            metrics = wb.get_metrics()
            output_payload = {
                "formula_id": index + 1,
                "formula_string": formula_string,
                "run": run,
                "optimization": optimization_str,
                "metrics": metrics,
            }
            output_data[formula_string] = output_payload

        # Close the DBWorkbook
        wb.close()

        # Save the statistics
        formula_file_name = os.path.basename(formula_file_path)

        output_file = os.path.join(
            output_folder, table_name, formula_file_name, optimization_str, str(run), "result.json"
        )
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=4)

    finally:
        # Tear down the PostgreSQL container
        container.stop()
        container.remove()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Benchmarking script for FormS")
    parser.add_argument("--dataset_path", required=True, help="Path to the dataset folder")
    parser.add_argument(
        "--schema_path", required=True, help="Path to the sql query that creates the table"
    )
    parser.add_argument("--table_name", required=True, help="Name of the table")
    parser.add_argument("--primary_key", required=True, help="Primary key of the table")
    parser.add_argument("--formula_file_path", required=True, help="Path of the formula file")
    parser.add_argument("--run", required=True, type=int, help="Test run identifier")
    parser.add_argument(
        "--pipeline_optimization",
        required=True,
        help="False: function-level translation; True: subtree-level translation",
    )
    parser.add_argument("--output_folder", required=True, help="Path to the output folder")

    args = parser.parse_args()

    pipeline_optimization = args.pipeline_optimization.lower() == "true"

    run(
        dataset_path=args.dataset_path,
        schema_path=args.schema_path,
        table_name=args.table_name,
        primary_key=args.primary_key,
        formula_file_path=args.formula_file_path,
        run=args.run,
        pipeline_optimization=pipeline_optimization,
        output_folder=args.output_folder,
    )
