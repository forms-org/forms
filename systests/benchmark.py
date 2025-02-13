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
import docker
import pandas as pd
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
from docker.errors import DockerException
from docker.models.containers import Container

from forms.core.forms import from_db

def wait_for_postgres(host, port, user, password, dbname, timeout=15):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            conn.close()
            return True
        except Error:
            time.sleep(1)
    raise Exception("PostgreSQL did not start within the given timeout")

def start_postgres_container(postgres_user: str,
                             dbname: str,
                             password: str,
                             port: int) -> Container:
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

def load_table(postgres_user: str,
               password: str, dbname: str,
               host: str, port: str,
               dataset_path: str,
               schema_path: str, 
               test_table: str):

    # Wait for the PostgreSQL service to be ready
    wait_for_postgres(host=host, port=port, user=postgres_user,
                      password=password, dbname=dbname)

    # Load a DataFrame into the database
    engine = create_engine(f"postgresql://{postgres_user}:{password}@{host}:{port}/{dbname}")
    with open(schema_path, 'r') as schema_file:
        schema_sql = schema_file.read()
    with engine.connect() as connection:
        connection.execute(schema_sql)

    df = pd.read_csv(dataset_path)
    df.to_sql(test_table, engine, if_exists="append", index=False)


def run(dataset_path, schema_path, table_name, primary_key, 
        formula_file_path, run, pipeline_optimization: bool, output_folder):    
    
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
            primary_key=primary_key,
            order_key=order_key,
            enable_rewriting=True,
            enable_pipelining=pipeline_optimization,
        )

        # Parse the formula file
        formula_id = 'formula_id'
        formula_string = 'formula_string'
        formulas = pd.read_csv(formula_file_path, header=None, names=[formula_id, formula_string])

        for _, row in formulas.iterrows():
            formula_id = row['formula_id']
            formula_string = row['formula_string']
            # Execute the formula string
            print(f"Running formula {formula_id}: {formula_string}")
            wb.compute_formula(formula_string)

        # Close the DBWorkbook
        wb.close()

    finally:
        # Tear down the PostgreSQL container
        container.stop()
        container.remove()
 

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Benchmarking script for FormS")
    parser.add_argument("--dataset_path", required=True, help="Path to the dataset folder")
    parser.add_argument("--schema_path", required=True, help="Path to the sql query that creates the table")
    parser.add_argument("--table_name", required=True, help="Name of the table")
    parser.add_argument("--primary_key", required=True, help="Primary key of the table")
    parser.add_argument("--formula_file_path", required=True, help="Path of the formula file")
    parser.add_argument("--run", required=True, help="Test run identifier")
    parser.add_argument("--pipeline_optimization", required=True, help="False: function-level transalation; True: subtree-level transalation)")
    parser.add_argument("--output_folder", required=True, help="Path to the output folder")

    args = parser.parse_args()

    run(
        dataset_path=args.dataset_path,
        schema_path=args.schema_path,
        table_name=args.table_name,
        primary_key=args.primary_key,
        formula_file_path=args.formula_file_path,
        run=args.run,
        pipeline_optimization=args.pipeline_optimization,
        output_folder=args.output_folder
    )