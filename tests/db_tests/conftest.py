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
import pytest
import docker
import pandas as pd
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
from docker.errors import DockerException


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


@pytest.fixture(scope="session", autouse=True)
def setup_postgres():
    docker_client = docker.from_env()
    host = "localhost"
    port = "5432"
    postgres_user = "dt"
    password = "1234"
    dbname = "forms_db"
    test_table = "test_table"

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

    try:
        # Wait for the PostgreSQL service to be ready
        wait_for_postgres(host=host, port=port, user=postgres_user, password=password, dbname=dbname)

        # Set the environment variables for the database connection
        os.environ["POSTGRES_USER"] = postgres_user
        os.environ["POSTGRES_PASSWORD"] = password
        os.environ["POSTGRES_DB"] = dbname
        os.environ["POSTGRES_HOST"] = host
        os.environ["POSTGRES_PORT"] = port

        # Load a DataFrame into the database
        engine = create_engine(f"postgresql://{postgres_user}:{password}@{host}:{port}/{dbname}")
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 2, 2, 2], "c": [2, 3, 4, 5], "d": [3, 2, 2, 3]})
        df.to_sql(test_table, engine, if_exists="replace", index=False)

        os.environ["POSTGRES_TEST_TABLE"] = test_table
        os.environ["POSTGRES_PRIMARY_KEY"] = "a"
        os.environ["POSTGRES_ORDER_KEY"] = "a"

        yield

    finally:
        # Tear down the PostgreSQL container
        container.stop()
        container.remove()
