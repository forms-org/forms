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
import psycopg2


def test_database_connection():
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )

    test_table = os.getenv('POSTGRES_TEST_TABLE')

    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {test_table}")
        result = cur.fetchall()
        assert len(result) == 3
        assert result[0] == (1, 'Alice')
        assert result[1] == (2, 'Bob')
        assert result[2] == (3, 'Charlie')

    conn.close()
