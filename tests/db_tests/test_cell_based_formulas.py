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

import pytest
import os
import pandas as pd
import numpy as np

from forms.core.forms import from_db


@pytest.fixture(scope="module")
def get_wb():
    wb = from_db(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        table_name=os.getenv("POSTGRES_TEST_TABLE"),
        primary_key=[os.getenv("POSTGRES_PRIMARY_KEY")],
        order_key=[os.getenv("POSTGRES_ORDER_KEY")],
        enable_rewriting=False,
    )

    # Yield the object to be used in tests
    yield wb
    # Close the DBWorkbook
    wb.close()


def test_print_table(get_wb):
    wb = get_wb
    wb.print_workbook()


def test_plus(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=B1+C1")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [4, 5, 6, 7]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_cells_in_different_rows(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=B1+C2")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [5, 6, 7, np.nan]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_multiple_functions(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=A1-B1+C2")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [2, 4, 6, np.nan]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_if_function(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=IF(A1 < 3, B1, C1)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [2, 2, 4, 5]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)
