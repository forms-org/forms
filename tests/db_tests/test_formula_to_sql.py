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


# Mixed formulas
# def test_mixed_formulas(get_wb):
#     wb = get_wb
#     computed_df = wb.compute_formula("=A1+INDEX(D$1:D$4,A1)")
#     expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [3, 3, 4, np.nan]})
#     assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


# Lookup formulas

# TODO: index function is correct, but pandas does not execute "nth_value()" function in SQL
# def test_translate_index__to_window(get_wb):
#     wb = get_wb
#     computed_df = wb.compute_formula("=INDEX(C1:D2,B1,1)")
#     expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [3, 4, 5, np.nan]})
#     assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_translate_index_function_to_join(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=INDEX(D$1:D$4,A1)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [2, 2, 3, np.nan]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_translate_exact_lookup(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=LOOKUP(D1, B$1:B$4, C$1:C$4, 0)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [np.nan, 2, 2, np.nan]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_translate_approximate_lookup(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=LOOKUP(D1, B$1:B$4, C$1:C$4, 1)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [5, 5, 5, 5]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


# Range-wise formulas
def test_sum_over_columns(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=SUM(B1:C1)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [4, 5, 6, 7]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_sum_over_columns_and_rows(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=SUM(B1:C2)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [9, 11, 13, 7]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_sum_on_fr(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=SUM(B$1:C1)")
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [4, 9, 15, 22]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_sum_if_over_columns(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula('=SUMIF(B1:C1,">2")')
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [0, 3, 4, 5]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_sum_if_over_rows(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula('=SUMIF(C1:C2,">2")')
    expected_df = pd.DataFrame({"row_id": [1, 2, 3, 4], "A": [3, 7, 9, 5]})
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


# Cell-wise formulas
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
