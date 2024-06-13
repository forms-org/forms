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
import pandas as pd
import numpy as np

from forms.core.forms import from_df


@pytest.fixture(scope="module")
def get_wb():
    m = 100
    n = 5
    df = pd.DataFrame(np.ones((m, n)))
    wb = from_df(df)
    yield wb
    wb.close()


def test_compute_sum(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=SUM(A1:B3)")
    expected_df = pd.DataFrame(np.full(100, 6.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_sum_ff(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=SUM(A$1:B$3)")
    expected_df = pd.DataFrame(np.full(100, 6.0))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_literal(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=SUM(A1:B3, 10)")
    expected_df = pd.DataFrame(np.full(100, 16.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_plus(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=A1+1")
    expected_df = pd.DataFrame(np.full(100, 2.0))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_minus(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=10-A3")
    expected_df = pd.DataFrame(np.full(100, 9.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_multiply(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=10*A1")
    expected_df = pd.DataFrame(np.full(100, 10.0))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_divide(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=A$1/B3")
    expected_df = pd.DataFrame(np.full(100, 1.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_max(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=MAX(A1:B3, 3)")
    expected_df = pd.DataFrame(np.full(100, 3))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_min(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=MIN(A1:B3, A$1, A1:A$100, 3)")
    expected_df = pd.DataFrame(np.full(100, 1))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_count(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=COUNT(A1:B3,B1:B2)")
    expected_df = pd.DataFrame(np.full(100, 8))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_average(get_wb):
    wb = get_wb
    computed_df = wb.compute_formula("=AVERAGE(A1:B3,A$1,A1:A$100,A$1:A1,1,1,1)")
    # local_df = pd.DataFrame(np.ones((5, 10)))
    # local_wb = open_workbook_from_df(local_df)
    # computed_df = local_wb.compute_formula("=AVERAGE(A1:B3, A$1, A$1:A1, 1,1,1)")
    expected_df = pd.DataFrame(np.full(100, 1))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)
