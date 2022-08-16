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

import forms
from forms import forms_config
from forms.executor.pandasexecutor.functionexecutor import *
from forms.executor.scheduler import Schedulers

table = None


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    m = 100
    n = 5
    global df
    df = pd.DataFrame(np.ones((m, n)))

    forms_config.cores = 4
    forms_config.scheduler = Schedulers.SIMPLE.name.lower()
    forms_config.enable_logical_rewriting = False
    forms_config.enable_physical_opt = False
    yield


def test_compute_sum():
    global df
    computed_df = forms.compute_formula(df, "=SUM(A1:B3)")
    expected_df = pd.DataFrame(np.full(100, 6.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_literal():
    global df
    computed_df = forms.compute_formula(df, "=SUM(A1:B3, 10)")
    expected_df = pd.DataFrame(np.full(100, 16.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_plus():
    global df
    computed_df = forms.compute_formula(df, "=A1+1")
    expected_df = pd.DataFrame(np.full(100, 2.0))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_minus():
    global df
    computed_df = forms.compute_formula(df, "=10-A3")
    expected_df = pd.DataFrame(np.full(100, 9.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_multiply():
    global df
    computed_df = forms.compute_formula(df, "=10*A1")
    expected_df = pd.DataFrame(np.full(100, 10.0))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_divide():
    global df
    computed_df = forms.compute_formula(df, "=A$1/B3")
    expected_df = pd.DataFrame(np.full(100, 1.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_max():
    global df
    computed_df = forms.compute_formula(df, "=MAX(A1:B3, 3)")
    expected_df = pd.DataFrame(np.full(100, 3))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_min():
    global df
    computed_df = forms.compute_formula(df, "=MIN(A1:B3, 3)")
    expected_df = pd.DataFrame(np.full(100, 1))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_count():
    global df
    computed_df = forms.compute_formula(df, "=COUNT(A1:B3)")
    expected_df = pd.DataFrame(np.full(100, 6))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_average():
    global df
    computed_df = forms.compute_formula(df, "=AVERAGE(A1:B3,1,1,1,1)")
    expected_df = pd.DataFrame(np.full(100, 1))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)
