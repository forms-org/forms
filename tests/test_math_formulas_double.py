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

import forms
from tests.test_config import test_df

df = pd.DataFrame([])


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    global df
    df = test_df
    forms.config(cores=4, function_executor="df_pandas_executor")
    yield


def test_compute_atan2():
    global df
    computed_df = forms.compute_formula(df, "=ATAN2(D1, E1)")
    expected_df = pd.DataFrame(np.array([2.356, 0.785] * 20))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_decimal():
    global df
    computed_df = forms.compute_formula(df, "=DECIMAL(C1, 16)")
    expected_df = pd.DataFrame(np.array([10, 11, 12, 13] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_mod():
    global df
    computed_df = forms.compute_formula(df, "=MOD(D1, K1)")
    expected_df = pd.DataFrame(np.array([1, 0] * 20))
    assert np.array_equal(computed_df.values, expected_df.values)


# NOTE: this version of MROUND uses "banker's rounding", which rounds 0.5 to the even number.
# This is different than Google Sheets, which rounds 0.5 up to the next integer.
# However, banker's rounding is the "modern" way of rounding as it helps reduce bias with many numbers.
# "Banker's rounding" is the default for Python.
# More info: https://en.wikipedia.org/wiki/Rounding#Round_half_to_even
def test_compute_mround():
    global df
    computed_df = forms.compute_formula(df, "=MROUND(D1, K1)")
    expected_df = pd.DataFrame(np.array([0, 2, 4, 4] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_power():
    global df
    computed_df = forms.compute_formula(df, "=POWER(D1, 3)")
    expected_df = pd.DataFrame(np.array([1, 8, 27, 64] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)
    computed_df = forms.compute_formula(df, "=POWER(-5, 3)")
    expected_df = pd.DataFrame(np.array([-125] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_rand_between():
    global df
    computed_df = forms.compute_formula(df, "=RANDBETWEEN(F1, K1)")
    for i in list(computed_df):
        assert 0 <= i <= 2
