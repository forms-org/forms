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

df = None


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    m = 100
    n = 5
    global df
    df = pd.DataFrame(np.array(np.arange(0, m * n).reshape(m, n)))

    forms.config(
        cores=4,
        scheduler="simple",
    )
    yield


def test_compute_median_rr():
    global df
    computed_df = forms.compute_formula(df, "=MEDIAN(A1:C3)")
    expected_df = pd.DataFrame(np.arange(6, 492, 5))
    assert np.array_equal(computed_df.iloc[0:98].values, expected_df.values, equal_nan=True)


def test_compute_median_ff():
    global df
    computed_df = forms.compute_formula(df, "=MEDIAN(A$1:C$3)")
    expected_df = pd.DataFrame(np.full(100, 6))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_median_rf():
    global df
    computed_df = forms.compute_formula(df, "=MEDIAN(A1:C$100)")
    expected_df = pd.DataFrame(np.arange(248.5, 497, 2.5))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_median_fr():
    global df
    computed_df = forms.compute_formula(df, "=MEDIAN(A$1:C3)")
    expected_df = pd.DataFrame(np.arange(6, 249, 2.5))
    assert np.array_equal(computed_df.iloc[0:98].values, expected_df.values, equal_nan=True)
