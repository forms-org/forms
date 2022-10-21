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


def test_compute_vlookup_exact():
    global df
    computed_df = forms.compute_formula(df, '=VLOOKUP("B", A1:I40, 7, 0)')
    expected_df = pd.DataFrame(np.array([1.6222] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, "=VLOOKUP(1, C1:I40, 5, 0)")
    expected_df = pd.DataFrame(np.array([1.6222] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, "=VLOOKUP(1.5, C1:I40, 5, 0)")
    assert computed_df.iloc[:, 0].isnull().all()


def test_compute_vlookup_approx():
    global df
    computed_df = forms.compute_formula(df, "=VLOOKUP(1.5, C1:I40, 5, 1)")
    expected_df = pd.DataFrame(np.array([1.6222] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, "=VLOOKUP(41, C1:I40, 5, 1)")
    expected_df = pd.DataFrame(np.array([3.999] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, "=VLOOKUP(-1, C1:I40, 5, 1)")
    assert computed_df.iloc[:, 0].isnull().all()
    computed_df = forms.compute_formula(df, "=VLOOKUP(C1, $C$1:$I$40, $C$6)")
    expected_df = pd.DataFrame(np.array([0.4111, 1.6222, 2.93333333, 3.999] * 10))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, "=VLOOKUP($C$4, $C$1:$I$40, $C$6)")
    expected_df = pd.DataFrame(np.array([3.999] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
