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


def test_compute_ceiling():
    global df
    computed_df = forms.compute_formula(df, "=CEILING(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.42, 1.63, 2.94, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=CEILING(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=CEILING(-0.4111, 0.01)")
    expected_df = pd.DataFrame(np.array([-0.41] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_ceiling_math():
    global df
    computed_df = forms.compute_formula(df, "=CEILING.MATH(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.42, 1.63, 2.94, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=CEILING.MATH(-0.4111, 0.01, 0)")
    expected_df = pd.DataFrame(np.array([-0.41] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=CEILING.MATH(-0.4111, 0.01, 1)")
    expected_df = pd.DataFrame(np.array([-0.42] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_ceiling_precise():
    global df
    computed_df = forms.compute_formula(df, "=CEILING.PRECISE(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=CEILING.PRECISE(4.3, -2)")
    expected_df = pd.DataFrame(np.array([6] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=CEILING.PRECISE(-4.3, 2)")
    expected_df = pd.DataFrame(np.array([-4] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_floor():
    global df
    computed_df = forms.compute_formula(df, "=FLOOR(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=FLOOR(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=FLOOR(-0.4111, 0.01)")
    expected_df = pd.DataFrame(np.array([-0.42] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_floor_math():
    global df
    computed_df = forms.compute_formula(df, "=FLOOR.MATH(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=FLOOR.MATH(-0.4111, 0.01, 0)")
    expected_df = pd.DataFrame(np.array([-0.42] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=FLOOR.MATH(-0.4111, 0.01, 1)")
    expected_df = pd.DataFrame(np.array([-0.41] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_floor_precise():
    global df
    computed_df = forms.compute_formula(df, "=FLOOR.PRECISE(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=FLOOR.PRECISE(4.3, -2)")
    expected_df = pd.DataFrame(np.array([4] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=FLOOR.PRECISE(-4.3, 2)")
    expected_df = pd.DataFrame(np.array([-6] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_iso_ceiling():
    global df
    computed_df = forms.compute_formula(df, "=ISO.CEILING(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=ISO.CEILING(4.3, -2)")
    expected_df = pd.DataFrame(np.array([6] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=ISO.CEILING(-4.3, 2)")
    expected_df = pd.DataFrame(np.array([-4] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_roman():
    global df
    computed_df = forms.compute_formula(df, "=ROMAN(D1)")
    expected_df = pd.DataFrame(np.array(["I", "II", "III", "IV"] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)
    computed_df = forms.compute_formula(df, "=ROMAN(H1)")
    expected_df = pd.DataFrame(np.array(["N", "XXX", "LX", "XC"] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_round():
    global df
    computed_df = forms.compute_formula(df, "=ROUND(G1)")
    expected_df = pd.DataFrame(np.array([0, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=ROUND(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_round_down():
    global df
    computed_df = forms.compute_formula(df, "=ROUNDDOWN(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=ROUNDDOWN(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_round_up():
    global df
    computed_df = forms.compute_formula(df, "=ROUNDUP(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=ROUNDUP(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.42, 1.63, 2.94, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_trunc():
    global df
    computed_df = forms.compute_formula(df, "=TRUNC(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = forms.compute_formula(df, "=TRUNC(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
