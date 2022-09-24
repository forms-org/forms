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

df = pd.DataFrame([])


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    global df
    df = pd.DataFrame(
        {
            "col1": ["A", "B", "C", "D"] * 10,
            "col2": [1] * 40,
            "col3": ["A", "B", "C", "D"] * 10,
            "col4": [1, 2, 3, 4] * 10,
            "col5": [-1, 2, -3, 4] * 10,
            "col6": [0] * 40,
            "col7": [0.4, 1.6, 2.9, 3.999] * 10,
            "col8": [0, 30, 60, 90] * 10,
        }
    )
    forms.config(cores=4, function_executor="df_pandas_executor")
    yield


def test_compute_abs():
    global df
    computed_df = forms.compute_formula(df, "=ABS(E1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_acos():
    global df
    computed_df = forms.compute_formula(df, "=ACOS(B1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_acosh():
    global df
    computed_df = forms.compute_formula(df, "=ACOSH(B1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_asin():
    global df
    computed_df = forms.compute_formula(df, "=ASIN(B1)")
    expected_df = pd.DataFrame(np.array([1.571] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_asinh():
    global df
    computed_df = forms.compute_formula(df, "=ASINH(D1)")
    expected_df = pd.DataFrame(np.array([0.881, 1.444, 1.818, 2.095] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_atan():
    global df
    computed_df = forms.compute_formula(df, "=ATAN(B1)")
    expected_df = pd.DataFrame(np.array([0.785] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_atanh():
    global df
    computed_df = forms.compute_formula(df, "=ATANH(F1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_cos():
    global df
    computed_df = forms.compute_formula(df, "=COS(B1)")
    expected_df = pd.DataFrame(np.array([0.540] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_cosh():
    global df
    computed_df = forms.compute_formula(df, "=COSH(B1)")
    expected_df = pd.DataFrame(np.array([1.543] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_degrees():
    global df
    computed_df = forms.compute_formula(df, "=DEGREES(D1)")
    expected_df = pd.DataFrame(np.array([57.296, 114.592, 171.887, 229.183] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_exp():
    global df
    computed_df = forms.compute_formula(df, "=EXP(D1)")
    expected_df = pd.DataFrame(np.array([2.718, 7.389, 20.086, 54.598] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_fact():
    global df
    computed_df = forms.compute_formula(df, "=FACT(D1)")
    expected_df = pd.DataFrame(np.array([1, 2, 6, 24] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_int():
    global df
    computed_df = forms.compute_formula(df, "=INT(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_is_even():
    global df
    computed_df = forms.compute_formula(df, "=ISEVEN(D1)")
    expected_df = pd.DataFrame(np.array([False, True] * 20))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_is_odd():
    global df
    computed_df = forms.compute_formula(df, "=ISODD(D1)")
    expected_df = pd.DataFrame(np.array([True, False] * 20))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_ln():
    global df
    computed_df = forms.compute_formula(df, "=LN(D1)")
    expected_df = pd.DataFrame(np.array([0, 0.693, 1.099, 1.386] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_log10():
    global df
    computed_df = forms.compute_formula(df, "=LOG10(D1)")
    expected_df = pd.DataFrame(np.array([0, 0.301, 0.477, 0.602] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_radians():
    global df
    computed_df = forms.compute_formula(df, "=RADIANS(H1)")
    expected_df = pd.DataFrame(np.array([0, np.pi / 6, np.pi / 3, np.pi / 2] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sign():
    global df
    computed_df = forms.compute_formula(df, "=SIGN(E1)")
    expected_df = pd.DataFrame(np.array([-1, 1] * 20))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sin():
    global df
    computed_df = forms.compute_formula(df, "=SIN(B1)")
    expected_df = pd.DataFrame(np.array([0.841] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sinh():
    global df
    computed_df = forms.compute_formula(df, "=SINH(B1)")
    expected_df = pd.DataFrame(np.array([1.175] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sqrt():
    global df
    computed_df = forms.compute_formula(df, "=SQRT(D1)")
    expected_df = pd.DataFrame(np.array([1, 1.414, 1.732, 2] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sqrt_pi():
    global df
    computed_df = forms.compute_formula(df, "=SQRTPI(D1)")
    expected_df = pd.DataFrame(np.array([1.772, 2.507, 3.070, 3.545] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_tan():
    global df
    computed_df = forms.compute_formula(df, "=TAN(D1)")
    expected_df = pd.DataFrame(np.array([1.557, -2.185, -0.1425, 1.158] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_tanh():
    global df
    computed_df = forms.compute_formula(df, "=TANH(D1)")
    expected_df = pd.DataFrame(np.array([0.762, 0.964, 0.995, 0.999] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
