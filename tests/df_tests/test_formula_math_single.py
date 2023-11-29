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

from forms.core.forms import open_workbook_from_df
from tests.df_tests.test_base import test_df

wb = open_workbook_from_df(test_df)


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    global wb
    yield


def test_compute_abs():
    global wb
    computed_df = wb.compute_formula("=ABS(E1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)
    computed_df = wb.compute_formula("=ABS(-5)")
    expected_df = pd.DataFrame(np.array([5] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_acos():
    global wb
    computed_df = wb.compute_formula("=ACOS(B1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_acosh():
    global wb
    computed_df = wb.compute_formula("=ACOSH(B1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_acot():
    global wb
    computed_df = wb.compute_formula("=ACOT(B1)")
    expected_df = pd.DataFrame(np.array([np.pi / 4] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_acoth():
    global wb
    computed_df = wb.compute_formula("=ACOTH(K1)")
    expected_df = pd.DataFrame(np.array([0.549] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_arabic():
    global wb
    computed_df = wb.compute_formula("=ARABIC(I1)")
    expected_df = pd.DataFrame(np.array([1, 6, 9, 1050] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_asin():
    global wb
    computed_df = wb.compute_formula("=ASIN(B1)")
    expected_df = pd.DataFrame(np.array([1.571] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_asinh():
    global wb
    computed_df = wb.compute_formula("=ASINH(D1)")
    expected_df = pd.DataFrame(np.array([0.881, 1.444, 1.818, 2.095] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_atan():
    global wb
    computed_df = wb.compute_formula("=ATAN(B1)")
    expected_df = pd.DataFrame(np.array([0.785] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_atanh():
    global wb
    computed_df = wb.compute_formula("=ATANH(F1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_cos():
    global wb
    computed_df = wb.compute_formula("=COS(B1)")
    expected_df = pd.DataFrame(np.array([0.540] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_cosh():
    global wb
    computed_df = wb.compute_formula("=COSH(B1)")
    expected_df = pd.DataFrame(np.array([1.543] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_cot():
    global wb
    computed_df = wb.compute_formula("=COT(J1)")
    expected_df = pd.DataFrame(np.array([0] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_coth():
    global wb
    computed_df = wb.compute_formula("=COTH(K1)")
    expected_df = pd.DataFrame(np.array([1.037] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_csc():
    global wb
    computed_df = wb.compute_formula("=CSC(B1)")
    expected_df = pd.DataFrame(np.array([1.188] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_csch():
    global wb
    computed_df = wb.compute_formula("=CSCH(B1)")
    expected_df = pd.DataFrame(np.array([0.851] * 40))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)


def test_compute_degrees():
    global wb
    computed_df = wb.compute_formula("=DEGREES(D1)")
    expected_df = pd.DataFrame(np.array([57.296, 114.592, 171.887, 229.183] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_even():
    global wb
    computed_df = wb.compute_formula("=EVEN(G1)")
    expected_df = pd.DataFrame(np.array([2, 2, 4, 4] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_exp():
    global wb
    computed_df = wb.compute_formula("=EXP(D1)")
    expected_df = pd.DataFrame(np.array([2.718, 7.389, 20.086, 54.598] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_fact():
    global wb
    computed_df = wb.compute_formula("=FACT(D1)")
    expected_df = pd.DataFrame(np.array([1, 2, 6, 24] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_fact_double():
    global wb
    computed_df = wb.compute_formula("=FACTDOUBLE(D1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 8] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_int():
    global wb
    computed_df = wb.compute_formula("=INT(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_is_even():
    global wb
    computed_df = wb.compute_formula("=ISEVEN(D1)")
    expected_df = pd.DataFrame(np.array([False, True] * 20))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_is_odd():
    global wb
    computed_df = wb.compute_formula("=ISODD(D1)")
    expected_df = pd.DataFrame(np.array([True, False] * 20))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_ln():
    global wb
    computed_df = wb.compute_formula("=LN(D1)")
    expected_df = pd.DataFrame(np.array([0, 0.693, 1.099, 1.386] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_log10():
    global wb
    computed_df = wb.compute_formula("=LOG10(D1)")
    expected_df = pd.DataFrame(np.array([0, 0.301, 0.477, 0.602] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_negate():
    global wb
    computed_df = wb.compute_formula("=-D1")
    expected_df = pd.DataFrame(np.array([-1, -2, -3, -4] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_odd():
    global wb
    computed_df = wb.compute_formula("=ODD(G1)")
    expected_df = pd.DataFrame(np.array([1, 3, 3, 5] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_radians():
    global wb
    computed_df = wb.compute_formula("=RADIANS(H1)")
    expected_df = pd.DataFrame(np.array([0, np.pi / 6, np.pi / 3, np.pi / 2] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sec():
    global wb
    computed_df = wb.compute_formula("=SEC(B1)")
    expected_df = pd.DataFrame(np.array([1.851] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sech():
    global wb
    computed_df = wb.compute_formula("=SECH(B1)")
    expected_df = pd.DataFrame(np.array([0.648] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sign():
    global wb
    computed_df = wb.compute_formula("=SIGN(E1)")
    expected_df = pd.DataFrame(np.array([-1, 1] * 20))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_sin():
    global wb
    computed_df = wb.compute_formula("=SIN(B1)")
    expected_df = pd.DataFrame(np.array([0.841] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sinh():
    global wb
    computed_df = wb.compute_formula("=SINH(B1)")
    expected_df = pd.DataFrame(np.array([1.175] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sqrt():
    global wb
    computed_df = wb.compute_formula("=SQRT(D1)")
    expected_df = pd.DataFrame(np.array([1, 1.414, 1.732, 2] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_sqrt_pi():
    global wb
    computed_df = wb.compute_formula("=SQRTPI(D1)")
    expected_df = pd.DataFrame(np.array([1.772, 2.507, 3.070, 3.545] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_tan():
    global wb
    computed_df = wb.compute_formula("=TAN(D1)")
    expected_df = pd.DataFrame(np.array([1.557, -2.185, -0.1425, 1.158] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_tanh():
    global wb
    computed_df = wb.compute_formula("=TANH(D1)")
    expected_df = pd.DataFrame(np.array([0.762, 0.964, 0.995, 0.999] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
