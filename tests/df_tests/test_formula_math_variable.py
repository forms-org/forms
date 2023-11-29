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


def test_compute_ceiling():
    global wb
    computed_df = wb.compute_formula("=CEILING(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.42, 1.63, 2.94, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=CEILING(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=CEILING(-0.4111, 0.01)")
    expected_df = pd.DataFrame(np.array([-0.41] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_ceiling_math():
    global wb
    computed_df = wb.compute_formula("=CEILING.MATH(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.42, 1.63, 2.94, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=CEILING.MATH(-0.4111, 0.01, 0)")
    expected_df = pd.DataFrame(np.array([-0.41] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=CEILING.MATH(-0.4111, 0.01, 1)")
    expected_df = pd.DataFrame(np.array([-0.42] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_ceiling_precise():
    global wb
    computed_df = wb.compute_formula("=CEILING.PRECISE(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=CEILING.PRECISE(4.3, -2)")
    expected_df = pd.DataFrame(np.array([6] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=CEILING.PRECISE(-4.3, 2)")
    expected_df = pd.DataFrame(np.array([-4] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_floor():
    global wb
    computed_df = wb.compute_formula("=FLOOR(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=FLOOR(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=FLOOR(-0.4111, 0.01)")
    expected_df = pd.DataFrame(np.array([-0.42] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_floor_math():
    global wb
    computed_df = wb.compute_formula("=FLOOR.MATH(G1, 0.01)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=FLOOR.MATH(-0.4111, 0.01, 0)")
    expected_df = pd.DataFrame(np.array([-0.42] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=FLOOR.MATH(-0.4111, 0.01, 1)")
    expected_df = pd.DataFrame(np.array([-0.41] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_floor_precise():
    global wb
    computed_df = wb.compute_formula("=FLOOR.PRECISE(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=FLOOR.PRECISE(4.3, -2)")
    expected_df = pd.DataFrame(np.array([4] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=FLOOR.PRECISE(-4.3, 2)")
    expected_df = pd.DataFrame(np.array([-6] * 40))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_roman():
    global wb
    computed_df = wb.compute_formula("=ROMAN(D1)")
    expected_df = pd.DataFrame(np.array(["I", "II", "III", "IV"] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)
    computed_df = wb.compute_formula("=ROMAN(H1)")
    expected_df = pd.DataFrame(np.array(["N", "XXX", "LX", "XC"] * 10))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_round():
    global wb
    computed_df = wb.compute_formula("=ROUND(G1)")
    expected_df = pd.DataFrame(np.array([0, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=ROUND(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_round_down():
    global wb
    computed_df = wb.compute_formula("=ROUNDDOWN(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=ROUNDDOWN(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_round_up():
    global wb
    computed_df = wb.compute_formula("=ROUNDUP(G1)")
    expected_df = pd.DataFrame(np.array([1, 2, 3, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=ROUNDUP(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.42, 1.63, 2.94, 4] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)


def test_compute_trunc():
    global wb
    computed_df = wb.compute_formula("=TRUNC(G1)")
    expected_df = pd.DataFrame(np.array([0, 1, 2, 3] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
    computed_df = wb.compute_formula("=TRUNC(G1, 2)")
    expected_df = pd.DataFrame(np.array([0.41, 1.62, 2.93, 3.99] * 10))
    assert np.allclose(computed_df.values, expected_df.values, rtol=1e-03)
