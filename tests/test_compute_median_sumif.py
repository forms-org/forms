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
from forms.core.config import forms_config

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


def get_result_with_and_without_sumif_opt(df, formula_str):
    computed_df = forms.compute_formula(df, formula_str)
    forms_config.enable_sumif_opt = True
    computed_df_opt = forms.compute_formula(df, formula_str)
    return computed_df, computed_df_opt


def test_compute_sumif_rr():
    global df
    computed_df, computed_df_opt = get_result_with_and_without_sumif_opt(df, '=SUMIF(A1:C3, ">50")')
    expected_df = pd.concat(
        [pd.DataFrame([0] * 8 + [103, 271, 454]), pd.DataFrame(np.arange(549, 4420, 45))]
    )
    assert np.array_equal(computed_df.iloc[0:98].values, expected_df.values)
    assert np.array_equal(computed_df_opt.iloc[0:98].values, expected_df.values)


def test_compute_sumif_ff():
    global df
    computed_df, computed_df_opt = get_result_with_and_without_sumif_opt(df, '=SUMIF(A$10:C$12, ">50")')
    expected_df = pd.DataFrame(np.full(100, 271))
    assert np.array_equal(computed_df.values, expected_df.values)
    assert np.array_equal(computed_df_opt.values, expected_df.values)


def test_compute_sumif_rf():
    df = pd.DataFrame(np.ones((100, 5)))
    computed_df, computed_df_opt = get_result_with_and_without_sumif_opt(df, '=SUMIF(A1:C$100, "<=1")')
    expected_df = pd.DataFrame(np.arange(300, 0, -3))
    assert np.array_equal(computed_df.values, expected_df.values)
    assert np.array_equal(computed_df_opt.values, expected_df.values)


def test_compute_sumif_fr():
    df = pd.DataFrame(np.ones((100, 5)))
    computed_df, computed_df_opt = get_result_with_and_without_sumif_opt(df, '=SUMIF(A$1:C3, ">=1")')
    expected_df = pd.DataFrame(np.arange(9, 303, 3))
    assert np.array_equal(computed_df.iloc[0:98].values, expected_df.values)
    assert np.array_equal(computed_df_opt.iloc[0:98].values, expected_df.values)
