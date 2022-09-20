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
    m = 10000
    n = 100
    global df
    df = pd.DataFrame(np.ones((m, n)))

    forms.config(
        cores=4,
        scheduler="simple",
        enable_logical_rewriting=False,
        enable_physical_opt=False,
        enable_communication_opt=True,
        partition_shape=(4, 4),
    )
    yield


def test_compute_sum():
    global df
    computed_df = forms.compute_formula(df, "=SUM(A1:B3, SUM(A$1))")
    expected_df = pd.DataFrame(np.full(10000, 7.0))
    expected_df.iloc[-2:, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_compute_ff():
    global df
    computed_df = forms.compute_formula(df, "=SUM(A$2:B$3)")
    expected_df = pd.DataFrame(np.full(10000, 4))
    assert np.array_equal(computed_df.values, expected_df.values)


def test_compute_prioritized_2_phase():
    global df
    forms_config.scheduler = "prioritized"
    forms_config.enable_physical_opt = True
    computed_df = forms.compute_formula(df, "=MAX(SUM(A$2:B3), SUM(A$1:A1))")
    expected_df = pd.DataFrame(np.arange(4, 20000, 2))
    assert np.array_equal(computed_df.iloc[0:9998].values, expected_df.values)


def test_compute_formulas():
    df = pd.DataFrame(
        {"col1": ["A", "B", "C", "D"] * 10, "col2": [1] * 40, "col3": ["A", "B", "C", "D"] * 10}
    )
    forms_config.function_executor = "df_formulas_executor"
    computed_df = forms.compute_formula(df, "=SUMIF(A$1:A$40, C1, B$1:B$40)")
    expected_df = pd.DataFrame(np.full(40, 10.0))
    assert np.array_equal(computed_df.values, expected_df.values)
