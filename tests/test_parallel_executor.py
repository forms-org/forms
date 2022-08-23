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
from forms.executor.scheduler import Schedulers

df = None


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    global df
    m = 100
    n = 5
    df = pd.DataFrame(np.ones((m, n)))

    forms.config(
        scheduler=Schedulers.SIMPLE.name.lower(),
        enable_logical_rewriting=False,
        enable_physical_opt=False,
    )

    yield


def test_one_worker():
    forms.config(cores=1)
    computed_df = forms.compute_formula(df, "=SUM(A1:B3, A1:C2)")
    expected_df = pd.DataFrame(np.full(100, 12.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)


def test_multiple_workers():
    forms.config(cores=4)
    computed_df = forms.compute_formula(df, "=SUM(A1:B3, A1:C2)")
    expected_df = pd.DataFrame(np.full(100, 12.0))
    expected_df.iloc[98:100, 0] = np.NaN
    assert np.array_equal(computed_df.values, expected_df.values, equal_nan=True)
