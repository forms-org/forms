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
from tests.test_config import test_df_big, DF_ROWS

df = pd.DataFrame([])


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    global df
    df = test_df_big
    cores = 4
    forms.config(cores=cores, function_executor="df_pandas_executor", partition_shape=(cores, 1))
    yield


def test_compute_lookup():
    global df
    computed_df = forms.compute_formula(df, f"=LOOKUP(C1 + 0.5, C1:C{DF_ROWS}, G1:G{DF_ROWS})")
    expected_df = pd.DataFrame(np.tile([0.4111, 1.6222, 2.93333333, 3.999], DF_ROWS // 4))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=LOOKUP(C1, C1:G{DF_ROWS})")
    expected_df = pd.DataFrame(np.tile([0.4111, 1.6222, 2.93333333, 3.999], DF_ROWS // 4))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=LOOKUP(1.5, C1:C{DF_ROWS}, G1:G{DF_ROWS})")
    expected_df = pd.DataFrame(np.tile(1.6222, DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=LOOKUP(1.5, C1:G{DF_ROWS})")
    expected_df = pd.DataFrame(np.tile(1.6222, DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=LOOKUP(-1, C1:C{DF_ROWS}, G1:G{DF_ROWS})")
    assert computed_df.iloc[:, 0].isnull().all()


def test_compute_vlookup_exact():
    global df
    computed_df = forms.compute_formula(df, f'=VLOOKUP("B", A1:I{DF_ROWS}, 3, FALSE)')
    expected_df = pd.DataFrame(np.tile([1], DF_ROWS))
    assert np.array_equal(computed_df.values, expected_df.values)
    computed_df = forms.compute_formula(df, f"=VLOOKUP(1, C1:I{DF_ROWS}, 5, 0)")
    expected_df = pd.DataFrame(np.tile([1.6222], DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=VLOOKUP(1.5, C1:I{DF_ROWS}, 5, FALSE)")
    assert computed_df.iloc[:, 0].isnull().all()


def test_compute_vlookup_approx():
    global df
    computed_df = forms.compute_formula(df, f"=VLOOKUP(1.5, C1:I{DF_ROWS}, 5, 1)")
    expected_df = pd.DataFrame(np.tile([1.6222], DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=SUM(VLOOKUP(1.5, C1:I{DF_ROWS}, 5, 1), VLOOKUP(1.5, C1:I{DF_ROWS}, 5, 1))")
    expected_df = pd.DataFrame(np.tile([3.2444], DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=VLOOKUP({DF_ROWS + 1}, C1:I{DF_ROWS}, 5, TRUE)")
    expected_df = pd.DataFrame(np.tile([3.999], DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=VLOOKUP(-1, C1:I{DF_ROWS}, 5, 1)")
    assert computed_df.iloc[:, 0].isnull().all()
    computed_df = forms.compute_formula(df, f"=VLOOKUP(C1, C1:I{DF_ROWS}, $C$6)")
    expected_df = pd.DataFrame(np.tile([0.4111, 1.6222, 2.93333333, 3.999], (DF_ROWS // 4)))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)
    computed_df = forms.compute_formula(df, f"=VLOOKUP($C$4, C1:I{DF_ROWS}, $C$6)")
    expected_df = pd.DataFrame(np.tile([3.999], DF_ROWS))
    assert np.allclose(computed_df.values, expected_df.values, atol=1e-03)

    # Below executions are for performance only
    forms.compute_formula(df, f"=TRIM(VLOOKUP(M1, M1:O{DF_ROWS}, 3))")
    forms.compute_formula(df, f"=VLOOKUP(M1, M1:O{DF_ROWS}, 3)")
