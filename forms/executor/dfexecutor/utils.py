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

import pandas as pd
import numpy as np

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode


def construct_df_table(array):
    return DFTable(df=pd.DataFrame(array))


def get_value_rr(df: pd.DataFrame, window_size: int, func1, func2) -> pd.DataFrame:
    return df.agg(func1, axis=1).rolling(window_size, min_periods=window_size).agg(func2).dropna()


def get_value_ff(single_value, n_formula: int) -> pd.DataFrame:
    return pd.DataFrame(np.full(n_formula, single_value))


def get_value_fr(df: pd.DataFrame, min_window_size: int, func1, func2) -> pd.DataFrame:
    return df.agg(func1, axis=1).expanding(min_window_size).agg(func2).dropna()


def get_value_rf(df: pd.DataFrame, min_window_size: int, func1, func2) -> pd.DataFrame:
    return df.iloc[::-1].agg(func1, axis=1).expanding(min_window_size).agg(func2).dropna().iloc[::-1]


def fill_in_nan(value, n_formula: int) -> pd.DataFrame:
    if n_formula > len(value):
        extra = pd.DataFrame(np.full(n_formula - len(value), np.nan))
        value = pd.concat([value, extra], ignore_index=True)
    return value


# TODO
def remote_access_planning(exec_subtree: FunctionExecutionNode) -> FunctionExecutionNode:
    return exec_subtree
