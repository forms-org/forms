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
import numpy as np
import pandas as pd

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_execution_node_n_formula,
    get_single_value,
)
from forms.executor.dfexecutor.lookup.utils import (
    approx_binary_search,
    clean_col_idxes,
    clean_string_values,
    get_df,
    get_literal_value,
)


def vlookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values, df, col_idxes, approx = get_vlookup_params(physical_subtree)
    values, col_idxes = values.iloc[:, 0], col_idxes.iloc[:, 0]
    if approx:
        result_df = vlookup_approx(values, df, col_idxes)
    else:
        result_df = vlookup_exact_hash(values, df, col_idxes)
    return construct_df_table(result_df)


def vlookup_approx(values, df, col_idxes) -> pd.DataFrame:
    df_arr: list = []
    for i in range(len(values)):
        value, col_idx = values[i], col_idxes[i]
        value_idx = approx_binary_search(value, list(df.iloc[:, 0]))
        result = np.nan
        if value_idx != -1:
            result = df.iloc[value_idx, col_idx - 1]
        df_arr.append(result)
    return pd.DataFrame(df_arr)


def vlookup_approx_np(values, df, col_idxes) -> pd.DataFrame:
    df_arr: list = [np.nan] * len(values)
    search_range = df.iloc[:, 0]
    value_idxes = np.searchsorted(list(search_range), list(values), side="left")
    for i in range(len(values)):
        value, value_idx, col_idx = values[i], value_idxes[i], col_idxes[i]
        if value_idx >= len(search_range) or value != search_range[value_idx]:
            value_idx -= 1
        if value_idx != -1:
            df_arr[i] = df.iloc[value_idx, col_idx - 1]
    return pd.DataFrame(df_arr)


def vlookup_exact_hash(values, df, col_idxes) -> pd.DataFrame:
    df_arr: list = [np.nan] * len(values)
    cache = {}
    for i in range(df.shape[0]):
        value = df.iloc[i, 0]
        if value not in cache:
            cache[value] = i
    for i in range(len(values)):
        value, col_idx = values[i], col_idxes[i]
        if value in cache:
            value_idx = cache[value]
            result = df.iloc[value_idx, col_idx - 1]
            df_arr[i] = result
    return pd.DataFrame(df_arr)


def vlookup_exact_loops(values, df, col_idxes) -> pd.DataFrame:
    df_arr: list = []
    for i in range(len(values)):
        value, col_idx = values[i], col_idxes[i]
        value_idx = exact_scan_search(value, list(df.iloc[:, 0]))
        result = np.nan
        if value_idx != -1:
            result = df.iloc[value_idx, col_idx - 1]
        df_arr.append(result)
    return pd.DataFrame(df_arr)


# Performs a scan of array ARR to find value VALUE.
# If the value is not found, returns -1.
def exact_scan_search(value: any, arr: list) -> int:
    for i in range(len(arr)):
        if arr[i] == value:
            return i
    return -1


# Retrives parameters for VLOOKUP.
def get_vlookup_params(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify VLOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    # Retrieve params
    size = get_execution_node_n_formula(children[1])
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size))
    df: pd.DataFrame = get_df(children[1])
    col_idxes: pd.DataFrame = clean_col_idxes(get_literal_value(children[2], size))
    approx: bool = True
    if len(children) == 4:
        approx = get_single_value(children[3]) != 0

    return values, df, col_idxes, approx
