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
from time import time

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
        result_df = vlookup_approx_np_vector(values, df, col_idxes)
    else:
        result_df = vlookup_exact_hash_vector(values, df, col_idxes)
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
    search_range = df.iloc[:, 0]
    value_idxes = np.searchsorted(list(search_range), list(values), side="left")
    result_arr = [np.nan] * len(values)
    for i in range(len(values)):
        value, value_idx, col_idx = values.iloc[i], value_idxes[i], col_idxes.iloc[i]
        if value_idx >= len(search_range) or value != search_range.iloc[value_idx]:
            value_idx -= 1
        if value_idx != -1:
            result_arr[i] = df.iloc[value_idx, col_idx - 1]
    return pd.DataFrame(result_arr)


def vlookup_approx_np_lc(values, df, col_idxes) -> pd.DataFrame:
    search_range = df.iloc[:, 0]
    value_idxes = np.searchsorted(list(search_range), list(values), side="left")
    value_idxes = [((value_idxes[i] - 1)
                    if (value_idxes[i] >= len(search_range) or values.iloc[i] != search_range.iloc[value_idxes[i]])
                    else value_idxes[i]) for i in range(len(value_idxes))]
    result_arr = [df.iloc[value_idxes[i], col_idxes.iloc[i] - 1] if value_idxes != -1 else np.nan
                  for i in range(len(values))]
    return pd.DataFrame(result_arr)


def vlookup_approx_np_vector(values, df, col_idxes) -> pd.DataFrame:
    search_range = df.iloc[:, 0]
    value_idxes = np.searchsorted(list(search_range), list(values), side="left")
    greater_than_length = np.greater_equal(value_idxes, len(search_range))
    value_idxes_no_oob = np.minimum(value_idxes, len(search_range) - 1)
    search_range_values = np.take(search_range, value_idxes_no_oob)
    approximate_matches = (values.reset_index(drop=True) != search_range_values.reset_index(drop=True))
    combined = np.logical_or(greater_than_length, approximate_matches).astype(int)
    adjusted_idxes = value_idxes - combined
    row_res = np.take(df.to_numpy(), adjusted_idxes, axis=0)
    res = np.choose(col_idxes - 1, row_res.T).to_numpy()
    nan_mask = np.equal(adjusted_idxes, -1)
    nan_idxes = nan_mask[nan_mask].index
    if np.float64 > res.dtype:
        res = res.astype(np.float64)
    if len(nan_idxes) > 0:
        np.put(res, nan_idxes, np.nan)
    res_type = type(res[0])
    if np.issubdtype(type(res[0]), np.integer):
        res_type = np.float64
    return pd.DataFrame(res).astype(res_type)


def vlookup_exact_hash(values, df, col_idxes) -> pd.DataFrame:
    search_range = df.iloc[:, 0]
    cache = {}
    for i in range(df.shape[0]):
        value = search_range.iloc[i]
        if value not in cache:
            cache[value] = i
    result_arr = [np.nan] * len(values)
    for i in range(len(values)):
        value, col_idx = values.iloc[i], col_idxes.iloc[i]
        if value in cache:
            value_idx = cache[value]
            result = df.iloc[value_idx, col_idx - 1]
            result_arr[i] = result
    return pd.DataFrame(result_arr)


def vlookup_exact_hash_vector(values, df, col_idxes) -> pd.DataFrame:
    search_range = df.iloc[:, 0]
    idxes = list(enumerate(search_range))
    idxes.reverse()
    cache = {v: i for i, v in idxes}
    nan_mask = values.isin(set(cache.keys())) ^ True
    nan_idxes = nan_mask.to_numpy().nonzero()
    print(len(nan_idxes[0]))
    start_time = time()
    result_idxes = values.replace(cache)
    print(f"replace time: {time() - start_time}")
    np.put(result_idxes.to_numpy(), nan_idxes, 1)
    row_res = np.take(df.to_numpy(), result_idxes, axis=0, mode='clip')
    res = np.choose(col_idxes - 1, row_res.T).to_numpy()
    if np.float64 > res.dtype:
        res = res.astype(np.float64)
    if len(nan_idxes) > 0:
        np.put(res, nan_idxes, np.nan)
    res_type = type(res[0])
    if np.issubdtype(type(res[0]), np.integer):
        res_type = np.float64
    return pd.DataFrame(res).astype(res_type)


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
