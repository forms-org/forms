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
from forms.executor.dfexecutor.utils import construct_df_table, get_execution_node_n_formula
from forms.executor.dfexecutor.lookup.utils import (
    approx_binary_search,
    clean_string_values,
    get_df,
    get_literal_value,
)


def lookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values, search_range, result_range = get_lookup_params(physical_subtree)
    values = values.iloc[:, 0]
    result_df = lookup_sort_merge(values, search_range, result_range)
    return construct_df_table(result_df)


def lookup_binary_search(values, search_range, result_range) -> pd.DataFrame:
    df_arr: list = []
    for i in range(len(values)):
        value_idx = approx_binary_search(values[i], list(search_range))
        result = np.nan
        if value_idx != -1:
            result = result_range[value_idx]
        df_arr.append(result)
    return pd.DataFrame(df_arr)


def lookup_sort_merge(values, search_range, result_range) -> pd.DataFrame:
    # Sort values and preserve index for later
    sorted_values = list(enumerate(values))
    sorted_values.sort(key=lambda x: x[1])

    # Use sorted_values as the left and search_range as the right
    left_idx, right_idx = 0, 0
    df_arr: list = [np.nan] * len(values)
    while left_idx < len(values):
        searching_val = sorted_values[left_idx][1]
        while right_idx < len(search_range) and search_range[right_idx] <= searching_val:
            right_idx += 1
        stop_val = search_range[right_idx] if right_idx < len(search_range) else np.infty
        while left_idx < len(values) and sorted_values[left_idx][1] < stop_val:
            if right_idx != 0:
                df_arr[sorted_values[left_idx][0]] = result_range[right_idx - 1]
            left_idx += 1

    return pd.DataFrame(df_arr)


# Retrives parameters for LOOKUP.
def get_lookup_params(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify LOOKUP param count
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 2 or num_children == 3

    # Retrieve params
    size = get_execution_node_n_formula(children[1])
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size))
    search_range: pd.DataFrame = get_df(children[1])
    if num_children == 2:
        result_range = search_range.iloc[:, -1]
        search_range = search_range.iloc[:, 0]
    else:
        result_range = get_df(children[2]).iloc[:, 0]
        search_range = search_range.iloc[:, 0]

    return values, search_range, result_range
