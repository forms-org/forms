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
from forms.executor.executionnode import (
    FunctionExecutionNode,
    RefExecutionNode,
    ExecutionNode,
)
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)


def vlookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    size, values, df, col_idxes, approx = get_vlookup_params(physical_subtree)
    values, col_idxes = values.iloc[:, 0], col_idxes.iloc[:, 0]
    df_arr: list = []
    for i in range(size):
        value, col_idx = values[i], col_idxes[i]
        value_idx, found = approx_binary_search(value, list(df.iloc[:, 0]))
        result = np.nan
        if (found or approx) and value_idx != -1:
            result = df.iloc[value_idx, col_idx - 1]
        df_arr.append(result)
    return construct_df_table(pd.DataFrame(df_arr))


# Performs binary search for value VALUE in array ARR. Assumes the array is sorted.
# If the value is found, returns the index of the value and True.
# If the value is not found, returns the index of the value before the sorted value and False.
# If the value is less than the first value, return -1.
def approx_binary_search(value: any, arr: list) -> tuple[int, bool]:
    assert len(arr) > 0
    if value < arr[0]:
        return -1, False
    low, high = 0, len(arr) - 1
    while low <= high:
        mid = (high + low) // 2
        if arr[mid] < value:
            low = mid + 1
        elif arr[mid] > value:
            high = mid - 1
        else:
            return mid, True
    return (high + low) // 2, False


# Retrives parameters for VLOOKUP.
# The first parameter is the size of the dataframe for this core, followed by the actual VLOOKUP parameters.
def get_vlookup_params(physical_subtree: FunctionExecutionNode) -> tuple:
    # Verify VLOOKUP params
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    # Calculate the number of items in this node
    start_idx = children[1].exec_context.start_formula_idx
    end_idx = children[1].exec_context.end_formula_idx
    size = end_idx - start_idx

    # Retrieve params
    values: pd.DataFrame = clean_string_values(get_literal_value(children[0], size))
    df: pd.DataFrame = get_df(children[1])
    col_idxes: pd.DataFrame = clean_col_idxes(get_literal_value(children[2], size))
    approx: bool = is_approx(children)

    return size, values, df, col_idxes, approx


# Gets a literal value from a child, removing any quotes.
# If the value is in a dataframe, extracts the value from the dataframe.
def get_literal_value(child: ExecutionNode, size: int) -> pd.DataFrame:
    value = get_single_value(child)
    result = value
    if not isinstance(value, pd.DataFrame):
        result = pd.DataFrame([value] * size)
    elif value.shape[0] == 1:
        result = pd.DataFrame([value.iloc[0, 0]] * size)
    return result


# Obtains the full dataframe for this lookup.
def get_df(child: RefExecutionNode) -> pd.DataFrame:
    ref = child.ref
    df: pd.DataFrame = child.table.get_table_content()
    return df.iloc[ref.row : ref.last_row + 1, ref.col : ref.last_col + 1]


# If VLOOKUP has a 4th parameter, determines if approximate match should be used.
def is_approx(children: list) -> bool:
    if len(children) == 4:
        return get_single_value(children[3]) != 0
    return True


# Clean string values by removing quotations.
def clean_string_values(values: pd.DataFrame):
    return values.applymap(lambda x: x.strip('"').strip("'") if isinstance(x, str) else x)


# Clean column indices by casting floats to integers.
def clean_col_idxes(col_idxes: pd.DataFrame):
    return col_idxes.applymap(lambda x: int(x))
