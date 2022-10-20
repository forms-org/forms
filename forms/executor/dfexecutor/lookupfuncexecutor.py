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
    size, value, df, col_idx, approx = get_vlookup_params(physical_subtree)
    value_idx, found = approx_binary_search(value, list(df.iloc[:, 0]))
    result = np.nan
    if (found or approx) and value_idx != -1:
        result = df.iloc[value_idx, col_idx - 1]
    return construct_df_table(pd.DataFrame([result] * size))


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
def get_vlookup_params(
    physical_subtree: FunctionExecutionNode,
) -> tuple[int, any, pd.DataFrame, int, bool]:
    # Verify VLOOKUP params
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4

    # Retrieve params
    value: any = get_literal_value(children[0])
    df: pd.DataFrame = get_df(children[1])
    col_idx: int = int(get_literal_value(children[2]))
    approx: bool = get_approx(children[3], num_children)

    # Calculate the number of items in this node
    start_idx = children[1].exec_context.start_formula_idx
    end_idx = children[1].exec_context.end_formula_idx
    n_formula = end_idx - start_idx

    return n_formula, value, df, col_idx, approx


# Gets a literal value from a child, removing any quotes.
# If the value is in a dataframe, extracts the value from the dataframe.
def get_literal_value(child: ExecutionNode) -> any:
    value = get_single_value(child)
    if isinstance(value, pd.DataFrame):
        value = value.iloc[0, 0]
    if isinstance(value, str):
        value = value.strip('"').strip("'")
    return value


# Obtains the full dataframe for this lookup.
def get_df(child: RefExecutionNode) -> pd.DataFrame:
    ref = child.ref
    df: pd.DataFrame = child.table.get_table_content()
    return df.iloc[ref.row : ref.last_row + 1, ref.col : ref.last_col + 1]


# If VLOOKUP has a 4th parameter, determines if approximate match should be used.
def get_approx(child: ExecutionNode, num_children: int) -> bool:
    if num_children == 4:
        return get_single_value(child) != 0
    return True
