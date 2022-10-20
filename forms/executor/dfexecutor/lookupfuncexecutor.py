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
from forms.executor.executionnode import FunctionExecutionNode, LitExecutionNode, RefExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)


def vlookup_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    size, value, df, col_idx, approx = get_lookup_params(physical_subtree)

    value_idx, found = binary_search(value, df)

    result = np.nan
    if found or approx:
        result = df.iloc[value_idx, col_idx - 1]

    return construct_df_table(pd.DataFrame([result] * size))


def binary_search(value, df) -> tuple[int, bool]:
    low, high = 0, df.shape[0]
    while low <= high:
        mid = (high + low) // 2
        if df.iloc[mid, 0] < value:
            low = mid + 1
        elif df.iloc[mid, 0] > value:
            high = mid - 1
        else:
            return mid, True
    print("Binary search debug:", low, high)
    return high, False


def get_lookup_params(
    physical_subtree: FunctionExecutionNode,
) -> tuple[int, any, pd.DataFrame, int, bool]:
    # Verify VLOOKUP params
    children = physical_subtree.children
    num_children = len(children)
    assert num_children == 3 or num_children == 4
    assert isinstance(children[0], LitExecutionNode)
    assert isinstance(children[1], RefExecutionNode)
    assert isinstance(children[2], LitExecutionNode)

    # Retrieve params
    value = get_single_value(children[0])
    if isinstance(value, str):
        value = value.strip('"').strip("'")
    ref_node: RefExecutionNode = children[1]
    ref = ref_node.ref
    df: pd.DataFrame = ref_node.table.get_table_content()
    df = df.iloc[ref.row : ref.last_row + 1, ref.col : ref.last_col + 1]
    col_idx: int = int(get_single_value(children[2]))
    approx: bool = True
    if num_children == 4:
        isinstance(children[3], LitExecutionNode)
        approx = get_single_value(children[3]) != 0

    # Calculate the number of items in this node
    start_idx = children[1].exec_context.start_formula_idx
    end_idx = children[1].exec_context.end_formula_idx
    n_formula = end_idx - start_idx

    return n_formula, value, df, col_idx, approx
