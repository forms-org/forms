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
import math

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode, RefExecutionNode, LitExecutionNode
from forms.executor.dfexecutor.utils import construct_df_table, fill_in_nan, get_value_ff
from forms.utils.reference import axis_along_row, RefType


def is_odd_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    value = get_math_single_function_values(physical_subtree)
    df = value.applymap(lambda x: x % 2 == 1)
    return construct_df_table(df)


def sin_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    value = get_math_single_function_values(physical_subtree)
    df = value.applymap(math.sin)
    return construct_df_table(df)


def get_math_single_function_values(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    assert len(physical_subtree.children) == 1
    child = physical_subtree.children[0]
    value = pd.DataFrame([])
    # Can probably abstract this into a util function taking in a parameter CHILD and returning VALUE
    if isinstance(child, RefExecutionNode):
        ref = child.ref
        df = child.table.get_table_content()
        out_ref_type = child.out_ref_type
        start_idx = child.exec_context.start_formula_idx
        end_idx = child.exec_context.end_formula_idx
        n_formula = end_idx - start_idx
        axis = child.exec_context.axis
        if axis == axis_along_row:
            df = df.iloc[:, ref.col : ref.last_col + 1]
            if out_ref_type == RefType.RR:
                value = df.iloc[start_idx + ref.row : end_idx + ref.row]
                value.index, value.columns = [range(value.index.size), range(value.columns.size)]
                value = fill_in_nan(value, n_formula)
            elif out_ref_type == RefType.FF:
                value = get_value_ff(df.iloc[ref.row : ref.last_row + 1], n_formula)
    elif isinstance(child, LitExecutionNode):
        value = child.literal
    return value
