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

from forms.executor.dfexecutor.dfexecnode import DFExecNode, DFLitExecNode, DFRefExecNode
from forms.executor.dfexecutor.dftable import DFTable
from forms.utils.reference import RefType, axis_along_row


def get_refs(exec_subtree):
    refs = []
    if isinstance(exec_subtree, DFRefExecNode):
        refs.append(exec_subtree)
    elif exec_subtree.children:
        for child in exec_subtree.children:
            refs.extend(get_refs(child))
    return refs


def construct_df_table(array):
    return DFTable(df=pd.DataFrame(array))


def get_value_rr(
    df: pd.DataFrame, window_size: int, func1, func2, along_row_first: bool = False
) -> pd.DataFrame:
    return (
        df.agg(func1, axis=1).rolling(window_size, min_periods=window_size).agg(func2).dropna()
        if not along_row_first
        else df.rolling(window_size, min_periods=window_size).agg(func1).dropna().agg(func2, axis=1)
    )


def get_value_ff(single_value, n_formula: int) -> pd.DataFrame:
    return pd.DataFrame(np.full(n_formula, single_value))


def get_value_fr(
    df: pd.DataFrame, min_window_size: int, func1, func2, along_row_first: bool = False
) -> pd.DataFrame:
    return (
        df.agg(func1, axis=1).expanding(min_window_size).agg(func2).dropna()
        if not along_row_first
        else df.expanding(min_window_size).agg(func1).dropna().agg(func2, axis=1)
    )


def get_value_rf(
    df: pd.DataFrame, min_window_size: int, func1, func2, along_row_first: bool = False
) -> pd.DataFrame:
    return (
        df.iloc[::-1].agg(func1, axis=1).expanding(min_window_size).agg(func2).dropna().iloc[::-1]
        if not along_row_first
        else df.iloc[::-1].expanding(min_window_size).agg(func1).dropna().agg(func2, axis=1).iloc[::-1]
    )


def fill_in_nan(value, n_formula: int) -> pd.DataFrame:
    if n_formula > len(value):
        extra = pd.DataFrame(np.full(n_formula - len(value), np.nan))
        value = pd.concat([value, extra], ignore_index=True)
    return value


def get_reference_indices(ref_node: DFRefExecNode):
    start_idx = ref_node.exec_context.start_formula_idx
    end_idx = ref_node.exec_context.end_formula_idx
    out_ref_type = ref_node.out_ref_type
    ref = ref_node.ref
    row_length = ref.last_row + 1 - ref.row
    col_width = ref.last_col + 1 - ref.col
    row = ref.row
    col = ref.col
    if out_ref_type == RefType.RR:
        return start_idx + row, col, end_idx + ref.last_row, col + col_width
    elif out_ref_type == RefType.FF:
        return row, col, row + row_length, col + col_width
    elif out_ref_type == RefType.FR:
        return row, col, ref.last_row + end_idx, col + col_width
    elif out_ref_type == RefType.RF:
        return row + start_idx, col, ref.last_row + 1, col + col_width


def get_reference_indices_for_single_index(ref_node: DFRefExecNode, idx: int):
    ref = ref_node.ref
    table = ref_node.table
    row_length = ref.last_row + 1 - ref.row
    col_width = ref.last_col + 1 - ref.col
    row = ref.row
    col = ref.col
    out_ref_type = ref_node.out_ref_type
    if out_ref_type == RefType.RR and idx + ref.last_row + 1 <= table.get_num_of_rows():
        return row + idx, col, row + idx + row_length, col + col_width
    elif out_ref_type == RefType.FF:
        return row, col, row + row_length, col + col_width
    elif out_ref_type == RefType.FR and idx + ref.last_row + 1 <= table.get_num_of_rows():
        return row, col, row + row_length + idx, col + col_width
    elif out_ref_type == RefType.RF and ref.row + idx < ref.last_row + 1:
        return row + idx, col, row + row_length, col + col_width
    return None


def get_single_value(child: DFExecNode):
    value = pd.DataFrame([])
    if isinstance(child, DFRefExecNode):
        df = child.table.get_table_content()
        out_ref_type = child.out_ref_type
        start_idx = child.exec_context.start_formula_idx
        end_idx = child.exec_context.end_formula_idx
        n_formula = end_idx - start_idx
        axis = child.exec_context.axis
        if axis == axis_along_row:
            start_row, start_column, end_row, end_column = get_reference_indices(child)
            df = df.iloc[start_row:end_row, start_column:end_column]
            if out_ref_type == RefType.RR:
                value = df
                value.index, value.columns = [range(value.index.size), range(value.columns.size)]
                value = fill_in_nan(value, n_formula)
            elif out_ref_type == RefType.FF:
                value = get_value_ff(df, n_formula)
    elif isinstance(child, DFLitExecNode):
        value = child.literal
    return value
