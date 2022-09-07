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
import math

import numpy as np
import pandas as pd

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode, RefExecutionNode, LitExecutionNode
from forms.utils.functions import Function
from forms.utils.reference import axis_along_row, RefType
from forms.utils.optimizations import FRRFOptimization
from forms.executor.dfexecutor.formulasexecutor import formulas_executor


def max_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.MAX)


def min_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.MIN)


def count_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.COUNT)


def sum_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.SUM)


def average_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return DFTable(
        df=distributive_function_executor(physical_subtree, Function.SUM).df
        / distributive_function_executor(physical_subtree, Function.COUNT).df
    )


def plus_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] + values[1])


def minus_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] - values[1])


def multiply_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] * values[1])


def divide_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] / values[1])


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


def compute_max(values, literal):
    return np.max(values, initial=literal, axis=0)


def compute_min(values, literal):
    return np.min(values, initial=literal, axis=0)


def compute_sum(values, literal):
    return sum(values) + literal


def distributive_function_executor(
    physical_subtree: FunctionExecutionNode, function: Function
) -> DFTable:
    values = []
    (
        func_first_axis,
        func_second_axis,
        func_ff,
        func_literal,
        func_all,
        literal,
    ) = distributive_function_to_parameters_dict[function]
    if physical_subtree.fr_rf_optimization == FRRFOptimization.NOOPT:
        if physical_subtree.out_ref_type == RefType.FF:
            # all children must be either FF-type RefNode or LiteralNode
            for child in physical_subtree.children:
                if isinstance(child, RefExecutionNode):
                    ref = child.ref
                    df = child.table.get_table_content()
                    value = func_ff(
                        df.iloc[ref.row : ref.last_row + 1, ref.col : ref.last_col + 1].values.flatten()
                    )
                    values.append(value)
                elif isinstance(child, LitExecutionNode):
                    literal = func_literal((literal, child.literal))
            # construct a one-cell dataframe table
            return construct_df_table([func_all(values, literal)])
        else:
            for child in physical_subtree.children:
                if isinstance(child, RefExecutionNode):
                    ref = child.ref
                    df = child.table.get_table_content()
                    out_ref_type = child.out_ref_type
                    start_idx = child.exec_context.start_formula_idx
                    end_idx = child.exec_context.end_formula_idx
                    n_formula = end_idx - start_idx
                    axis = child.exec_context.axis
                    # TODO: add support for axis_along_column
                    if axis == axis_along_row:
                        df = df.iloc[:, ref.col : ref.last_col + 1]
                        window_size = ref.last_row - ref.row + 1
                        if out_ref_type == RefType.RR:
                            df = df.iloc[start_idx + ref.row : end_idx + ref.last_row]
                            value = get_value_rr(df, window_size, func_first_axis, func_second_axis)
                        elif out_ref_type == RefType.FF:
                            single_value = func_ff(df.iloc[ref.row : ref.last_row + 1].values.flatten())
                            value = get_value_ff(single_value, n_formula)
                        elif out_ref_type == RefType.FR:
                            df = df.iloc[ref.row : end_idx + ref.last_row]
                            value = get_value_fr(
                                df, window_size + start_idx, func_first_axis, func_second_axis
                            )
                        elif out_ref_type == RefType.RF:
                            df = df.iloc[ref.row + start_idx : ref.last_row]
                            value = get_value_rf(
                                df, window_size - end_idx, func_first_axis, func_second_axis
                            )
                        value.index = range(value.index.size)
                        value = fill_in_nan(value, n_formula)
                    values.append(pd.DataFrame(value))
                elif isinstance(child, LitExecutionNode):
                    literal = func_literal((literal, child.literal))
            return construct_df_table(func_all(values, literal))
    else:
        assert len(physical_subtree.children) == 1
        child = physical_subtree.children[0]
        ref = child.ref
        df = child.table.get_table_content()
        out_ref_type = child.out_ref_type
        start_idx = child.exec_context.start_formula_idx
        end_idx = child.exec_context.end_formula_idx
        n_formula = end_idx - start_idx
        axis = child.exec_context.axis
        all_formula_idx = child.exec_context.all_formula_idx
        if physical_subtree.fr_rf_optimization == FRRFOptimization.PHASEONE:
            if axis == axis_along_row:
                df = df.iloc[:, ref.col : ref.last_col + 1]
                h = ref.last_row - ref.row + 1
                if out_ref_type == RefType.FR:
                    df = df.iloc[
                        ref.row if start_idx == 0 else start_idx + ref.last_row : end_idx + ref.last_row
                    ]
                    value = get_value_fr(
                        df, h if start_idx == 0 else 1, func_first_axis, func_second_axis
                    )
                else:  # RefType.RF
                    df = df.iloc[
                        ref.row + start_idx : ref.last_row + 1
                        if end_idx == all_formula_idx[-1]
                        else ref.row + end_idx
                    ]
                    value = get_value_rf(
                        df,
                        max(1, h - end_idx) if end_idx == all_formula_idx[-1] else 1,
                        func_first_axis,
                        func_second_axis,
                    )
                value.index = range(value.index.size)
                value = fill_in_nan(value, n_formula)
            return construct_df_table(value)
        elif physical_subtree.fr_rf_optimization == FRRFOptimization.PHASETWO:
            if axis == axis_along_row:
                additional = literal
                if out_ref_type == RefType.FR:
                    # calculate the last values of previous partitions
                    for i, formula_idx in enumerate(all_formula_idx):
                        if start_idx > formula_idx:
                            additional = func_second_axis(
                                (additional, df.iloc[all_formula_idx[i + 1] - 1][0])
                            )
                        else:
                            break
                else:  # RefType.RF
                    # calculate the first values of following partitions
                    for formula_idx in reversed(all_formula_idx):
                        if formula_idx == all_formula_idx[-1]:
                            continue
                        elif start_idx < formula_idx:
                            additional = func_second_axis((additional, df.iloc[formula_idx][0]))
                        else:
                            break
                value = df.iloc[start_idx:end_idx].values
            return construct_df_table(func_all([value], additional))


def get_arithmetic_function_values(physical_subtree: FunctionExecutionNode) -> list:
    values = []
    assert len(physical_subtree.children) == 2
    if physical_subtree.out_ref_type == RefType.FF:
        for child in physical_subtree.children:
            if isinstance(child, RefExecutionNode):
                ref = child.ref
                df = child.table.get_table_content()
                values.append(
                    df.iloc[ref.row : ref.last_row + 1, ref.col : ref.last_col + 1].values.flatten()
                )
            elif isinstance(child, LitExecutionNode):
                values.append(child.literal)
    else:
        for child in physical_subtree.children:
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
                values.append(value)
            elif isinstance(child, LitExecutionNode):
                values.append(child.literal)
    return values


distributive_function_to_parameters_dict = {
    Function.MAX: [max] * 4 + [compute_max, -math.inf],
    Function.MIN: [min] * 4 + [compute_min, math.inf],
    Function.COUNT: ["count", sum, np.ma.count, lambda x: x[0] + 1, compute_sum, 0],
    Function.SUM: [sum] * 4 + [compute_sum, 0],
}


function_to_executor_dict = {
    Function.MAX: max_df_executor,
    Function.MIN: min_df_executor,
    Function.COUNT: count_df_executor,
    Function.AVG: average_df_executor,
    Function.SUM: sum_df_executor,
    Function.PLUS: plus_df_executor,
    Function.MINUS: minus_df_executor,
    Function.MULTIPLY: multiply_df_executor,
    Function.DIVIDE: divide_df_executor,
    Function.FORMULAS: formulas_executor,
}


def find_function_executor(function: Function):
    return function_to_executor_dict[function]
