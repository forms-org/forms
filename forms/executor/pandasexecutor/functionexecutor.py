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

from forms.executor.table import *
from forms.executor.executionnode import *
from forms.utils.functions import *
from forms.utils.reference import axis_along_row


def df_executor(physical_subtree: FunctionExecutionNode, function: Function) -> DFTable:
    values = []
    func1, func2, func3, literal = function_to_parameters_dict[function]
    if physical_subtree.fr_rf_optimization == FRRFOptimization.NOOPT:
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
                    h = ref.last_row - ref.row + 1
                    if out_ref_type == RefType.RR:
                        df = df.iloc[start_idx + ref.row : end_idx + ref.last_row]
                        value = df.rolling(h, min_periods=h).agg(func1).dropna().apply(func2, axis=1)
                    elif out_ref_type == RefType.FF:
                        value = func3(df.iloc[ref.row : ref.last_row + 1].values.flatten())
                        value = pd.DataFrame(np.full(n_formula, value))
                    elif out_ref_type == RefType.FR:
                        df = df.iloc[ref.row : end_idx + ref.last_row]
                        value = df.expanding(h + start_idx).agg(func1).dropna().apply(func2, axis=1)
                    elif out_ref_type == RefType.RF:
                        df = df.iloc[ref.row + start_idx : ref.last_row]
                        value = (
                            df.iloc[::-1]
                            .expanding(h - end_idx)
                            .agg(func1)
                            .dropna()
                            .iloc[::-1]
                            .apply(func2, axis=1)
                        )
                    value.index = range(value.index.size)
                    if out_ref_type != RefType.FF and n_formula > len(value):
                        extra = pd.DataFrame(np.full(n_formula - len(value), np.nan))
                        value = pd.concat([value, extra], ignore_index=True)
                values.append(pd.DataFrame(value))
            elif isinstance(child, LitExecutionNode):
                if function == Function.COUNT:
                    literal += 1
                else:
                    literal = func1((literal, child.literal))
        if function == Function.MAX:
            return construct_df_table(np.max(values, initial=literal, axis=0))
        elif function == Function.MIN:
            return construct_df_table(np.min(values, initial=literal, axis=0))
        elif function == Function.SUM:
            return construct_df_table(sum(values) + literal)
        elif function == Function.COUNT:
            return construct_df_table(sum(values) + literal)
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
                    value = (
                        df.expanding(h if start_idx == 0 else 1).agg(func1).dropna().apply(func2, axis=1)
                    )
                else:  # RefType.RF
                    value = (
                        df.iloc[::-1]
                        .expanding(max(1, h - end_idx) if end_idx == all_formula_idx[-1] else 1)
                        .agg(func1)
                        .dropna()
                        .iloc[::-1]
                        .apply(func2, axis=1)
                    )
                value.index = range(value.index.size)
                if n_formula > len(value):
                    extra = pd.DataFrame(np.full(n_formula - len(value), np.nan))
                    value = pd.concat([value, extra], ignore_index=True)
            return construct_df_table(value)
        elif physical_subtree.fr_rf_optimization == FRRFOptimization.PHASETWO:
            if axis == axis_along_row:
                additional = literal
                if out_ref_type == RefType.FR:
                    # calculate the last values of previous partitions
                    for i, formula_idx in enumerate(all_formula_idx):
                        if start_idx > formula_idx:
                            additional = func2((additional, df.iloc[all_formula_idx[i + 1] - 1][0]))
                        else:
                            break
                else:  # RefType.RF
                    # calculate the first values of following partitions
                    for formula_idx in reversed(all_formula_idx):
                        if formula_idx == all_formula_idx[-1]:
                            continue
                        elif start_idx < formula_idx:
                            additional = func2((additional, df.iloc[formula_idx][0]))
                        else:
                            break
                value = df.iloc[start_idx:end_idx].values
            if function == Function.MAX:
                return construct_df_table(np.max(value, initial=additional, axis=1))
            elif function == Function.MIN:
                return construct_df_table(np.min(value, initial=additional, axis=1))
            elif function == Function.SUM:
                return construct_df_table(value + additional)
            elif function == Function.COUNT:
                return construct_df_table(value + additional)


def max_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, Function.MAX)


def min_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, Function.MIN)


def count_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, Function.COUNT)


def sum_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, Function.SUM)


def average_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return DFTable(
        df=df_executor(physical_subtree, Function.SUM).df
        / df_executor(physical_subtree, Function.COUNT).df
    )


def binary_df_executor(physical_subtree: FunctionExecutionNode, function: Function) -> DFTable:
    values = []
    assert len(physical_subtree.children) == 2
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
                    if n_formula > len(value):
                        extra = pd.DataFrame(np.full(n_formula - len(value), np.nan))
                        value = pd.concat([value, extra], ignore_index=True)
                elif out_ref_type == RefType.FF:
                    value = np.array(df.iloc[ref.row : ref.last_row + 1])
                    value = pd.DataFrame(np.full(n_formula, value))
            values.append(value)
        elif isinstance(child, LitExecutionNode):
            values.append(child.literal)
    if function == Function.PLUS:
        return construct_df_table(values[0] + values[1])
    elif function == Function.MINUS:
        return construct_df_table(values[0] - values[1])
    elif function == Function.MULTIPLY:
        return construct_df_table(values[0] * values[1])
    elif function == Function.DIVIDE:
        return construct_df_table(values[0] / values[1])


def plus_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return binary_df_executor(physical_subtree, Function.PLUS)


def minus_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return binary_df_executor(physical_subtree, Function.MINUS)


def multiply_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return binary_df_executor(physical_subtree, Function.MULTIPLY)


def divide_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return binary_df_executor(physical_subtree, Function.DIVIDE)


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
}

function_to_parameters_dict = {
    Function.MAX: [max] * 3 + [-math.inf],
    Function.MIN: [min] * 3 + [math.inf],
    Function.COUNT: ["count", sum, np.ma.count, 0],
    Function.SUM: [sum] * 3 + [0],
}


def find_function_executor(function: Function):
    return function_to_executor_dict[function]


def construct_df_table(array):
    return DFTable(df=pd.DataFrame(array))
