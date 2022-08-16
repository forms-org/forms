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

import pandas as pd
import numpy as np

from forms.executor.table import *
from forms.executor.executionnode import *
from forms.utils.functions import *


def df_executor(physical_subtree: FunctionExecutionNode, function, default_literal) -> DFTable:
    values = []
    literal = default_literal
    for child in physical_subtree.children:
        if isinstance(child, RefExecutionNode):
            ref = child.ref
            df = child.table.df
            out_ref_type = child.out_ref_type
            start_idx = child.exec_context.start_formula_idx
            end_idx = child.exec_context.end_formula_idx
            n_formula = end_idx - start_idx
            axis = child.exec_context.axis
            if axis == 0:
                df = df.iloc[:, ref.col : ref.last_col + 1]
                h = ref.last_row - ref.row + 1
                if out_ref_type == RefType.RR:
                    df = df.iloc[start_idx + ref.row : end_idx + ref.last_row]
                    value = df.rolling(h).agg(function).dropna().apply(function, axis=1)
                elif out_ref_type == RefType.FF:
                    value = function(df.iloc[ref.row : ref.last_row + 1].values.flatten())
                    value = np.full(n_formula, value)
                elif out_ref_type == RefType.FR:
                    df = df.iloc[ref.row : end_idx + ref.last_row]
                    value = df.expanding(h + start_idx).apply(function).dropna().apply(function, axis=1)
                elif out_ref_type == RefType.RF:
                    df = df.iloc[ref.row + start_idx : ref.last_row]
                    value = (
                        df.iloc[::-1]
                        .expanding(h - end_idx)
                        .apply(function)
                        .dropna()
                        .iloc[::-1]
                        .apply(function, axis=1)
                    )
                if out_ref_type != RefType.FF and n_formula > len(value):
                    value = np.append(value, np.full(n_formula - len(value), np.nan))
            values.append(value)
        elif isinstance(child, LitExecutionNode):
            literal = function((literal, child.literal))
    if function == max:
        return DFTable(pd.DataFrame(np.max(values, initial=literal, axis=0)))
    elif function == min:
        return DFTable(pd.DataFrame(np.min(values, initial=literal, axis=0)))
    elif function == sum:
        return DFTable(pd.DataFrame(sum(values)) + literal)


def max_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, max, -math.inf)


def min_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, min, math.inf)


def count_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    pass


def sum_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return df_executor(physical_subtree, sum, 0)


def average_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    pass


def binary_df_executor(physical_subtree: FunctionExecutionNode, function: Function) -> DFTable:
    values = []
    assert len(physical_subtree.children) == 2
    for child in physical_subtree.children:
        if isinstance(child, RefExecutionNode):
            ref = child.ref
            df = child.table.df
            out_ref_type = child.out_ref_type
            start_idx = child.exec_context.start_formula_idx
            end_idx = child.exec_context.end_formula_idx
            n_formula = end_idx - start_idx
            axis = child.exec_context.axis
            if axis == 0:
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
        return DFTable(pd.DataFrame(values[0] + values[1]))
    elif function == Function.MINUS:
        return DFTable(pd.DataFrame(values[0] - values[1]))
    elif function == Function.MULTIPLY:
        return DFTable(pd.DataFrame(values[0] * values[1]))
    elif function == Function.DIVIDE:
        return DFTable(pd.DataFrame(values[0] / values[1]))


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


def find_function_executor(function: Function):
    return function_to_executor_dict[function]
