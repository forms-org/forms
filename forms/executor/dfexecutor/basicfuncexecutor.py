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
from forms.executor.dfexecutor.mathfuncexecutor import (
    abs_df_executor,
    acos_df_executor,
    acosh_df_executor,
    asin_df_executor,
    asinh_df_executor,
    atan_df_executor,
    atanh_df_executor,
    cos_df_executor,
    cosh_df_executor,
    degrees_df_executor,
    exp_df_executor,
    fact_df_executor,
    int_df_executor,
    is_even_df_executor,
    is_odd_df_executor,
    ln_df_executor,
    log10_df_executor,
    radians_df_executor,
    sign_df_executor,
    sin_df_executor,
    sinh_df_executor,
    sqrt_df_executor,
    sqrt_pi_df_executor,
    tan_df_executor,
    tanh_df_executor,
)

from forms.executor.dfexecutor.utils import (
    construct_df_table,
    fill_in_nan,
    get_value_fr,
    get_value_rf,
    get_value_rr,
    get_reference_indices,
    get_single_value,
    get_reference_indices_2_phase_rf_fr,
)


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


def median_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 1
    child = physical_subtree.children[0]
    result = None
    if physical_subtree.out_ref_type == RefType.FF:
        # all children must be either FF-type RefNode or LiteralNode
        df = child.table.get_table_content()
        start_row, start_column, end_row, end_column = get_reference_indices(child)
        result = np.median(df.iloc[start_row:end_row, start_column:end_column].values.flatten())
        # construct a one-cell dataframe table
        return construct_df_table([result])
    else:
        ref = child.ref
        df = child.table.get_table_content()
        out_ref_type = child.out_ref_type
        start_idx = child.exec_context.start_formula_idx
        end_idx = child.exec_context.end_formula_idx
        n_formula = end_idx - start_idx
        axis = child.exec_context.axis
        start_row, start_column, end_row, end_column = get_reference_indices(child)
        df = df.iloc[start_row:end_row, start_column:end_column]
        # TODO: add support for axis_along_column
        if axis == axis_along_row:
            window_size = ref.last_row - ref.row + 1
            step = df.shape[1]
            if out_ref_type == RefType.RR:
                new_df = pd.concat([pd.Series([np.NaN]), df.stack()])
                window_size = window_size * step
                result = new_df.rolling(window_size, step=step).median().dropna()
            elif out_ref_type == RefType.FR:
                window_size = (window_size + start_idx) * step
                result = df.stack().expanding(window_size).median().dropna()[::step]
            elif out_ref_type == RefType.RF:
                window_size = (window_size - end_idx + 1) * step
                result = (
                    df.stack().iloc[::-1].expanding(window_size).median().dropna()[::step].iloc[::-1]
                )
            result.index = range(result.index.size)
            result = fill_in_nan(result, n_formula)
            return construct_df_table(result)


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


def compute_max(values, literal):
    return np.max(values, initial=literal, axis=0)


def compute_min(values, literal):
    return np.min(values, initial=literal, axis=0)


def compute_sum(values, literal):
    return np.sum(values, initial=literal, axis=0)


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
                    df = child.table.get_table_content()
                    start_row, start_column, end_row, end_column = get_reference_indices(child)
                    value = func_ff(df.iloc[start_row:end_row, start_column:end_column].values.flatten())
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
                    start_row, start_column, end_row, end_column = get_reference_indices(child)
                    df = df.iloc[start_row:end_row, start_column:end_column]
                    value = None
                    # TODO: add support for axis_along_column
                    if axis == axis_along_row:
                        window_size = ref.last_row - ref.row + 1
                        if out_ref_type == RefType.RR:
                            value = get_value_rr(df, window_size, func_first_axis, func_second_axis)
                        elif out_ref_type == RefType.FF:
                            # treat FF-type as literal value
                            single_value = func_ff(df.values.flatten())
                            literal = func_literal((literal, single_value))
                        elif out_ref_type == RefType.FR:
                            value = get_value_fr(
                                df, window_size + start_idx, func_first_axis, func_second_axis
                            )
                        elif out_ref_type == RefType.RF:
                            value = get_value_rf(
                                df, window_size - end_idx + 1, func_first_axis, func_second_axis
                            )
                        if out_ref_type != RefType.FF:
                            value.index = range(value.index.size)
                            value = fill_in_nan(value, n_formula)
                    if value is not None:
                        values.append(pd.DataFrame(value))
                elif isinstance(child, LitExecutionNode):
                    literal = func_literal((literal, child.literal))
            result = func_all(values, literal)
            if not isinstance(result, np.ndarray):
                result = [result]
            return construct_df_table(result)
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
                start_row, start_column, end_row, end_column = get_reference_indices_2_phase_rf_fr(child)
                df = df.iloc[start_row:end_row, start_column:end_column]
                h = ref.last_row - ref.row + 1
                if out_ref_type == RefType.FR:
                    value = get_value_fr(
                        df, h if start_idx == 0 else 1, func_first_axis, func_second_axis
                    )
                else:  # RefType.RF
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
                df = child.table.get_table_content()
                start_row, start_column, end_row, end_column = get_reference_indices(child)
                values.append(df.iloc[start_row:end_row, start_column:end_column].values.flatten())
            elif isinstance(child, LitExecutionNode):
                values.append(child.literal)
    else:
        for child in physical_subtree.children:
            values.append(get_single_value(child))
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
    Function.MEDIAN: median_df_executor,
    Function.SUM: sum_df_executor,
    Function.PLUS: plus_df_executor,
    Function.MINUS: minus_df_executor,
    Function.MULTIPLY: multiply_df_executor,
    Function.DIVIDE: divide_df_executor,
    Function.ABS: abs_df_executor,
    Function.ACOS: acos_df_executor,
    Function.ACOSH: acosh_df_executor,
    Function.ASIN: asin_df_executor,
    Function.ASINH: asinh_df_executor,
    Function.ATAN: atan_df_executor,
    Function.ATANH: atanh_df_executor,
    Function.COS: cos_df_executor,
    Function.COSH: cosh_df_executor,
    Function.DEGREES: degrees_df_executor,
    Function.EXP: exp_df_executor,
    Function.FACT: fact_df_executor,
    Function.INT: int_df_executor,
    Function.ISEVEN: is_even_df_executor,
    Function.ISODD: is_odd_df_executor,
    Function.LN: ln_df_executor,
    Function.LOG10: log10_df_executor,
    Function.RADIANS: radians_df_executor,
    Function.SIGN: sign_df_executor,
    Function.SIN: sin_df_executor,
    Function.SINH: sinh_df_executor,
    Function.SQRT: sqrt_df_executor,
    Function.SQRTPI: sqrt_pi_df_executor,
    Function.TAN: tan_df_executor,
    Function.TANH: tanh_df_executor,
    Function.FORMULAS: formulas_executor,
}


def find_function_executor(function: Function):
    return function_to_executor_dict[function]
