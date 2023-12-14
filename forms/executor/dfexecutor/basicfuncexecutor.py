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

from forms.executor.dfexecutor.dftable import DFTable
from forms.executor.dfexecutor.dfexecnode import DFFuncExecNode, DFRefExecNode, DFLitExecNode
from forms.utils.functions import Function
from forms.utils.reference import AXIS_ALONG_ROW, RefType
from forms.executor.dfexecutor.mathfuncexecutorsingle import (
    abs_df_executor,
    acos_df_executor,
    acosh_df_executor,
    acot_df_executor,
    acoth_df_executor,
    arabic_df_executor,
    asin_df_executor,
    asinh_df_executor,
    atan_df_executor,
    atanh_df_executor,
    cos_df_executor,
    cosh_df_executor,
    cot_df_executor,
    coth_df_executor,
    csc_df_executor,
    csch_df_executor,
    degrees_df_executor,
    even_df_executor,
    exp_df_executor,
    fact_df_executor,
    fact_double_df_executor,
    int_df_executor,
    is_even_df_executor,
    is_odd_df_executor,
    ln_df_executor,
    log10_df_executor,
    negate_df_executor,
    odd_df_executor,
    radians_df_executor,
    sec_df_executor,
    sech_df_executor,
    sign_df_executor,
    sin_df_executor,
    sinh_df_executor,
    sqrt_df_executor,
    sqrt_pi_df_executor,
    tan_df_executor,
    tanh_df_executor,
)

from forms.executor.dfexecutor.mathfuncexecutordouble import (
    atan2_df_executor,
    decimal_df_executor,
    mod_df_executor,
    mround_df_executor,
    power_df_executor,
    rand_between_df_executor,
)

from forms.executor.dfexecutor.mathfuncexecutorvariable import (
    ceiling_df_executor,
    ceiling_math_df_executor,
    ceiling_precise_df_executor,
    floor_df_executor,
    floor_math_df_executor,
    floor_precise_df_executor,
    iso_ceiling_df_executor,
    roman_df_executor,
    round_df_executor,
    round_down_df_executor,
    round_up_df_executor,
    trunc_df_executor,
)


from forms.executor.dfexecutor.textfunctionexecutor import (
    concat_executor,
    concatenate_executor,
    exact_executor,
    find_executor,
    left_executor,
    len_executor,
    lower_executor,
    mid_executor,
    replace_executor,
    right_executor,
    trim_executor,
    upper_executor,
    value_executor,
)

from forms.executor.dfexecutor.utils import (
    construct_df_table,
    fill_in_nan,
    get_value_fr,
    get_value_rf,
    get_value_rr,
    get_reference_indices,
    get_single_value,
)


def max_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.MAX)


def min_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.MIN)


def count_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.COUNT)


def sum_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return distributive_function_executor(physical_subtree, Function.SUM)


def average_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return DFTable(
        df=distributive_function_executor(physical_subtree, Function.SUM).df
        / distributive_function_executor(physical_subtree, Function.COUNT).df
    )


def median_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) == 1
    child = physical_subtree.children[0]
    result = None
    if physical_subtree.out_ref_type == RefType.FF:
        num_formulas = (
            physical_subtree.exec_context.end_formula_idx
            - physical_subtree.exec_context.start_formula_idx
        )
        # all children must be either FF-type RefNode or LiteralNode
        df = child.table.get_table_content()
        start_row, start_column, end_row, end_column = get_reference_indices(child)
        result = np.median(df.iloc[start_row:end_row, start_column:end_column].values.flatten())
        # construct a one-cell dataframe table
        return construct_df_table(np.full((num_formulas, 1), result))
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
        if axis == AXIS_ALONG_ROW:
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


operator_dict = {
    "=": np.equal,
    "<": np.less,
    "<=": np.less_equal,
    ">": np.greater,
    ">=": np.greater_equal,
}


def sumif_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    ref_node = physical_subtree.children[0]
    criteria = physical_subtree.children[1]
    assert isinstance(ref_node, DFRefExecNode)
    assert isinstance(criteria, DFLitExecNode)
    literal_str = criteria.literal.replace('"', "")
    first_digit = next(i for i, c in enumerate(literal_str) if c.isdigit())
    op = literal_str[:first_digit]
    val = literal_str[first_digit:]
    val = float(val)
    assert op in operator_dict.keys()
    ref = ref_node.ref
    out_ref_type = ref_node.out_ref_type
    start_idx = ref_node.exec_context.start_formula_idx
    end_idx = ref_node.exec_context.end_formula_idx
    n_formula = end_idx - start_idx
    axis = ref_node.exec_context.axis
    if physical_subtree.out_ref_type == RefType.FF:
        df = ref_node.table.get_table_content()
        start_row, start_column, end_row, end_column = get_reference_indices(ref_node)
        value = df.iloc[start_row:end_row, start_column:end_column].values.flatten()
        # construct a one-cell dataframe table
        result = np.sum(operator_dict[op](value, val) * value)
        return construct_df_table(np.full((n_formula, 1), result))
    else:
        df = ref_node.table.get_table_content()
        start_row, start_column, end_row, end_column = get_reference_indices(ref_node)
        df = df.iloc[start_row:end_row, start_column:end_column]
        df = df[operator_dict[op](df, val)].fillna(0)
        # TODO: add support for axis_along_column
        if axis == AXIS_ALONG_ROW:
            window_size = ref.last_row - ref.row + 1
            if out_ref_type == RefType.RR:
                result = df.sum(axis=1).rolling(window_size).sum().dropna()
            elif out_ref_type == RefType.FR:
                result = df.sum(axis=1).expanding(window_size + start_idx).sum().dropna()
            elif out_ref_type == RefType.RF:
                result = (
                    df.iloc[::-1]
                    .sum(axis=1)
                    .expanding(window_size - end_idx + 1)
                    .sum()
                    .dropna()
                    .iloc[::-1]
                )
            result.index = range(result.index.size)
            result = fill_in_nan(result, n_formula)
            return construct_df_table(result)


def plus_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] + values[1])


def minus_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] - values[1])


def multiply_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] * values[1])


def divide_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    values = get_arithmetic_function_values(physical_subtree)
    return construct_df_table(values[0] / values[1])


def compute_max(values, literal):
    return np.max(values, initial=literal, axis=0)


def compute_min(values, literal):
    return np.min(values, initial=literal, axis=0)


def compute_sum(values, literal):
    return np.sum(values, initial=literal, axis=0)


def distributive_function_executor(physical_subtree: DFFuncExecNode, function: Function) -> DFTable:
    values = []
    (
        func_first_axis,
        func_second_axis,
        func_ff,
        func_literal,
        func_all,
        literal,
    ) = distributive_function_to_parameters_dict[function]
    if physical_subtree.out_ref_type == RefType.FF:
        num_formulas = (
            physical_subtree.exec_context.end_formula_idx
            - physical_subtree.exec_context.start_formula_idx
        )
        # all children must be either FF-type RefNode or LiteralNode
        for child in physical_subtree.children:
            if isinstance(child, DFRefExecNode):
                df = child.table.get_table_content()
                start_row, start_column, end_row, end_column = get_reference_indices(child)
                value = func_ff(df.iloc[start_row:end_row, start_column:end_column].values.flatten())
                values.append(value)
            elif isinstance(child, DFLitExecNode):
                literal = func_literal((literal, child.literal))
        # construct a one-cell dataframe table
        result = func_all(values, literal)
        return construct_df_table(np.full((num_formulas, 1), result))
    else:
        for child in physical_subtree.children:
            if isinstance(child, DFRefExecNode):
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
                if axis == AXIS_ALONG_ROW:
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
            elif isinstance(child, DFLitExecNode):
                literal = func_literal((literal, child.literal))
        result = func_all(values, literal)
        if not isinstance(result, np.ndarray):
            result = [result]
        return construct_df_table(result)


def get_arithmetic_function_values(physical_subtree: DFFuncExecNode) -> list:
    values = []
    assert len(physical_subtree.children) == 2
    if physical_subtree.out_ref_type == RefType.FF:
        for child in physical_subtree.children:
            if isinstance(child, DFRefExecNode):
                df = child.table.get_table_content()
                start_row, start_column, end_row, end_column = get_reference_indices(child)
                values.append(df.iloc[start_row:end_row, start_column:end_column].values.flatten())
            elif isinstance(child, DFLitExecNode):
                values.append(child.literal)
    else:
        for child in physical_subtree.children:
            values.append(get_single_value(child))
    return values


distributive_function_to_parameters_dict = {
    Function.MAX: ["max"] * 2 + [max] * 2 + [compute_max, -math.inf],
    Function.MIN: ["min"] * 2 + [min] * 2 + [compute_min, math.inf],
    Function.COUNT: ["count", "sum", np.ma.count, lambda x: x[0] + 1, compute_sum, 0],
    Function.SUM: ["sum"] * 2 + [sum] * 2 + [compute_sum, 0],
}

function_to_executor_dict = {
    # Basic functions
    Function.MAX: max_df_executor,
    Function.MIN: min_df_executor,
    Function.COUNT: count_df_executor,
    Function.AVG: average_df_executor,
    Function.MEDIAN: median_df_executor,
    Function.SUM: sum_df_executor,
    Function.SUMIF: sumif_df_executor,
    Function.PLUS: plus_df_executor,
    Function.MINUS: minus_df_executor,
    Function.MULTIPLY: multiply_df_executor,
    Function.DIVIDE: divide_df_executor,
    # Text functions
    Function.CONCAT: concat_executor,
    Function.CONCATENATE: concatenate_executor,
    Function.EXACT: exact_executor,
    Function.FIND: find_executor,
    Function.LEFT: left_executor,
    Function.LEN: len_executor,
    Function.LOWER: lower_executor,
    Function.MID: mid_executor,
    Function.REPLACE: replace_executor,
    Function.RIGHT: right_executor,
    Function.TRIM: trim_executor,
    Function.UPPER: upper_executor,
    Function.VALUE: value_executor,
    # Single-parameter math functions
    Function.ABS: abs_df_executor,
    Function.ACOS: acos_df_executor,
    Function.ACOSH: acosh_df_executor,
    Function.ACOT: acot_df_executor,
    Function.ACOTH: acoth_df_executor,
    Function.ARABIC: arabic_df_executor,
    Function.ASIN: asin_df_executor,
    Function.ASINH: asinh_df_executor,
    Function.ATAN: atan_df_executor,
    Function.ATANH: atanh_df_executor,
    Function.COS: cos_df_executor,
    Function.COSH: cosh_df_executor,
    Function.COT: cot_df_executor,
    Function.COTH: coth_df_executor,
    Function.CSC: csc_df_executor,
    Function.CSCH: csch_df_executor,
    Function.DEGREES: degrees_df_executor,
    Function.EVEN: even_df_executor,
    Function.EXP: exp_df_executor,
    Function.FACT: fact_df_executor,
    Function.FACTDOUBLE: fact_double_df_executor,
    Function.INT: int_df_executor,
    Function.ISEVEN: is_even_df_executor,
    Function.ISODD: is_odd_df_executor,
    Function.LN: ln_df_executor,
    Function.LOG10: log10_df_executor,
    Function.NEGATE: negate_df_executor,
    Function.ODD: odd_df_executor,
    Function.RADIANS: radians_df_executor,
    Function.SEC: sec_df_executor,
    Function.SECH: sech_df_executor,
    Function.SIGN: sign_df_executor,
    Function.SIN: sin_df_executor,
    Function.SINH: sinh_df_executor,
    Function.SQRT: sqrt_df_executor,
    Function.SQRTPI: sqrt_pi_df_executor,
    Function.TAN: tan_df_executor,
    Function.TANH: tanh_df_executor,
    # Double-parameter math functions
    Function.ATAN2: atan2_df_executor,
    Function.DECIMAL: decimal_df_executor,
    Function.MOD: mod_df_executor,
    Function.MROUND: mround_df_executor,
    Function.POWER: power_df_executor,
    Function.RANDBETWEEN: rand_between_df_executor,
    # Variable-parameter math functions
    Function.CEILING: ceiling_df_executor,
    Function.CEILING_MATH: ceiling_math_df_executor,
    Function.CEILING_PRECISE: ceiling_precise_df_executor,
    Function.FLOOR: floor_df_executor,
    Function.FLOOR_MATH: floor_math_df_executor,
    Function.FLOOR_PRECISE: floor_precise_df_executor,
    Function.ROMAN: roman_df_executor,
    Function.ROUND: round_df_executor,
    Function.ROUNDDOWN: round_down_df_executor,
    Function.ROUNDUP: round_up_df_executor,
    Function.TRUNC: trunc_df_executor,
}


def find_function_executor(function: Function):
    return function_to_executor_dict[function]
