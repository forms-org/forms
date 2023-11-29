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
import numpy
import pandas as pd
from typing import Callable
import roman

from forms.executor.dfexecutor.dftable import DFTable
from forms.executor.dfexecutor.dfexecnode import DFFuncExecNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)
from forms.executor.dfexecutor.mathfuncexecutorsingle import math_single_df_executor
from forms.executor.dfexecutor.mathfuncexecutordouble import math_double_df_executor
from forms.utils.exceptions import FunctionNotSupportedException


def ceiling_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def ceiling(x, y=1):
        return math.ceil(x / y) * y

    return math_variable_df_executor(physical_subtree, ceiling)


def ceiling_math_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def ceiling_math(x, y=1, mode=0):
        if mode == 0:
            return math.ceil(x / y) * y
        sign = 1 if x > 0 else -1
        return sign * math.ceil(abs(x) / y) * y

    return math_variable_df_executor(physical_subtree, ceiling_math)


def ceiling_precise_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def ceiling_precise(x, y=1):
        y = abs(y)
        return math.ceil(x / y) * y

    return math_variable_df_executor(physical_subtree, ceiling_precise)


def floor_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def floor(x, y=1):
        return math.floor(x / y) * y

    return math_variable_df_executor(physical_subtree, floor)


def floor_math_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def floor_math(x, y=1, mode=0):
        if mode == 0:
            return math.floor(x / y) * y
        sign = 1 if x > 0 else -1
        return sign * math.floor(abs(x) / y) * y

    return math_variable_df_executor(physical_subtree, floor_math)


def floor_precise_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def floor_precise(x, y=1):
        y = abs(y)
        return math.floor(x / y) * y

    return math_variable_df_executor(physical_subtree, floor_precise)


# ISO.CEILING seems to be identical to CEILING.PRECISE.
def iso_ceiling_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return ceiling_precise_df_executor(physical_subtree)


# This operator has several relaxations that make it extremely difficult to implement.
# Since the relaxations are likely not to be used, we will hold off on implementing them for now.
# Thus, this can be treated as a single parameter function
def roman_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return math_single_df_executor(physical_subtree, roman.toRoman)


def round_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def round_to_place(x, y=0):
        return round(x * (10**y)) / (10**y)

    return math_variable_df_executor(physical_subtree, round_to_place)


def round_down_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def round_down(x, y=0):
        return math.floor(x * (10**y)) / (10**y)

    return math_variable_df_executor(physical_subtree, round_down)


def round_up_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def round_up(x, y=0):
        return math.ceil(x * (10**y)) / (10**y)

    return math_variable_df_executor(physical_subtree, round_up)


def trunc_df_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def trunc(number, digits=0):
        nb_decimals = len(str(number).split(".")[1])
        if nb_decimals <= digits:
            return number
        stepper = 10.0**digits
        return math.trunc(stepper * number) / stepper

    return math_variable_df_executor(physical_subtree, trunc)


def math_variable_df_executor(physical_subtree: DFFuncExecNode, func: Callable):
    num_children = len(physical_subtree.children)
    if num_children == 1:
        return math_single_df_executor(physical_subtree, func)
    if num_children == 2:
        return math_double_df_executor(physical_subtree, func)
    if num_children == 3:
        return math_triple_df_executor(physical_subtree, func)
    raise FunctionNotSupportedException("Function has incorrect number of parameters!")


def math_triple_df_executor(physical_subtree: DFFuncExecNode, func: Callable) -> DFTable:
    values = get_math_triple_function_values(physical_subtree)
    first, second, third = values[0], values[1], values[2]

    num_rows = 1
    is_first_literal, is_second_literal, is_third_literal = True, True, True
    if isinstance(first, pd.DataFrame):
        is_first_literal = False
        num_rows = len(first)
    if isinstance(second, pd.DataFrame):
        is_second_literal = False
        num_rows = len(second)
    if isinstance(third, pd.DataFrame):
        is_third_literal = False
        num_rows = len(third)

    if is_first_literal and is_second_literal and is_third_literal:
        num_formulas = (
            physical_subtree.exec_context.end_formula_idx
            - physical_subtree.exec_context.start_formula_idx
        )
        return construct_df_table(numpy.full(num_formulas, func(first, second, third)))

    if is_first_literal:
        first = pd.DataFrame([first] * num_rows)
    if is_second_literal:
        second = pd.DataFrame([second] * num_rows)
    if is_third_literal:
        third = pd.DataFrame([third] * num_rows)

    result_list = [0] * num_rows
    for i in range(num_rows):
        result_list[i] = func(first[0][i], second[0][i], third[0][i])
    df = pd.DataFrame(result_list)
    return construct_df_table(df)


def get_math_triple_function_values(physical_subtree: DFFuncExecNode) -> list:
    values = []
    assert len(physical_subtree.children) == 3
    for child in physical_subtree.children:
        values.append(get_single_value(child))
    return values
