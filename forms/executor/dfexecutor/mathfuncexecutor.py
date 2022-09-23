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
from typing import Callable

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)


def abs_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, abs)


def acos_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.acos)


def acosh_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.acosh)


def asin_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.asin)


def asinh_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.asinh)


def atan_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.atan)


def atanh_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.atanh)


def cos_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.cos)


def cosh_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.cosh)


def degrees_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.degrees)


def exp_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.exp)


def fact_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.factorial)


def int_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.floor)


def is_even_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, lambda x: x % 2 == 0)


def is_odd_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, lambda x: x % 2 == 1)


def ln_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.log)


def log10_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.log10)


def radians_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.radians)


def sign_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    def sign(x):
        if x > 0:
            return 1
        elif x < 0:
            return -1
        return 0

    return math_df_executor(physical_subtree, sign)


def sin_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.sin)


def sinh_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.sinh)


def sqrt_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.sqrt)


def sqrt_pi_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, lambda x: math.sqrt(math.pi * x))


def tan_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.tan)


def tanh_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_df_executor(physical_subtree, math.tanh)


def math_df_executor(physical_subtree: FunctionExecutionNode, func: Callable) -> DFTable:
    value = get_math_single_function_values(physical_subtree)
    df = value.applymap(func)
    return construct_df_table(df)


def get_math_single_function_values(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    assert len(physical_subtree.children) == 1
    child = physical_subtree.children[0]
    # Can probably abstract this into a util function taking in a parameter CHILD and returning VALUE
    value = get_single_value(child)
    return value
