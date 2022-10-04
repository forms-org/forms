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
from random import randrange
from typing import Callable
from baseconvert import base

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode, LitExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)


def decimal_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(
        physical_subtree, lambda x, y: float(base(str(x), int(y), 10, string=True))
    )


def atan2_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(physical_subtree, np.arctan2)


def mod_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(physical_subtree, np.mod)


def mround_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(physical_subtree, lambda x, y: y * round(x / y))


def power_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(physical_subtree, lambda x, y: x**y)


def rand_between_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(physical_subtree, lambda x, y: randrange(x, y, 1))


# This logic is a bit messy. Essentially if one argument is a literal and the other
# is a reference, we convert the literal into a pandas DataFrame and proceed normally.
# If both arguments are literals, then we can take a shortcut and run the function on
# only the two arguments given.
def math_double_df_executor(physical_subtree: FunctionExecutionNode, func: Callable) -> DFTable:
    values = get_math_double_function_values(physical_subtree)
    first, second = values[0], values[1]
    if not isinstance(first, pd.DataFrame) and not isinstance(second, pd.DataFrame):
        return construct_df_table(pd.DataFrame([func(first, second)]))
    if not isinstance(first, pd.DataFrame):
        first = pd.DataFrame([first] * len(second))
    if not isinstance(second, pd.DataFrame):
        second = pd.DataFrame([second] * len(first))
    df = first.iloc[:, 0].combine(second.iloc[:, 0], func)
    return construct_df_table(df)


def get_math_double_function_values(physical_subtree: FunctionExecutionNode) -> list:
    values = []
    assert len(physical_subtree.children) == 2
    for child in physical_subtree.children:
        value = get_single_value(child)
        values.append(value)
    return values
