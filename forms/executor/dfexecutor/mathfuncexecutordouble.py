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
from typing import Callable

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)


def atan2_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return math_double_df_executor(physical_subtree, np.arctan2)


def math_double_df_executor(physical_subtree: FunctionExecutionNode, func: Callable) -> DFTable:
    values = get_math_double_function_values(physical_subtree)
    df = values[0].combine(values[1], func)
    return construct_df_table(df)


def get_math_double_function_values(physical_subtree: FunctionExecutionNode) -> list:
    values = []
    assert len(physical_subtree.children) == 2
    for child in physical_subtree.children:
        values.append(get_single_value(child))
    return values
