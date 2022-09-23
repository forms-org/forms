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

from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
)


def is_odd_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    value = get_math_single_function_values(physical_subtree)
    df = value.applymap(lambda x: x % 2 == 1)
    return construct_df_table(df)


def sin_df_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    value = get_math_single_function_values(physical_subtree)
    df = value.applymap(math.sin)
    return construct_df_table(df)


def get_math_single_function_values(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    assert len(physical_subtree.children) == 1
    child = physical_subtree.children[0]
    # Can probably abstract this into a util function taking in a parameter CHILD and returning VALUE
    value = get_single_value(child)
    return value
