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

# execute_formula_plan executes plan on formula table
# Need to add function to forms.utils.functions.Function and forms/utils/functions.pandas_supported_functions
# and dfexecutor/basicfuncexecutor.py/function_to_executor_dict
import math

import pandas as pd
from forms.utils.reference import RefType
from forms.executor.table import DFTable
from forms.executor.executionnode import FunctionExecutionNode, RefExecutionNode, LitExecutionNode
from forms.executor.dfexecutor.utils import (
    construct_df_table,
    get_single_value,
    get_reference_indices,
    fill_in_nan,
    get_value_rr,
)
from typing import Callable
from forms.utils.exceptions import FormSException
from openpyxl.formula import Tokenizer


# We want to support all text and math functions supported by FormS
def if_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 3
    # This will get the literal values of all nodes in the tree
    values = get_logical_function_values(physical_subtree)

    def execute_if(exp, true_value, false_value=None):
        return true_value if exp.iloc[:, 0].all() else false_value

    kwargs = {"true_value": values[1], "false_value": values[2]}
    return execute_if(values[0], values[1], values[2])


# Return the value of the first true condition.
# If all are false, throw an error
def ifs_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    values = get_logical_function_values(physical_subtree)
    values = iter(values)
    for exp in values:
        t = next(values)
        # exp will always be a DataFrame
        if exp.iloc[:, 0].all():
            return t
    raise FormSException


# Handles any type of input, basically a super of exact string function
def equal_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_logical_function_values(physical_subtree)
    return construct_df_table(values[0] == values[1])


def greater_than_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_logical_function_values(physical_subtree)
    return construct_df_table(values[0] > values[1])


def greater_than_equal_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_logical_function_values(physical_subtree)
    return construct_df_table(values[0] >= values[1])


def less_than_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_logical_function_values(physical_subtree)
    return construct_df_table(values[0] < values[1])


def less_than_equal_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_logical_function_values(physical_subtree)
    return construct_df_table(values[0] <= values[1])


def get_logical_function_value(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    assert len(physical_subtree.children) == 1
    return get_single_value(physical_subtree.children[0])


def get_logical_function_values(physical_subtree: FunctionExecutionNode) -> list:
    values = []
    assert len(physical_subtree.children) >= 2
    print(physical_subtree.children)
    for child in physical_subtree.children:
        values.append(get_single_value(child))
    return values


def apply_single_value_func(physical_subtree: FunctionExecutionNode, func: Callable) -> DFTable:
    value = get_logical_function_value(physical_subtree)
    df = value.applymap(func)
    return construct_df_table(df)
