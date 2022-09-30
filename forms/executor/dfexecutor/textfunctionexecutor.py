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


# 1 string parameter, more strings are optional
# Example usages: CONCATENATE(A1, A2, A3), CONCATENATE(A2:B7)

# TODO
def concat_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) >= 2
    values = get_string_function_values(physical_subtree)

    def concat(s, to_concat):
        return s + "".join(to_concat)

    kwargs = {"to_concat": values[1:]}
    return construct_df_table(values[0].applymap(concat, **kwargs))


def concatenate_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) >= 2
    values = get_string_function_values(physical_subtree)

    def concatenate(s, to_concatenate):
        return s + "".join(to_concatenate)

    kwargs = {"to_concat": values[1:]}
    return construct_df_table(values[0].applymap(concatenate, **kwargs))


# 2 parameters, s1 and s2 to compare
def exact_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_string_function_values(physical_subtree)

    return construct_df_table(values[0] == values[1])


# 2 parameters, search_for and text_to_search, optional 'starting at' 3rd parameter
def find_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) >= 2
    values = get_string_function_values(physical_subtree)

    def find(text_to_search, search_for, starting_at=0):
        return text_to_search.find(search_for, starting_at)

    if len(values) == 3:
        kwargs = {"search_for": values[0], "starting_at": values[2]}
        return construct_df_table(values[1].applymap(find, **kwargs))
    else:
        kwargs = {"search_for": values[0]}
        return construct_df_table(values[1].applymap(find, **kwargs))


# 1 parameter, string, optional number_of_characters from left 2nd parameter (default = 1)
def left_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) >= 1
    values = get_string_function_values(physical_subtree)

    def left(text, num_characters=1):
        return text[:num_characters]

    if len(values) == 2:
        kwargs = {"num_characters": values[1]}
        return construct_df_table(values[0].applymap(left, **kwargs))
    else:
        return construct_df_table(values[0].applymap(left))


# 1 parameter
def len_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: len(x))


# 1 parameter
def lower_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.lower())


# 3 parameters, string starting_at extract_length
# First character in string = index 1
# If we reach end of string before reading extract_length characters, return [starting_at:end of string]
def mid_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 3
    values = get_string_function_values(physical_subtree)

    def mid(text, starting_at, num_characters=1):
        return text[starting_at : min(starting_at + num_characters, len(text))]

    kwargs = {"starting_at": values[1], "num_characters": values[2]}
    return construct_df_table(values[0].applymap(mid, **kwargs))


# 4 parameters, text, position, length, new_text
def replace_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) == 4
    values = get_string_function_values(physical_subtree)

    def replace(text, position, length, new_text):
        return text[:position] + new_text + text[position + length :]

    kwargs = {"position": values[1], "length": values[2], "new_text": values[3]}
    return construct_df_table(values[0].applymap(replace, **kwargs))


# 1 parameter, string, optional number_of_characters from right 2nd parameter (default = 1)
def right_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    assert len(physical_subtree.children) >= 1
    values = get_string_function_values(physical_subtree)

    def right(text, num_characters=1):
        return text[len(text) - num_characters : len(text)]

    if len(values) == 2:
        kwargs = {"num_characters": values[1]}
        return construct_df_table(values[0].applymap(right, **kwargs))
    else:
        return construct_df_table(values[0].applymap(right))


# 1 parameter
def trim_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.strip())


# 1 parameter
def upper_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.upper())


# TODO
def value_executor(physical_subtree: FunctionExecutionNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.upper())


def get_string_function_value(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    assert len(physical_subtree.children) == 1
    return get_single_value(physical_subtree.children[0])


def get_string_function_values(physical_subtree: FunctionExecutionNode) -> list:
    values = []
    assert len(physical_subtree.children) >= 2
    for child in physical_subtree.children:
        values.append(get_single_value(child))
    return values


def apply_single_value_func(physical_subtree: FunctionExecutionNode, func: Callable) -> DFTable:
    value = get_string_function_value(physical_subtree)
    df = value.applymap(func)
    return construct_df_table(df)
