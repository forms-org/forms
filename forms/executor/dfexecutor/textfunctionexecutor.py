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
from forms.executor.dfexecutor.dftable import DFTable
from forms.executor.dfexecutor.dfexecnode import DFFuncExecNode
from forms.executor.dfexecutor.utils import construct_df_table, get_single_value
from typing import Callable
from forms.utils.exceptions import FormSException
import re
from datetime import datetime, date, timedelta


# 1 string parameter, more strings are optional
# Example usages: CONCATENATE(A1, A2, A3), CONCATENATE(A2:B7)


def concat_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) >= 2
    values = get_string_function_values(physical_subtree)

    def concat(s, to_concat):
        return s + "".join(to_concat)

    kwargs = {"to_concat": values[1:]}
    return construct_df_table(values[0].map(concat, **kwargs))


def concatenate_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) >= 2
    values = get_string_function_values(physical_subtree)

    def concatenate(s, to_concatenate):
        return s + "".join(to_concatenate)

    kwargs = {"to_concatenate": values[1:]}
    return construct_df_table(values[0].map(concatenate, **kwargs))


# 2 parameters, s1 and s2 to compare
def exact_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) == 2
    values = get_string_function_values(physical_subtree)

    return construct_df_table(values[0] == values[1])


# 2 parameters, search_for and text_to_search, optional 'starting at' 3rd parameter
def find_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) >= 2
    values = get_string_function_values(physical_subtree)

    def find(text_to_search, search_for, starting_at=0):
        return text_to_search.find(search_for, starting_at)

    if len(values) == 3:
        kwargs = {"search_for": values[0], "starting_at": values[2]}
        return construct_df_table(values[1].map(find, **kwargs))
    else:
        kwargs = {"search_for": values[0]}
        return construct_df_table(values[1].map(find, **kwargs))


# 1 parameter, string, optional number_of_characters from left 2nd parameter (default = 1)
def left_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) >= 1
    values = get_string_function_values(physical_subtree)

    def left(text, num_characters=1):
        return text[:num_characters]

    if len(values) == 2:
        kwargs = {"num_characters": values[1]}
        return construct_df_table(values[0].map(left, **kwargs))
    else:
        return construct_df_table(values[0].map(left))


# 1 parameter
def len_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: len(x))


# 1 parameter
def lower_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.lower())


# 3 parameters, string starting_at extract_length
# First character in string = index 1
# If we reach end of string before reading extract_length characters, return [starting_at:end of string]
def mid_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) == 3
    values = get_string_function_values(physical_subtree)

    def mid(text, starting_at, num_characters=1):
        return text[starting_at : min(starting_at + num_characters, len(text))]

    kwargs = {"starting_at": values[1], "num_characters": values[2]}
    return construct_df_table(values[0].map(mid, **kwargs))


# 4 parameters, text, position, length, new_text
def replace_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) == 4
    values = get_string_function_values(physical_subtree)

    def replace(text, position, length, new_text):
        return text[:position] + new_text + text[position + length :]

    kwargs = {"position": values[1], "length": values[2], "new_text": values[3]}
    return construct_df_table(values[0].map(replace, **kwargs))


# 1 parameter, string, optional number_of_characters from right 2nd parameter (default = 1)
def right_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    assert len(physical_subtree.children) >= 1
    values = get_string_function_values(physical_subtree)

    def right(text, num_characters=1):
        return text[len(text) - num_characters : len(text)]

    if len(values) == 2:
        kwargs = {"num_characters": values[1]}
        return construct_df_table(values[0].map(right, **kwargs))
    else:
        return construct_df_table(values[0].map(right))


# 1 parameter
def trim_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.strip())


# 1 parameter
def upper_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    return apply_single_value_func(physical_subtree, lambda x: x.upper())


# TODO: Handle three cases-number, date, time
# If string, check that each character is alphanumeric else remove
# If time, returns a float from 0 to 1. Eqn: # seconds in parameter number / # seconds in a day
# If date, regex for date format
def value_executor(physical_subtree: DFFuncExecNode) -> DFTable:
    def value(text):
        try:
            # Handle certain inputs, expect well-formatted inputs so we can just search for ':'
            if ":" in text:
                # Need to handle AM PM
                time = text.split(":")
                # Hours:minutes AM/PM
                if len(time) == 2:
                    if "PM" in time[1]:
                        to_minutes = 720
                        time[1] = time[1].split(" ")[0]
                    elif "AM" in time[1]:
                        to_minutes = 0
                        time[1] = time[1].split(" ")[0]
                    else:
                        to_minutes = 0
                    to_minutes += int(time[0]) * 60 + int(time[1])
                    return to_minutes / 1440
                # Hours:minutes:seconds AM/PM
                elif len(time) == 3:
                    if "PM" in time[2]:
                        to_seconds = 43200
                        time[2] = time[2].split(" ")[0]
                    elif "AM" in time[2]:
                        to_seconds = 0
                        time[2] = time[2].split(" ")[0]
                    else:
                        to_seconds = 0
                    to_seconds += int(time[0]) * 3600 + int(time[1]) * 60 + int(time[2])
                    return to_seconds / 86400
            # Handle cases: M/D/Y, Y-M-D, M D, Y
            # In Excel, 1/1/1900 = 1 -> Need to find the difference between provided date and this date.
            elif (
                "-" in text or "/" in text or re.compile(r"\w+ \d{1,2}, \d{4}").search(text) is not None
            ):
                # Can't confirm is excel uses this, but cross-checked different values
                init_date = datetime(1899, 12, 30)
                if "-" in text:
                    d = datetime.strptime(text, "%Y-%m-%d")
                    delta = d - init_date
                    return delta.days
                elif "/" in text:
                    d = datetime.strptime(text, "%m/%d/%Y")
                    delta = d - init_date
                    return delta.days
                else:
                    d = datetime.strptime(text, "%B %d, %Y")
                    delta = d - init_date
                    return delta.days
            else:
                # Don't want to remove decimal or percentage symbols. Maybe more edge cases?
                remove_special = "".join(c for c in text if c.isnumeric() or c == "." or c == "%")
                # $125.0 = 125, but $125.55 = 125.55
                if "%" in remove_special:
                    return float(remove_special.replace("%", "")) / 100
                elif "." in remove_special and not float(remove_special).is_integer():
                    return float(remove_special)
                else:
                    return int(float(remove_special))
        except Exception as e:
            print("invalid format: ", e)
            raise FormSException

    return apply_single_value_func(physical_subtree, value)


def get_string_function_value(physical_subtree: DFFuncExecNode) -> pd.DataFrame:
    assert len(physical_subtree.children) == 1
    return get_single_value(physical_subtree.children[0])


def get_string_function_values(physical_subtree: DFFuncExecNode) -> list:
    values = []
    assert len(physical_subtree.children) >= 2
    for child in physical_subtree.children:
        values.append(get_single_value(child))
    return values


def apply_single_value_func(physical_subtree: DFFuncExecNode, func: Callable) -> DFTable:
    value = get_string_function_value(physical_subtree)
    df = value.map(func)
    return construct_df_table(df)
