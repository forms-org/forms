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
from forms.executor.executionnode import *
from forms.utils.functions import *


def max_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def min_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def count_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def sum_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def average_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def plus_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def minus_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def multiply_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


def divide_df_executor(physical_subtree: FunctionExecutionNode) -> pd.DataFrame:
    pass


function_to_executor_dict = {
    Function.MAX: max_df_executor,
    Function.MIN: min_df_executor,
    Function.COUNT: count_df_executor,
    Function.AVG: average_df_executor,
    Function.SUM: sum_df_executor,
    Function.PLUS: plus_df_executor,
    Function.MINUS: minus_df_executor,
    Function.MULTIPLY: multiply_df_executor,
    Function.DIVIDE: divide_df_executor,
}


def find_function_executor(function: Function):
    return function_to_executor_dict[function]
