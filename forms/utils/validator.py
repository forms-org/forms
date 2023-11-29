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


from forms.planner.plannode import PlanNode, FunctionNode, RefNode, is_reference_range
from forms.utils.exceptions import (
    FunctionNotSupportedException,
    InvalidArithmeticInputException,
    InvalidIndexException,
)

from forms.utils.functions import FunctionExecutor
from forms.utils.functions import (
    arithmetic_functions,
    pandas_supported_functions,
    db_supported_functions,
)


def validate(function_executor: FunctionExecutor, num_rows: int, num_cols: int, plannode: PlanNode):
    if isinstance(plannode, FunctionNode):
        if plannode.function in arithmetic_functions:
            if any(is_reference_range(child) for child in plannode.children):
                raise InvalidArithmeticInputException(
                    "Not supporting range input for arithmetic functions"
                )

        if (
            function_executor == FunctionExecutor.df_executor
            and plannode.function not in pandas_supported_functions
        ):
            raise FunctionNotSupportedException(
                f"Function {plannode.function} is not supported by pandas executors"
            )

        if (
            function_executor == FunctionExecutor.db_executor
            and plannode.function not in db_supported_functions
        ):
            raise FunctionNotSupportedException(
                f"Function {plannode.function} is not supported by db executors"
            )

    elif isinstance(plannode, RefNode):
        if plannode.ref.row >= num_rows or plannode.ref.last_row >= num_rows:
            raise InvalidIndexException(
                f"Reference {plannode.construct_formula_string()} is out of bound: the max row index is {num_rows - 1}"
            )
        if plannode.ref.col >= num_cols or plannode.ref.last_col >= num_cols:
            raise InvalidIndexException(
                f"Reference {plannode.construct_formula_string()} is out of bound: the max col index is {num_cols - 1}"
            )

    for child in plannode.children:
        validate(function_executor, num_rows, num_cols, child)
