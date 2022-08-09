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

from enum import Enum

from forms.utils.exceptions import FunctionNotSupportedException


# Function-related definitions
class Function(Enum):
    SUM = "sum"
    COUNT = "count"
    AVG = "average"
    MIN = "min"
    MAX = "max"
    PLUS = "+"
    MINUS = "-"
    MULTIPLY = "*"
    DIVIDE = "/"


def from_function_str(function_str: str) -> Function:
    for function in Function:
        if function.value == function_str.lower():
            return function
    raise FunctionNotSupportedException(f"Function {function_str} Not Supported")


arithmetic_functions = {Function.PLUS, Function.MINUS, Function.MULTIPLY, Function.DIVIDE}


def is_arithmetic_function(function: Function):
    return function in arithmetic_functions
