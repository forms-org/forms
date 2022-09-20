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

from enum import Enum, auto

from forms.utils.exceptions import FunctionNotSupportedException
from openpyxl.formula.tokenizer import Token


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
    SUMIF = "sumif"
    COUNTIF = "countif"
    AVERAGEIF = "averageif"
    ISODD = "isodd"
    SIN = "sin"
    FORMULAS = "formulas"  # This is a generic function for supporting formula execution based on the formulas lib


def from_function_str(function_str: str) -> Function:
    for function in Function:
        if function.value == function_str.lower():
            return function
    raise FunctionNotSupportedException(f"Function {function_str} Not Supported")


arithmetic_functions = {Function.PLUS, Function.MINUS, Function.MULTIPLY, Function.DIVIDE}
distributive_functions = {Function.SUM, Function.MIN, Function.MAX, Function.COUNT}
distributive_functions_if = {Function.SUMIF}
algebraic_functions = {Function.AVG}
algebraic_functions_if = {}

pandas_supported_functions = {
    Function.PLUS,
    Function.MINUS,
    Function.MULTIPLY,
    Function.DIVIDE,
    Function.SUM,
    Function.MIN,
    Function.MAX,
    Function.COUNT,
    Function.AVG,
    Function.ISODD,
    Function.SIN,
}

formulas_unsupported_functions = set()
formulas_supported_functions = set(Function) - formulas_unsupported_functions


# The following is for supporting the formulas executor
def from_function_to_open_value(function: Function) -> str:
    return function.value + "("


close_value = ")"


class FunctionType(Enum):
    FUNC = Token.FUNC
    OP_IN = Token.OP_IN
    OP_PRE = Token.OP_PRE
    OP_POST = Token.OP_POST


class FunctionExecutor(Enum):
    df_pandas_executor = auto()
    df_formulas_executor = auto()
    df_mixed_executor = auto()
