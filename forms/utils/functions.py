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
    # Basic functions
    SUM = "sum"
    COUNT = "count"
    AVG = "average"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    PLUS = "+"
    MINUS = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    SUMIF = "sumif"
    COUNTIF = "countif"
    AVERAGEIF = "averageif"

    # Single-parameter math functions
    ABS = "abs"
    ACOS = "acos"
    ACOSH = "acosh"
    ACOT = "acot"
    ACOTH = "acoth"
    ARABIC = "arabic"
    ASIN = "asin"
    ASINH = "asinh"
    ATAN = "atan"
    ATANH = "atanh"
    COS = "cos"
    COSH = "cosh"
    COT = "cot"
    COTH = "coth"
    CSC = "csc"
    CSCH = "csch"
    DEGREES = "degrees"
    EVEN = "even"
    EXP = "exp"
    FACT = "fact"
    INT = "int"
    ISEVEN = "iseven"
    ISODD = "isodd"
    LN = "ln"
    LOG10 = "log10"
    ODD = "odd"
    RADIANS = "radians"
    SEC = "sec"
    SECH = "sech"
    SIGN = "sign"
    SIN = "sin"
    SINH = "sinh"
    SQRT = "sqrt"
    SQRTPI = "sqrtpi"
    TAN = "tan"
    TANH = "tanh"

    # Double-parameter math functions
    ATAN2 = "atan2"

    # This is a generic function for supporting formula execution based on the formulas lib
    FORMULAS = "formulas"


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
    # Basic functions
    Function.PLUS,
    Function.MINUS,
    Function.MULTIPLY,
    Function.DIVIDE,
    # Aggregation functions
    Function.SUM,
    Function.MIN,
    Function.MAX,
    Function.COUNT,
    Function.AVG,
    Function.MEDIAN,
    Function.SUMIF,
    # Single-parameter math functions
    Function.ABS,
    Function.ACOS,
    Function.ACOSH,
    Function.ACOT,
    Function.ACOTH,
    Function.ARABIC,
    Function.ASIN,
    Function.ASINH,
    Function.ATAN,
    Function.ATANH,
    Function.COS,
    Function.COSH,
    Function.COT,
    Function.COTH,
    Function.CSC,
    Function.CSCH,
    Function.DEGREES,
    Function.EVEN,
    Function.EXP,
    Function.FACT,
    Function.INT,
    Function.ISEVEN,
    Function.ISODD,
    Function.LN,
    Function.LOG10,
    Function.ODD,
    Function.RADIANS,
    Function.SEC,
    Function.SECH,
    Function.SIGN,
    Function.SIN,
    Function.SINH,
    Function.SQRT,
    Function.SQRTPI,
    Function.TAN,
    Function.TANH,
    # Double-parameter math functions
    Function.ATAN2,
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
