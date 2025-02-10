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
    MAXIF = "maxif"
    MINIF = "minif"

    # Text Functions
    CONCAT = "concat"
    CONCATENATE = "concatenate"
    EXACT = "exact"
    FIND = "find"
    LEFT = "left"
    LEN = "len"
    LOWER = "lower"
    MID = "mid"
    REPLACE = "replace"
    RIGHT = "right"
    TRIM = "trim"
    UPPER = "upper"
    VALUE = "value"

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
    FACTDOUBLE = "factdouble"
    INT = "int"
    ISEVEN = "iseven"
    ISODD = "isodd"
    LN = "ln"
    LOG10 = "log10"
    NEGATE = "operator-prefix_-"
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
    DECIMAL = "decimal"
    MOD = "mod"
    MROUND = "mround"
    POWER = "power"
    RANDBETWEEN = "randbetween"

    # Variable-parameter math functions
    CEILING = "ceiling"
    CEILING_MATH = "ceiling.math"
    CEILING_PRECISE = "ceiling.precise"
    FLOOR = "floor"
    FLOOR_MATH = "floor.math"
    FLOOR_PRECISE = "floor.precise"
    ROMAN = "roman"
    ROUND = "round"
    ROUNDDOWN = "rounddown"
    ROUNDUP = "roundup"
    TRUNC = "trunc"

    # Control Functions
    IF = "if"

    # Comparison Functions
    LARGER_THAN = ">"
    EQUAL = "="
    SMALLER_THAN = "<"

    # Lookup Functions
    VLOOKUP = "vlookup"
    HLOOKUP = "hlookup"
    LOOKUP = "lookup"
    MATCH = "match"
    INDEX = "index"


def from_function_str(function_str: str) -> Function:
    for function in Function:
        if function.value == function_str.lower():
            return function
    raise FunctionNotSupportedException(f"Function {function_str} Not Supported")


ARITHMETIC_FUNCTIONS = {Function.PLUS, Function.MINUS, Function.MULTIPLY, Function.DIVIDE}
DISTRIBUTIVE_FUNCTIONS = {Function.SUM, Function.MIN, Function.MAX}
COMPARISON_FUNCTIONS = {Function.LARGER_THAN, Function.EQUAL, Function.SMALLER_THAN}

PANDAS_SUPPORTED_FUNCTIONS = {
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
    # Text Functions
    Function.CONCAT,
    Function.CONCATENATE,
    Function.EXACT,
    Function.FIND,
    Function.LEFT,
    Function.LEN,
    Function.LOWER,
    Function.MID,
    Function.REPLACE,
    Function.RIGHT,
    Function.TRIM,
    Function.UPPER,
    Function.VALUE,
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
    Function.FACTDOUBLE,
    Function.INT,
    Function.ISEVEN,
    Function.ISODD,
    Function.LN,
    Function.LOG10,
    Function.NEGATE,
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
    Function.DECIMAL,
    Function.MOD,
    Function.MROUND,
    Function.POWER,
    Function.RANDBETWEEN,
    # Variable-parameter math functions
    Function.CEILING,
    Function.CEILING_MATH,
    Function.CEILING_PRECISE,
    Function.FLOOR,
    Function.FLOOR_MATH,
    Function.FLOOR_PRECISE,
    Function.ROMAN,
    Function.ROUND,
    Function.ROUNDDOWN,
    Function.ROUNDUP,
    Function.TRUNC,
}

DB_SUPPORTED_FUNCTIONS = {
    # Arithmetic Functions
    Function.PLUS,
    Function.MINUS,
    Function.MULTIPLY,
    Function.DIVIDE,
    # Aggregation Functions
    Function.SUM,
    Function.MAX,
    Function.MIN,
    Function.COUNT,
    Function.AVG,
    Function.SUMIF,
    Function.MAXIF,
    Function.MINIF,
    Function.COUNTIF,
    Function.AVERAGEIF,
    # Control Function
    Function.IF,
    # Comparison Functions
    Function.LARGER_THAN,
    Function.EQUAL,
    Function.SMALLER_THAN,
    # Lookup Functions
    Function.VLOOKUP,
    Function.HLOOKUP,
    Function.LOOKUP,
    Function.MATCH,
    Function.INDEX,
}

DB_AGGREGATE_FUNCTIONS = {Function.SUM, Function.MAX, Function.MIN, Function.COUNT, Function.AVG}
DB_AGGREGATE_IF_FUNCTIONS = {Function.SUMIF, Function.MAXIF, Function.MINIF, Function.COUNTIF, Function.AVERAGEIF}
DB_CELL_REFERENCE_FUNCTIONS = ARITHMETIC_FUNCTIONS | {Function.IF} | COMPARISON_FUNCTIONS
DB_LOOKUP_FUNCTIONS = {
    Function.VLOOKUP,
    Function.HLOOKUP,
    Function.LOOKUP,
    Function.MATCH,
    Function.INDEX,
}


# The following is for supporting the formulas executor
def from_function_to_open_value(function: Function) -> str:
    return function.value + "("


CLOSE_VALUE = ")"


class FunctionType(Enum):
    FUNC = Token.FUNC
    OP_IN = Token.OP_IN
    OP_PRE = Token.OP_PRE
    OP_POST = Token.OP_POST


class FunctionExecutor(Enum):
    DF_EXECUTOR = auto()
    DB_EXECUTOR = auto()
