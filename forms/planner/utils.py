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
from forms.utils.exceptions import InvalidIndexException, FunctionNotSupportedException

# Reference-related definitions
INVALID_IDX = -1
RELATIVE = "r"
FIXED = "f"


class Ref:
    def __init__(self, row: int, col: int, last_row=INVALID_IDX, last_col=INVALID_IDX):
        if row <= INVALID_IDX or col <= INVALID_IDX:
            raise InvalidIndexException(f"Invalid index ({row}, {col})")
        self.row = row
        self.col = col
        if last_row <= INVALID_IDX:
            self.last_row = row
        else:
            self.last_row = last_row

        if last_col <= INVALID_IDX:
            self.last_col = col
        else:
            self.last_col = last_col

        self.is_cell = self.row == self.last_row and self.col == self.last_col


class RefType(Enum):
    RR = auto()
    RF = auto()
    FR = auto()
    FF = auto()
    LIT = auto()


class RefDirection(Enum):
    LEFT = auto()
    RIGHT = auto()
    UP = auto()
    DOWN = auto()
    NODIR = auto()


# Function-related definitions
class Function(Enum):
    SUM = "sum"
    COUNT = "count"
    AVG = "average"
    MIN = "min"
    MAX = "max"
    PLUS = "+"
    MINUS = "-"
    Multiply = "*"
    Divide = "/"


def from_function_str(function_str: str) -> Function:
    for function in Function:
        if function.value == function_str.lower():
            return function
    raise FunctionNotSupportedException(f"Function {function_str} Not Supported")


arithmetic_functions = {Function.PLUS, Function.MINUS, Function.Multiply, Function.Divide}


def is_arithmetic_function(function: Function):
    return function in arithmetic_functions
