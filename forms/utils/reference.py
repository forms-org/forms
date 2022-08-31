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
from forms.utils.exceptions import InvalidIndexException, AxisNotSupportedException

# Reference-related definitions
INVALID_IDX = -1
RELATIVE = "r"
FIXED = "f"


def compare_cells(row, col, other_row, other_col) -> int:
    if col < other_col:
        return -1
    elif col > other_col:
        return 1
    elif row < other_row:
        return -1
    elif row > other_row:
        return 1
    return 0


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

    def __eq__(self, other) -> bool:
        if not isinstance(other, Ref):
            return False
        if (
            self.row == other.row
            and self.col == other.col
            and self.last_row == other.last_row
            and self.last_col == other.last_col
        ):
            return True
        else:
            return False

    def cmp(self, other) -> int:
        res = compare_cells(self.row, self.col, other.row, other.col)
        if res != 0:
            return res
        else:
            return compare_cells(self.last_row, self.last_col, other.last_row, other.last_col)

    def get_row_or_column_count(self, axis: int) -> int:
        if axis == axis_along_row:
            return self.last_row - self.row + 1
        elif axis == axis_along_column:
            return self.last_col - self.col + 1
        raise AxisNotSupportedException(f"Axis {axis} not supported")


class RefType(Enum):
    RR = auto()
    RF = auto()
    FR = auto()
    FF = auto()
    LIT = auto()


axis_along_row = 0
axis_along_column = 1
default_axis = axis_along_row

origin = 0
origin_ref = Ref(origin, origin)
