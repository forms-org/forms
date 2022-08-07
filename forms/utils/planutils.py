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

from forms.utils.exceptions import InvalidIndexException

INVALID_IDX = -1


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
