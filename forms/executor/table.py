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
from abc import ABC, abstractmethod


class Table(ABC):
    @abstractmethod
    def get_num_of_rows(self) -> int:
        pass

    @abstractmethod
    def get_num_of_columns(self) -> int:
        pass


class DFTable(Table):
    def __init__(self, df: pd.DataFrame = None):
        self.df = df

    def get_num_of_rows(self) -> int:
        return self.df.shape[0]

    def get_num_of_columns(self) -> int:
        return self.df.shape[1]


class RelTable(Table):
    def get_num_of_rows(self) -> int:
        return -1

    def get_num_of_columns(self) -> int:
        pass
