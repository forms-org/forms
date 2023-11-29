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


class FormSConfig:
    def __init__(self, enable_rewriting):
        self.enable_rewriting = enable_rewriting


class DFConfig(FormSConfig):
    def __init__(self, enable_rewriting):
        super().__init__(enable_rewriting)


class DBConfig(FormSConfig):
    def __init__(self, db_url: str, enable_rewriting):
        super().__init__(enable_rewriting)
        self.db_url = db_url


class DFExecContext:
    def __init__(self, start_formula_idx: int, end_formula_idx: int, axis: int):
        self.start_formula_idx = start_formula_idx
        self.end_formula_idx = end_formula_idx
        self.axis = axis
