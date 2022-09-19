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
from forms.core.config import forms_config


class ExecutionConfig:
    def __init__(self, axis: int, function_executor, num_of_formulae: int, cores: int = 1):
        self.cores = cores
        self.axis = axis
        self.num_of_formulae = num_of_formulae
        self.function_executor = function_executor


class ExecutionContext:
    def __init__(self, start_formula_idx: int, end_formula_idx: int, axis: int):
        self.start_formula_idx = start_formula_idx
        self.end_formula_idx = end_formula_idx
        self.axis = axis
        self.all_formula_idx = None
        self.compiled_formula_func = None
        self.function_executor = None
        self.enable_communication_opt = forms_config.enable_communication_opt

    def set_all_formula_idx(self, all_formula_idx: list):
        """
        all_formula_idx: a list of index for all partitions (will be used in phase-two optimization)
        Example: [0, 25, 50, 75, 100]
        """
        self.all_formula_idx = all_formula_idx
