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

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from forms.core.config import forms_config
from forms.core.globals import forms_global
from forms.executor.compiler import BaseCompiler
from forms.executor.costmodel import create_cost_model_by_name
from forms.executor.executionnode import ExecutionNode, RefExecutionNode
from forms.executor.table import Table, DFTable
from forms.executor.utils import ExecutionConfig
from forms.utils.reference import RefType, axis_along_row


class BaseScheduler(ABC):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        self.compiler = compiler
        self.exec_config = exec_config
        self.execution_tree = execution_tree
        self.cost_model = create_cost_model_by_name(forms_config.cost_model, exec_config.num_of_formulae)

    @abstractmethod
    def next_subtree(self) -> (ExecutionNode, list):
        pass

    @abstractmethod
    def finish_subtree(self, execution_subtree: ExecutionNode, result_node: RefExecutionNode):
        pass

    def is_finished(self) -> bool:
        return isinstance(self.execution_tree, RefExecutionNode)

    def get_results(self) -> Table:
        forms_global.put_one_metric("planning_time", self.cost_model.time_cost)
        assert isinstance(self.execution_tree, RefExecutionNode)
        table = self.execution_tree.table
        if self.execution_tree.out_ref_type == RefType.FF:
            # construct a table with the same value
            num_of_formulae = self.exec_config.num_of_formulae
            shape = (
                (num_of_formulae, 1) if self.exec_config.axis == axis_along_row else (1, num_of_formulae)
            )
            table = DFTable(df=pd.DataFrame(np.full(shape, table.get_table_content().iloc[0, 0])))
        return table
