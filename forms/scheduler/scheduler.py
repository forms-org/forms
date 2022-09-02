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

from forms.core.config import forms_config
from forms.executor.compiler import BaseCompiler
from forms.executor.costmodel import create_cost_model_by_name
from forms.executor.executionnode import ExecutionNode, RefExecutionNode
from forms.executor.table import Table
from forms.executor.utils import ExecutionConfig


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
        assert isinstance(self.execution_tree, RefExecutionNode)
        return self.execution_tree.table
