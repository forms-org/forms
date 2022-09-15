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

from forms.executor.compiler import BaseCompiler
from forms.executor.executionnode import (
    ExecutionNode,
    create_intermediate_ref_node,
)
from forms.scheduler.dfscheduler.phase import create_phase_by_name
from forms.scheduler.scheduler import BaseScheduler
from forms.executor.table import Table
from forms.executor.dfexecutor.utils import remote_access_planning
from forms.executor.utils import ExecutionConfig, ExecutionContext


class DFSimpleScheduler(BaseScheduler):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        super().__init__(compiler, exec_config, execution_tree)
        self.scheduled = False

    def next_subtree(self) -> (ExecutionNode, list):
        if not self.scheduled:
            cores = self.exec_config.cores
            partition_plan = self.cost_model.get_partition_plan(self.execution_tree, cores)
            exec_subtree_list = [self.execution_tree.gen_exec_subtree() for _ in range(cores)]
            exec_context_list = [
                ExecutionContext(
                    partition_plan[i],
                    partition_plan[i + 1],
                    self.exec_config.axis,
                )
                for i in range(cores)
            ]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i] = remote_access_planning(exec_subtree_list[i])
            exec_subtree_list = [
                self.compiler.compile(exec_subtree_list[i], self.exec_config.function_executor)
                for i in range(cores)
            ]
            self.scheduled = True
            return self.execution_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)


class PrioritizedScheduler(BaseScheduler):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        super().__init__(compiler, exec_config, execution_tree)
        self.phases_name = ["ff", "rffrphaseone", "rffrphasetwo", "simple"]
        self.idx = -1
        self.next_phase()

    def next_phase(self):
        self.idx += 1
        p_name = self.phases_name[self.idx]
        phase = create_phase_by_name(p_name, self.compiler, self.exec_config, self.execution_tree)
        self.curr_phase = phase

    def next_subtree(self) -> (ExecutionNode, list):
        while self.curr_phase.empty:
            self.next_phase()
        if self.idx < len(self.phases_name):
            while not self.curr_phase.is_finished():
                return self.curr_phase.next_subtree()
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        self.curr_phase.finish_subtree(execution_subtree, result_table)
        if self.curr_phase.is_finished():
            if self.execution_tree != self.curr_phase.execution_tree:
                # replace self.execution_tree if the root is already changed
                self.execution_tree = self.curr_phase.execution_tree
            elif self.idx < len(self.phases_name) - 1:
                self.next_phase()
