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
from forms.executor.compiler import BaseCompiler
from forms.executor.executionnode import ExecutionNode, create_intermediate_ref_node
from forms.scheduler.scheduler import BaseScheduler
from forms.executor.table import Table
from forms.executor.dfexecutor.utils import remote_access_planning
from forms.executor.utils import ExecutionConfig, ExecutionContext
from forms.utils.treenode import link_parent_to_children


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


class RFFRTwoPhaseScheduler(BaseScheduler):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        super().__init__(compiler, exec_config, execution_tree)
        self.phase_one_scheduled = False
        self.phase_two_scheduled = False
        self.phase_one_finished = False
        self.ref_type = execution_tree.children[0].children[0].out_ref_type
        self.partition_plan = self.cost_model.get_partition_plan(
            self.execution_tree, self.exec_config.cores
        )

    def next_subtree(self) -> (ExecutionNode, list):
        cores = self.exec_config.cores
        partition_plan = self.partition_plan
        exec_context_list = [
            ExecutionContext(
                partition_plan[i],
                partition_plan[i + 1],
                self.exec_config.axis,
            )
            for i in range(cores)
        ]
        if not self.phase_one_scheduled:
            # create subtrees from child node (PHASEONE node) and slice table for each subtree
            child = self.execution_tree.children[0]
            exec_subtree_list = [child.gen_exec_subtree() for _ in range(cores)]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i].exec_context.set_all_formula_idx(partition_plan)
            self.phase_one_scheduled = True
            return self.execution_tree, exec_subtree_list
        elif self.phase_one_finished and not self.phase_two_scheduled:
            # create subtrees from PHASETWO node such that each tree has the whole PHASEONE result table
            exec_subtree_list = [self.execution_tree.gen_exec_subtree() for _ in range(cores)]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i].exec_context.set_all_formula_idx(partition_plan)
            self.phase_two_scheduled = True
            return self.execution_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        if not self.phase_one_finished:
            # link PHASEONE result ref_node with self.execution_tree
            self.phase_one_finished = True
            ref_node = create_intermediate_ref_node(result_table, execution_subtree)
            ref_node.out_ref_type = self.ref_type
            children = [ref_node]
            link_parent_to_children(self.execution_tree, children)
        else:
            self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)
