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

from abc import ABC, abstractmethod

from enum import Enum, auto
from forms.executor.executionnode import ExecutionNode, RefExecutionNode, create_intermediate_ref_node
from forms.executor.table import Table
from forms.executor.utils import ExecutionConfig, ExecutionContext, axis_along_row, axis_along_column
from forms.utils.exceptions import SchedulerNotSupportedException
from forms.utils.reference import RefType
from forms.utils.treenode import link_parent_to_children


class BaseScheduler(ABC):
    def __init__(self, exec_config: ExecutionConfig, execution_tree: ExecutionNode):
        self.exec_config = exec_config
        self.execution_tree = execution_tree

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


class SimpleScheduler(BaseScheduler):
    def __init__(self, exec_config: ExecutionConfig, execution_tree: ExecutionNode):
        super().__init__(exec_config, execution_tree)
        self.scheduled = False

    def next_subtree(self) -> (ExecutionNode, list):
        if not self.scheduled:
            cores = self.exec_config.cores
            num_of_formulae = self.exec_config.num_of_formulae
            exec_subtree_list = [self.execution_tree.gen_exec_subtree() for _ in range(cores)]
            exec_context_list = [
                ExecutionContext(
                    int(i * num_of_formulae / cores),
                    int((i + 1) * num_of_formulae / cores),
                    self.exec_config.axis,
                )
                for i in range(cores)
            ]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
            self.scheduled = True
            return self.execution_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)


class RFFRTwoPhaseScheduler(BaseScheduler):
    def __init__(self, exec_config: ExecutionConfig, execution_tree: ExecutionNode):
        super().__init__(exec_config, execution_tree)
        self.phase_one_scheduled = False
        self.phase_two_scheduled = False
        self.phase_one_finished = False
        self.ref_type = execution_tree.children[0].children[0].out_ref_type
        self.partition_plan = self.partition_plan()

    def partition_plan(self):
        cores = self.exec_config.cores
        num_of_formulae = self.exec_config.num_of_formulae
        partitions = [int(i * num_of_formulae / cores) for i in range(cores)]
        partitions.append(num_of_formulae)
        return partitions

    def slice(self, subtrees):
        partition_plan = self.partition_plan
        axis = self.exec_config.axis
        for i, subtree in enumerate(subtrees):
            child_ref_node = subtree.children[0]
            df = child_ref_node.table.get_table_content()
            ref = child_ref_node.ref
            if axis == axis_along_row:
                start_row = ref.row
                end_row = ref.last_row
                if self.ref_type == RefType.FR:
                    if i == 0:
                        child_ref_node.table.df = df.iloc[start_row : partition_plan[i + 1] + end_row]
                    else:
                        child_ref_node.table.df = df.iloc[
                            partition_plan[i] + end_row : partition_plan[i + 1] + end_row
                        ]
                else:
                    if i == len(subtrees) - 1:
                        child_ref_node.table.df = df.iloc[start_row + partition_plan[i] : end_row + 1]
                    else:
                        child_ref_node.table.df = df.iloc[
                            start_row + partition_plan[i] : start_row + partition_plan[i + 1]
                        ]

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
            child = self.execution_tree.children[0]
            exec_subtree_list = [child.gen_exec_subtree() for _ in range(cores)]
            self.slice(exec_subtree_list)
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i].exec_context.set_all_formula_idx(partition_plan)
            self.phase_one_scheduled = True
            return self.execution_tree, exec_subtree_list
        elif self.phase_one_finished and not self.phase_two_scheduled:
            exec_subtree_list = [self.execution_tree.gen_exec_subtree() for _ in range(cores)]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i].exec_context.set_all_formula_idx(partition_plan)
            self.phase_two_scheduled = True
            return self.execution_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        if not self.phase_one_finished:
            self.phase_one_finished = True
            ref_node = create_intermediate_ref_node(result_table, execution_subtree)
            ref_node.out_ref_type = self.ref_type
            children = [ref_node]
            link_parent_to_children(self.execution_tree, children)
        else:
            self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)


class Schedulers(Enum):
    SIMPLE = auto()
    FRRFTWOPHASE = auto()


scheduler_class_dict = {
    Schedulers.SIMPLE.name.lower(): SimpleScheduler,
    Schedulers.FRRFTWOPHASE.name.lower(): RFFRTwoPhaseScheduler,
}


def create_scheduler_by_name(s_name: str, exec_config: ExecutionConfig, execution_tree: ExecutionNode):
    if s_name.lower() in scheduler_class_dict.keys():
        return scheduler_class_dict[s_name](exec_config, execution_tree)
    raise SchedulerNotSupportedException(f"Scheduler {s_name} is not supported")
