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

from forms.core.config import forms_config
from forms.executor.compiler import BaseCompiler
from forms.executor.costmodel import create_cost_model_by_name
from forms.executor.dfexecutor.utils import remote_access_planning
from forms.executor.executionnode import (
    ExecutionNode,
    create_intermediate_ref_node,
    FunctionExecutionNode,
)
from forms.executor.table import Table
from forms.executor.utils import ExecutionConfig, ExecutionContext
from forms.utils.exceptions import PhaseNotSupportedException
from forms.utils.optimizations import FRRFOptimization
from forms.utils.reference import RefType
from forms.utils.treenode import link_parent_to_children


class BasePhase:
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        self.compiler = compiler
        self.exec_config = exec_config
        self.execution_tree = execution_tree
        self.cost_model = create_cost_model_by_name(forms_config.cost_model, exec_config.num_of_formulae)
        self.partition_plan = self.cost_model.get_partition_plan(
            self.execution_tree, self.exec_config.cores
        )
        self.phase_scheduled = False
        self.phase_finished = False

    def next_subtree(self):
        pass

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        pass

    def is_finished(self):
        return self.phase_finished


class SimplePhase(BasePhase):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        super().__init__(compiler, exec_config, execution_tree)
        self.empty = False

    def next_subtree(self):
        if not self.phase_scheduled:
            cores = self.exec_config.cores
            partition_plan = self.partition_plan
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
            self.phase_scheduled = True
            return self.execution_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)
        self.phase_finished = True


class FFPhase(BasePhase):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        super().__init__(compiler, exec_config, execution_tree)
        self.ff_trees = []
        self.subtrees = []
        self.find_ff_subtrees(self.execution_tree)
        self.scheduled_idx = 0
        self.finished_idx = 0
        self.empty = len(self.ff_trees) == 0

    def find_ff_subtrees(self, execution_tree):
        if (
            isinstance(execution_tree, FunctionExecutionNode)
            and execution_tree.out_ref_type == RefType.FF
        ):
            self.subtrees.append(execution_tree.gen_exec_subtree())
            self.ff_trees.append(execution_tree)
        elif execution_tree.children:
            for child in execution_tree.children:
                self.find_ff_subtrees(child)

    def next_subtree(self):
        if not self.phase_scheduled:
            curr_idx = self.scheduled_idx
            subtree = self.subtrees[curr_idx]
            subtree.set_exec_context(
                ExecutionContext(
                    0,
                    self.exec_config.num_of_formulae,
                    self.exec_config.axis,
                )
            )
            subtree = remote_access_planning(subtree)
            self.scheduled_idx += 1
            if self.scheduled_idx == len(self.ff_trees):
                self.phase_scheduled = True
            return self.ff_trees[curr_idx], [self.subtrees[curr_idx]]
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        ref_node = create_intermediate_ref_node(result_table, execution_subtree)
        parent = execution_subtree.parent
        if parent is not None:
            children = []
            for child in parent.children:
                if execution_subtree == child:
                    children.append(ref_node)
                else:
                    children.append(child)
            link_parent_to_children(parent, children)
        else:
            self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)
        self.finished_idx += 1
        if self.finished_idx == len(self.ff_trees):
            self.phase_finished = True


class RFFRPhaseOne(BasePhase):
    def __init__(self, compiler, exec_config, execution_tree):
        super().__init__(compiler, exec_config, execution_tree)
        self.subtrees = []
        self.phaseone_trees = []
        self.find_phase_one_subtrees(self.execution_tree)
        self.idx = 0
        self.empty = len(self.phaseone_trees) == 0

    def find_phase_one_subtrees(self, execution_tree):
        if (
            isinstance(execution_tree, FunctionExecutionNode)
            and execution_tree.fr_rf_optimization == FRRFOptimization.PHASEONE
        ):
            self.subtrees.append(execution_tree.gen_exec_subtree())
            self.phaseone_trees.append(execution_tree)
        elif execution_tree.children:
            for child in execution_tree.children:
                self.find_phase_one_subtrees(child)

    def next_subtree(self):
        if not self.phase_scheduled:
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
            phase_one_tree = self.phaseone_trees[self.idx]
            subtree = self.subtrees[self.idx]
            exec_subtree_list = [subtree.gen_exec_subtree() for _ in range(cores)]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i].exec_context.set_all_formula_idx(partition_plan)
                exec_subtree_list[i] = remote_access_planning(exec_subtree_list[i])
            exec_subtree_list = [
                self.compiler.compile(exec_subtree_list[i], self.exec_config.function_executor)
                for i in range(cores)
            ]
            self.phase_scheduled = True
            return phase_one_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        ref_node = create_intermediate_ref_node(result_table, execution_subtree)
        ref_node.out_ref_type = self.phaseone_trees[self.idx].children[0].out_ref_type
        parent = execution_subtree.parent
        if parent is not None:
            children = []
            for child in parent.children:
                if execution_subtree == child:
                    children.append(ref_node)
                else:
                    children.append(child)
            link_parent_to_children(parent, children)
        self.idx += 1
        if self.idx == len(self.phaseone_trees):
            self.phase_finished = True
        else:
            self.phase_scheduled = False


class RFFRPhaseTwo(BasePhase):
    def __init__(
        self, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
    ):
        super().__init__(compiler, exec_config, execution_tree)
        self.subtrees = []
        self.phasetwo_trees = []
        self.find_phase_two_subtrees(self.execution_tree)
        self.idx = 0
        self.empty = len(self.phasetwo_trees) == 0

    def find_phase_two_subtrees(self, execution_tree):
        if (
            isinstance(execution_tree, FunctionExecutionNode)
            and execution_tree.fr_rf_optimization == FRRFOptimization.PHASETWO
        ):
            self.subtrees.append(execution_tree.gen_exec_subtree())
            self.phasetwo_trees.append(execution_tree)
        elif execution_tree.children:
            for child in execution_tree.children:
                self.find_phase_two_subtrees(child)

    def next_subtree(self):
        if not self.phase_scheduled:
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
            phasetwo_tree = self.phasetwo_trees[self.idx]
            subtree = self.subtrees[self.idx]
            exec_subtree_list = [subtree.gen_exec_subtree() for _ in range(cores)]
            for i in range(cores):
                exec_subtree_list[i].set_exec_context(exec_context_list[i])
                exec_subtree_list[i].exec_context.set_all_formula_idx(partition_plan)
            exec_subtree_list = [
                self.compiler.compile(exec_subtree_list[i], self.exec_config.function_executor)
                for i in range(cores)
            ]
            self.phase_scheduled = True
            return phasetwo_tree, exec_subtree_list
        return None, None

    def finish_subtree(self, execution_subtree: ExecutionNode, result_table: Table):
        ref_node = create_intermediate_ref_node(result_table, execution_subtree)
        parent = execution_subtree.parent
        if parent is not None:
            children = []
            for child in parent.children:
                if execution_subtree == child:
                    children.append(ref_node)
                else:
                    children.append(child)
            link_parent_to_children(parent, children)
        else:
            self.execution_tree = create_intermediate_ref_node(result_table, execution_subtree)
        self.idx += 1
        if self.idx == len(self.phasetwo_trees):
            self.phase_finished = True
        else:
            self.phase_scheduled = False


class Phases(Enum):
    SIMPLE = auto()
    FF = auto()
    RFFRPhaseOne = auto()
    RFFRPhaseTwo = auto()


scheduler_class_dict = {
    Phases.SIMPLE.name.lower(): SimplePhase,
    Phases.FF.name.lower(): FFPhase,
    Phases.RFFRPhaseOne.name.lower(): RFFRPhaseOne,
    Phases.RFFRPhaseTwo.name.lower(): RFFRPhaseTwo,
}


def create_phase_by_name(
    p_name: str, compiler: BaseCompiler, exec_config: ExecutionConfig, execution_tree: ExecutionNode
):
    if p_name.lower() in scheduler_class_dict.keys():
        return scheduler_class_dict[p_name](compiler, exec_config, execution_tree)
    raise PhaseNotSupportedException(f"Phase {p_name} is not supported")
