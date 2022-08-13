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

from forms.planner.plannode import *
from forms.executor.table import Table
from forms.executor.utils import ExecutionContext
from forms.utils.treenode import TreeNode, link_parent_to_children

from abc import ABC, abstractmethod


class ExecutionNode(ABC, TreeNode):
    def __init__(self, out_ref_type: RefType, out_ref_dir: RefDirection):
        super().__init__()
        self.out_ref_type = out_ref_type
        self.out_ref_dir = out_ref_dir

    @abstractmethod
    def replicate_subtree(self):
        pass

    @abstractmethod
    def set_exec_context(self, exec_context: ExecutionContext):
        pass


class FunctionExecutionNode(ExecutionNode):
    def __init__(self, function: Function, out_ref_type: RefType, out_ref_dir: RefDirection):
        super().__init__(out_ref_type, out_ref_dir)
        self.function = function
        self.exec_context = None

    def replicate_subtree(self):
        parent = FunctionExecutionNode(self.function, self.out_ref_type, self.out_ref_dir)
        children = [child.replicate_subtree() for child in self.children]
        link_parent_to_children(parent, children)
        return parent

    def set_exec_context(self, exec_context: ExecutionContext):
        self.exec_context = exec_context


class RefExecutionNode(ExecutionNode):
    def __init__(self, ref: Ref, table: Table, out_ref_type: RefType, out_ref_dir: RefDirection):
        super().__init__(out_ref_type, out_ref_dir)
        self.table = table
        self.ref = ref

    def replicate_subtree(self):
        return RefExecutionNode(self.ref, self.table, self.out_ref_type, self.out_ref_dir)

    def set_exec_context(self, exec_context: ExecutionContext):
        pass


class LitExecutionNode(ExecutionNode):
    def __init__(self, literal, out_ref_type: RefType, out_ref_dir: RefDirection):
        super().__init__(out_ref_type, out_ref_dir)
        self.literal = literal

    def replicate_subtree(self):
        return LitExecutionNode(self.literal, self.out_ref_type, self.out_ref_dir)

    def set_exec_context(self, exec_context: ExecutionContext):
        pass


def from_plan_to_execution_tree(plan_node: PlanNode, table: Table) -> ExecutionNode:
    if isinstance(plan_node, RefNode):
        return RefExecutionNode(plan_node.ref, plan_node.out_ref_type, plan_node.out_ref_dir)
    elif isinstance(plan_node, LiteralNode):
        return LitExecutionNode(plan_node.literal, plan_node.out_ref_type, plan_node.out_ref_dir)
    elif isinstance(plan_node, FunctionNode):
        parent = FunctionExecutionNode(plan_node.function, plan_node.out_ref_type, plan_node.out_ref_dir)
        children = [from_plan_to_execution_tree(child, table) for child in plan_node.children]
        link_parent_to_children(parent, children)
        return parent
    assert False


def create_intermediate_ref_node(table: Table, exec_subtree: ExecutionNode) -> RefExecutionNode:
    ref = Ref(0, 0)
    return RefExecutionNode(ref, table, exec_subtree.out_ref_type, exec_subtree.out_ref_dir)
