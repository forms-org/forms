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
from forms.utils.reference import axis_along_row, default_axis
from forms.utils.treenode import TreeNode, link_parent_to_children

from abc import ABC, abstractmethod


class ExecutionNode(ABC, TreeNode):
    def __init__(self, out_ref_type: RefType, out_ref_axis: int):
        super().__init__()
        self.out_ref_type = out_ref_type
        self.out_ref_axis = out_ref_axis
        self.exec_context = None

    @abstractmethod
    def gen_exec_subtree(self):
        pass

    @abstractmethod
    def set_exec_context(self, exec_context: ExecutionContext):
        pass

    @abstractmethod
    def collect_ref_nodes_in_order(self) -> list:
        pass


class FunctionExecutionNode(ExecutionNode):
    def __init__(self, function: Function, out_ref_type: RefType, out_ref_axis: int):
        super().__init__(out_ref_type, out_ref_axis)
        self.function = function
        self.fr_rf_optimization = FRRFOptimization.NOOPT

    def gen_exec_subtree(self):
        parent = FunctionExecutionNode(self.function, self.out_ref_type, self.out_ref_axis)
        parent.fr_rf_optimization = self.fr_rf_optimization
        children = [child.gen_exec_subtree() for child in self.children]
        link_parent_to_children(parent, children)
        return parent

    def set_exec_context(self, exec_context: ExecutionContext):
        self.exec_context = exec_context
        for child in self.children:
            child.set_exec_context(exec_context)

    def collect_ref_nodes_in_order(self) -> list:
        ref_children = []
        for child in self.children:
            ref_children.extend(child.collect_ref_nodes_in_order())
        return ref_children


class RefExecutionNode(ExecutionNode):
    def __init__(self, ref: Ref, table: Table, out_ref_type: RefType, out_ref_axis: int):
        super().__init__(out_ref_type, out_ref_axis)
        self.table = table
        self.ref = ref

    def gen_exec_subtree(self):
        return RefExecutionNode(
            self.ref, self.table.gen_table_for_execution(), self.out_ref_type, self.out_ref_axis
        )

    def set_exec_context(self, exec_context: ExecutionContext):
        self.exec_context = exec_context

    def collect_ref_nodes_in_order(self) -> list:
        return [self]


class LitExecutionNode(ExecutionNode):
    def __init__(self, literal, out_ref_type: RefType, out_ref_axis: int):
        super().__init__(out_ref_type, out_ref_axis)
        self.literal = literal

    def gen_exec_subtree(self):
        return LitExecutionNode(self.literal, self.out_ref_type, self.out_ref_axis)

    def set_exec_context(self, exec_context: ExecutionContext):
        pass

    def collect_ref_nodes_in_order(self) -> list:
        return []


def from_plan_to_execution_tree(plan_node: PlanNode, table: Table) -> ExecutionNode:
    if isinstance(plan_node, RefNode):
        ref_node = RefExecutionNode(plan_node.ref, table, plan_node.out_ref_type, plan_node.out_ref_axis)
        ref_node.copy_formula_string_info_from(plan_node)
        return ref_node
    elif isinstance(plan_node, LiteralNode):
        lit_node = LitExecutionNode(plan_node.literal, plan_node.out_ref_type, plan_node.out_ref_axis)
        lit_node.copy_formula_string_info_from(plan_node)
        return lit_node
    elif isinstance(plan_node, FunctionNode):
        parent = FunctionExecutionNode(
            plan_node.function, plan_node.out_ref_type, plan_node.out_ref_axis
        )
        parent.fr_rf_optimization = plan_node.fr_rf_optimization
        parent.copy_formula_string_info_from(plan_node)
        children = [from_plan_to_execution_tree(child, table) for child in plan_node.children]
        link_parent_to_children(parent, children)
        return parent
    assert False


def create_intermediate_ref_node(table: Table, exec_subtree: ExecutionNode) -> RefExecutionNode:
    ref = Ref(0, 0)
    ref_node = RefExecutionNode(ref, table, exec_subtree.out_ref_type, exec_subtree.out_ref_axis)
    axis = exec_subtree.exec_context.axis if exec_subtree.exec_context is not None else default_axis
    ref_node.set_exec_context(
        ExecutionContext(
            0,
            table.get_num_of_rows() if axis == axis_along_row else table.get_num_of_columns(),
            axis=axis,
        )
    )
    return ref_node
