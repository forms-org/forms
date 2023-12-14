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

from forms.core.config import DFExecContext
from forms.executor.dfexecutor.dftable import DFTable
from forms.utils.reference import Ref, RefType, ORIGIN_REF
from forms.utils.treenode import TreeNode, link_parent_to_children
from forms.utils.functions import Function
from forms.planner.plannode import PlanNode, RefNode, FunctionNode, LiteralNode

from abc import ABC


class DFExecNode(ABC, TreeNode):
    def __init__(self, out_ref_type: RefType, out_ref_axis: int):
        super().__init__()
        self.out_ref_type = out_ref_type
        self.out_ref_axis = out_ref_axis
        self.exec_context = None

    def set_exec_context(self, exec_context: DFExecContext):
        self.exec_context = exec_context
        for child in self.children:
            child.set_exec_context(exec_context)


class DFFuncExecNode(DFExecNode):
    def __init__(self, function: Function, ref: Ref, out_ref_type: RefType, out_ref_axis: int):
        super().__init__(out_ref_type, out_ref_axis)
        self.ref = ref
        self.function = function

    def collect_ref_nodes_in_order(self) -> list:
        ref_children = []
        for child in self.children:
            ref_children.extend(child.collect_ref_nodes_in_order())
        return ref_children


class DFRefExecNode(DFExecNode):
    def __init__(self, ref: Ref, table: DFTable, out_ref_type: RefType, out_ref_axis: int):
        super().__init__(out_ref_type, out_ref_axis)
        self.table = table
        self.ref = ref
        self.row_offset = ORIGIN_REF.row
        self.col_offset = ORIGIN_REF.col

    def collect_ref_nodes_in_order(self) -> list:
        return [self]


class DFLitExecNode(DFExecNode):
    def __init__(self, literal, out_ref_type: RefType, out_ref_axis: int):
        super().__init__(out_ref_type, out_ref_axis)
        self.literal = literal

    def collect_ref_nodes_in_order(self) -> list:
        return []


def from_plan_to_execution_tree(plan_node: PlanNode, table: DFTable) -> DFExecNode:
    if isinstance(plan_node, RefNode):
        ref_node = DFRefExecNode(plan_node.ref, table, plan_node.out_ref_type, plan_node.out_ref_axis)
        ref_node.copy_formula_string_info_from(plan_node)
        return ref_node
    elif isinstance(plan_node, LiteralNode):
        lit_node = DFLitExecNode(plan_node.literal, plan_node.out_ref_type, plan_node.out_ref_axis)
        lit_node.copy_formula_string_info_from(plan_node)
        return lit_node
    elif isinstance(plan_node, FunctionNode):
        parent = DFFuncExecNode(
            plan_node.function, plan_node.ref, plan_node.out_ref_type, plan_node.out_ref_axis
        )
        parent.copy_formula_string_info_from(plan_node)
        children = [from_plan_to_execution_tree(child, table) for child in plan_node.children]
        link_parent_to_children(parent, children)
        return parent
    assert False


def create_intermediate_ref_node(df_table: DFTable, exec_subtree: DFFuncExecNode) -> DFRefExecNode:
    out_ref_type = (
        RefType.RR
        if exec_subtree.out_ref_type != RefType.FF or exec_subtree.out_ref_type != RefType.LIT
        else exec_subtree.out_ref_type
    )
    ref_node = DFRefExecNode(ORIGIN_REF, df_table, out_ref_type, exec_subtree.out_ref_axis)
    ref_node.set_exec_context(exec_subtree.exec_context)
    return ref_node
