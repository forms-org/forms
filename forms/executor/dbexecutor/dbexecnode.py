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


from forms.core.catalog import TableCatalog
from forms.planner.plannode import FunctionNode, LiteralNode, PlanNode, RefNode
from forms.utils.exceptions import FormSException
from forms.utils.reference import ORIGIN_REF, Ref, RefType
from forms.utils.treenode import TreeNode, link_parent_to_children
from forms.utils.functions import Function

from abc import ABC, abstractmethod


class DBExecNode(ABC, TreeNode):
    def __init__(self, out_ref_type: RefType):
        super().__init__()
        self.out_ref_type = out_ref_type

    @abstractmethod
    def collect_ref_nodes_in_order(self) -> list:
        pass


class DBRefExecNode(DBExecNode):
    def __init__(self, ref: Ref, table: TableCatalog, out_ref_type: RefType):
        super().__init__(out_ref_type)
        self.ref = ref
        self.table = table

    def collect_ref_nodes_in_order(self) -> list:
        return [self]


class DBLitExecNode(DBExecNode):
    def __init__(self, literal, out_ref_type: RefType):
        super().__init__(out_ref_type)
        self.literal = literal

    def collect_ref_nodes_in_order(self) -> list:
        return []


class DBFuncExecNode(DBExecNode):
    def __init__(self, function: Function, out_ref_type: RefType):
        super().__init__(out_ref_type)
        self.function = function
        self.is_subtree_root = False
        self.subtree_id = 0

    def collect_ref_nodes_in_order(self) -> list:
        ref_children = []
        for child in self.children:
            ref_children.extend(child.collect_ref_nodes_in_order())
        return ref_children


def from_plan_to_execution_tree(plan_node: PlanNode, table: TableCatalog) -> DBExecNode:
    if isinstance(plan_node, RefNode):
        ref_node = DBRefExecNode(plan_node.ref, table, plan_node.out_ref_type)
        ref_node.copy_formula_string_info_from(plan_node)
        return ref_node
    elif isinstance(plan_node, LiteralNode):
        lit_node = DBLitExecNode(plan_node.literal, plan_node.out_ref_type)
        lit_node.copy_formula_string_info_from(plan_node)
        return lit_node
    elif isinstance(plan_node, FunctionNode):
        parent = DBFuncExecNode(plan_node.function, plan_node.out_ref_type)
        parent.copy_formula_string_info_from(plan_node)
        children = []
        for child in plan_node.children:
            children.append(from_plan_to_execution_tree(child, table))
        link_parent_to_children(parent, children)
        return parent
    else:
        raise FormSException("Unknown plan node type: {}".format(type(plan_node)))


def create_intermediate_ref_node(table: TableCatalog, exec_subtree: DBFuncExecNode) -> DBFuncExecNode:
    out_ref_type = (
        RefType.RR
        if exec_subtree.out_ref_type != RefType.FF or exec_subtree.out_ref_type != RefType.LIT
        else exec_subtree.out_ref_type
    )
    ref_node = DBRefExecNode(ORIGIN_REF, table, out_ref_type)
    return ref_node
