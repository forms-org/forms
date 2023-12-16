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

from forms.executor.dbexecutor.dbexecnode import (
    DBExecNode,
    DBFuncExecNode,
    DBLitExecNode,
    create_intermediate_ref_node,
)
from forms.utils.functions import (
    DB_AGGREGATE_FUNCTIONS,
    DB_AGGREGATE_IF_FUNCTIONS,
    DB_CELL_REFERENCE_FUNCTIONS,
    Function,
)
from forms.utils.reference import RefType
from forms.utils.treenode import link_parent_to_children


def break_down_into_subtrees(exec_tree: DBExecNode) -> list:
    if isinstance(exec_tree, DBFuncExecNode):
        set_translatable_to_window(exec_tree)
        subtrees = []
        generate_subtrees(exec_tree, True, subtrees)
        return subtrees
    else:
        return [exec_tree]


def set_translatable_to_window(func_node: DBFuncExecNode):
    if func_node.function in DB_AGGREGATE_FUNCTIONS and len(func_node.children) == 1:
        func_node.translatable_to_window = True
    elif func_node.function in DB_AGGREGATE_IF_FUNCTIONS:
        if func_node.children[0].out_ref_type == RefType.RR and isinstance(
            func_node.children[1], DBLitExecNode
        ):
            if len(func_node.children) == 3 and func_node.children[2].out_ref_type == RefType.RR:
                func_node.translatable_to_window = True
    elif func_node.function in DB_CELL_REFERENCE_FUNCTIONS:
        func_node.translatable_to_window = True
    elif func_node.function == Function.INDEX:
        if (
            func_node.children[0].out_ref_type != RefType.FF
            and func_node.children[1].out_ref_type == RefType.RR
        ):
            if len(func_node.children) == 3 and func_node.children[2].out_ref_type == RefType.FF:
                func_node.translatable_to_window = True
    else:
        func_node.translatable_to_window = False

    for child in func_node.children:
        if isinstance(child, DBFuncExecNode):
            set_translatable_to_window(child)


def generate_subtrees(exec_tree: DBFuncExecNode, init_search: bool, subtrees: list):
    if exec_tree.translatable_to_window:
        if init_search:
            subtrees.append(exec_tree)
        for child in exec_tree.children:
            if isinstance(child, DBFuncExecNode):
                generate_subtrees(child, False, subtrees)
    else:
        subtrees.append(exec_tree)
        for child in exec_tree.children:
            if isinstance(child, DBFuncExecNode):
                generate_subtrees(child, True, subtrees)


class Scheduler:
    def __init__(self, exec_tree: DBExecNode):
        self.exec_tree = exec_tree
        self.subtrees = break_down_into_subtrees(exec_tree)

    def next_substree(self) -> DBExecNode:
        return self.subtrees.pop(0)

    def has_next_subtree(self) -> bool:
        return len(self.subtrees) > 0

    def finish_one_subtree(self, exec_subtree: DBExecNode, intermediate_table):
        intermediate_ref_node = create_intermediate_ref_node(intermediate_table, exec_subtree)

        parent = exec_subtree.parent
        children = parent.children
        children[children.index(exec_subtree)] = intermediate_ref_node

        link_parent_to_children(parent, children)
