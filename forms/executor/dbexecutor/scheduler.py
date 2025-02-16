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
)
from forms.utils.functions import (
    DB_AGGREGATE_FUNCTIONS,
    DB_AGGREGATE_IF_FUNCTIONS,
    DB_CELL_REFERENCE_FUNCTIONS,
    Function,
)
from forms.utils.reference import RefType
from forms.core.catalog import TEMP_TABLE_PREFIX


def break_down_into_subtrees(exec_tree: DBExecNode, enable_pipelining: bool) -> list:
    if isinstance(exec_tree, DBFuncExecNode):
        set_translatable_to_window(exec_tree)
        subtrees = []
        generate_subtrees(exec_tree, True, enable_pipelining, subtrees)
        return subtrees
    else:
        return [exec_tree]


def set_translatable_to_window(func_node: DBFuncExecNode) -> None:
    if func_node.function in DB_AGGREGATE_FUNCTIONS and len(func_node.children) == 1:
        func_node.translatable_to_window = True
    elif func_node.function in DB_AGGREGATE_IF_FUNCTIONS:
        if isinstance(func_node.children[1], DBLitExecNode):
            func_node.translatable_to_window = True
    elif func_node.function in DB_CELL_REFERENCE_FUNCTIONS:
        func_node.translatable_to_window = True
    elif func_node.function == Function.INDEX:
        if (
            func_node.children[0].out_ref_type != RefType.FF
            and func_node.children[1].out_ref_type == RefType.RR
        ):
            if len(func_node.children) == 3 and isinstance(func_node.children[2], DBLitExecNode):
                func_node.translatable_to_window = True
    else:
        func_node.translatable_to_window = False

    for child in func_node.children:
        if isinstance(child, DBFuncExecNode):
            set_translatable_to_window(child)


def generate_subtrees(
    exec_tree: DBFuncExecNode, init_search: bool, enable_pipelining: bool, subtrees: list
):
    if exec_tree.translatable_to_window and enable_pipelining:
        if init_search:
            subtrees.append(exec_tree)
        for child in exec_tree.children:
            if isinstance(child, DBFuncExecNode):
                generate_subtrees(child, False, enable_pipelining, subtrees)
    else:
        subtrees.append(exec_tree)
        for child in exec_tree.children:
            if isinstance(child, DBFuncExecNode):
                generate_subtrees(child, True, enable_pipelining, subtrees)


class Scheduler:
    def __init__(self, exec_tree: DBExecNode, enable_pipelining: bool):
        self.exec_tree = exec_tree
        self.subtrees = break_down_into_subtrees(exec_tree, enable_pipelining)
        for subtree_index, subtree in enumerate(self.subtrees):
            if isinstance(subtree, DBFuncExecNode):
                subtree.set_intermediate_table_name(TEMP_TABLE_PREFIX + str(subtree_index))

    def next_subtree(self) -> DBExecNode:
        return self.subtrees.pop()

    def has_next_subtree(self) -> bool:
        return len(self.subtrees) > 0

    def get_num_subtrees(self) -> int:
        return len(self.subtrees)
