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

from openpyxl.formula.tokenizer import Token


class TreeNode:
    def __init__(self):
        self.parent = None
        self.children = None
        self.out_ref_type = None
        self.out_ref_axis = None

        # for reconstructing a formula string
        self.open_value = None
        self.seps = []
        self.func_type = None
        self.close_value = None

    def copy_formula_string_info_from(self, from_node):
        self.open_value = from_node.open_value
        self.seps = from_node.seps
        self.func_type = from_node.func_type
        self.close_value = from_node.close_value

    def construct_formula_string(self) -> str:
        if self.func_type == Token.FUNC:
            assert len(self.children) == len(self.seps) + 1
            func_str = self.open_value.join(self.children[0].construct_formula_string())
            for child_idx in range(len(self.children) - 1):
                func_str = func_str.join(self.seps[child_idx + 1])\
                    .join(self.children[child_idx + 1].construct_formula_string())
            return func_str.join(self.close_value)
        elif self.func_type == Token.OP_IN:
            assert len(self.children) == 2
            left_subexpression = self.children[0].construct_formula_string()
            right_subexpression = self.children[1].construct_formula_string()
            return '{' + left_subexpression + self.open_value + right_subexpression + '}'
        elif self.func_type == Token.OP_PRE:
            assert len(self.children) == 1
            subexpression = self.children[0].construct_formula_string()
            return '{' + self.open_value + subexpression + '}'
        elif self.func_type == Token.OP_POST:
            assert len(self.children) == 1
            subexpression = self.children[0].construct_formula_string()
            return '{' + subexpression + self.open_value + '}'
        else:
            return self.open_value


def link_parent_to_children(parent: TreeNode, children: list):
    parent.children = children
    for child in children:
        child.parent = parent
