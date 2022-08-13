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
from forms.utils.reference import Ref, RefType, RefDirection
from forms.utils.functions import Function, is_arithmetic_function
from forms.utils.exceptions import InvalidArithmeticInputException
from forms.utils.treenode import TreeNode


class PlanNode(ABC, TreeNode):
    def __init__(self):
        super().__init__()
        self.out_ref_type = None
        self.out_ref_dir = None

    @abstractmethod
    def populate_ref_info(self):
        pass

    @abstractmethod
    def validate(self):
        pass


class RefNode(PlanNode):
    def __init__(self, ref: Ref, ref_type: RefType, ref_dir=RefDirection.DOWN):
        super().__init__()
        self.ref = ref
        self.out_ref_type = ref_type
        self.out_ref_dir = ref_dir

    def populate_ref_info(self):
        pass

    def validate(self):
        pass


class LiteralNode(PlanNode):
    def __init__(self, literal):
        super().__init__()
        self.literal = literal
        self.out_ref_type = RefType.LIT
        self.lit_type = type(literal)
        self.out_ref_dir = RefDirection.NODIR

    def populate_ref_info(self):
        pass

    def validate(self):
        pass


class FunctionNode(PlanNode):
    def __init__(self, function: Function):
        super().__init__()
        self.function = function

    def populate_ref_info(self):
        for child in self.children:
            child.populate_ref_info()
        if all(
            child.out_ref_type == RefType.FF or child.out_ref_type == RefType.LIT
            for child in self.children
        ):
            self.out_ref_type = RefType.FF
        else:
            self.out_ref_type = RefType.RR

    def validate(self):
        for child in self.children:
            child.validate()
        if is_arithmetic_function(self.function):
            if any(is_reference_range(child) for child in self.children):
                raise InvalidArithmeticInputException(
                    "Not supporting range" "input for arithmetic functions"
                )


def is_reference_range(node: PlanNode) -> bool:
    if isinstance(node, RefNode):
        return not node.ref.is_cell
    return False
