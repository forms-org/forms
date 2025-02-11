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
from forms.utils.reference import Ref, RefType, ORIGIN_REF, DEFAULT_AXIS
from forms.utils.functions import Function
from forms.utils.treenode import TreeNode, link_parent_to_children


class PlanNode(ABC, TreeNode):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def populate_ref_info(self):
        pass

    @abstractmethod
    def replicate_node(self):
        pass

    @abstractmethod
    def replicate_node_recursive(self):
        pass


class RefNode(PlanNode):
    def __init__(self, ref: Ref, ref_type: RefType, ref_axis = DEFAULT_AXIS):
        super().__init__()
        self.ref = ref
        self.out_ref_type = ref_type
        self.out_ref_axis = ref_axis

    def populate_ref_info(self):
        pass

    def replicate_node(self):
        ref_node = RefNode(self.ref, self.out_ref_type, self.out_ref_axis)
        ref_node.open_value = self.open_value
        return ref_node
    
    def replicate_node_recursive(self):
        return self.replicate_node() 


class LiteralNode(PlanNode):
    def __init__(self, literal, ref_axis = DEFAULT_AXIS):
        super().__init__()
        self.literal = literal
        self.out_ref_type = RefType.LIT
        self.lit_type = type(literal)
        self.out_ref_axis = ref_axis

    def populate_ref_info(self):
        pass

    def replicate_node(self):
        literal_node = LiteralNode(self.literal, self.out_ref_axis)
        literal_node.open_value = self.open_value
        return literal_node
    
    def replicate_node_recursive(self):
        return self.replicate_node() 


class FunctionNode(PlanNode):
    def __init__(self, function: Function, ref_axis = DEFAULT_AXIS):
        super().__init__()
        self.ref = ORIGIN_REF
        self.out_ref_axis = ref_axis
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

    def replicate_node(self):
        function_node = FunctionNode(self.function, self.out_ref_axis)
        function_node.ref = self.ref
        function_node.out_ref_type = self.out_ref_type
        function_node.open_value = self.open_value
        function_node.func_type = self.func_type
        function_node.seps = self.seps
        function_node.close_value = self.close_value
        return function_node
    
    def replicate_node_recursive(self):
        func_node = self.replicate_node()
        children = [child.replicate_node_recursive() for child in self.children]
        link_parent_to_children(func_node, children)
        return func_node

def is_reference_range(node: PlanNode) -> bool:
    if isinstance(node, RefNode):
        return not node.ref.is_cell
    return False
