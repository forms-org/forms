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
from forms.core.config import FormSConfig
from forms.utils.reference import Ref, RefType, origin_ref
from forms.utils.functions import (
    Function,
    arithmetic_functions,
    pandas_supported_functions,
    formulas_supported_functions,
    FunctionExecutor,
)
from forms.utils.exceptions import InvalidArithmeticInputException, FunctionNotSupportedException
from forms.utils.treenode import TreeNode
from forms.utils.optimizations import FRRFOptimization


class PlanNode(ABC, TreeNode):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def populate_ref_info(self):
        pass

    @abstractmethod
    def validate(self, forms_config: FormSConfig):
        pass

    @abstractmethod
    def replicate_node(self):
        pass


class RefNode(PlanNode):
    def __init__(self, ref: Ref, ref_type: RefType, ref_axis):
        super().__init__()
        self.ref = ref
        self.out_ref_type = ref_type
        self.out_ref_axis = ref_axis

    def populate_ref_info(self):
        pass

    def validate(self, forms_config: FormSConfig):
        pass

    def replicate_node(self):
        ref_node = RefNode(self.ref, self.out_ref_type, self.out_ref_axis)
        ref_node.open_value = self.open_value
        return ref_node


class LiteralNode(PlanNode):
    def __init__(self, literal, ref_axis):
        super().__init__()
        self.literal = literal
        self.out_ref_type = RefType.LIT
        self.lit_type = type(literal)
        self.out_ref_axis = ref_axis

    def populate_ref_info(self):
        pass

    def validate(self, forms_config: FormSConfig):
        pass

    def replicate_node(self):
        literal_node = LiteralNode(self.literal, self.out_ref_axis)
        literal_node.open_value = self.open_value
        return literal_node


class FunctionNode(PlanNode):
    def __init__(self, function: Function, ref_axis):
        super().__init__()
        self.ref = origin_ref
        self.out_ref_axis = ref_axis
        self.function = function
        self.fr_rf_optimization = FRRFOptimization.NOOPT

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

    def validate(self, forms_config: FormSConfig):
        for child in self.children:
            child.validate(forms_config)
        if self.function in arithmetic_functions:
            if any(is_reference_range(child) for child in self.children):
                raise InvalidArithmeticInputException(
                    "Not supporting range input for arithmetic functions"
                )
        if (
            forms_config.function_executor == FunctionExecutor.df_pandas_executor.name.lower()
            and self.function not in pandas_supported_functions
        ):
            raise FunctionNotSupportedException(
                f"Function {self.function} is not supported by pandas executors"
            )
        if (
            forms_config.function_executor == FunctionExecutor.df_formulas_executor.name.lower()
            and self.function not in formulas_supported_functions
        ):
            raise FunctionNotSupportedException(
                f"Function {self.function} is not supported by formula executors"
            )

    def replicate_node(self):
        function_node = FunctionNode(self.function, self.out_ref_axis)
        function_node.ref = self.ref
        function_node.out_ref_type = self.out_ref_type
        function_node.open_value = self.open_value
        function_node.func_type = self.func_type
        function_node.seps = self.seps
        function_node.close_value = self.close_value
        return function_node


def is_reference_range(node: PlanNode) -> bool:
    if isinstance(node, RefNode):
        return not node.ref.is_cell
    return False
