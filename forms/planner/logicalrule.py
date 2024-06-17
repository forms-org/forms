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
from openpyxl.formula.tokenizer import Token

from forms.planner.plannode import PlanNode, RefNode, FunctionNode
from forms.utils.functions import (
    Function,
    DISTRIBUTIVE_FUNCTIONS,
    from_function_to_open_value,
    CLOSE_VALUE,
    FunctionType,
)
from forms.utils.reference import RefType, Ref, ORIGIN, AXIS_ALONG_ROW, DEFAULT_AXIS
from forms.utils.treenode import link_parent_to_children
from forms.utils.generic import same_list


class RewritingRule(ABC):
    @staticmethod
    @abstractmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        pass


class PlusToSumRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.PLUS:
            plan_node.function = Function.SUM
            plan_node.open_value = from_function_to_open_value(plan_node.function)
            plan_node.close_value = CLOSE_VALUE
            plan_node.func_type = FunctionType.FUNC
        return plan_node


# Factor-out rules are adopted to optimize FF/FR/RF cases
def factor_out(child: PlanNode, parent: FunctionNode) -> PlanNode:
    new_child = child
    if (
            isinstance(child, RefNode)
            and child.out_ref_type != RefType.LIT
            and (parent.function in DISTRIBUTIVE_FUNCTIONS)
    ):
        new_child = parent.replicate_node()
        new_child.seps = []
        link_parent_to_children(new_child, [child])
        new_child.out_ref_type = child.out_ref_type
    return new_child


class DistFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        ret_plan_node = plan_node
        if len(plan_node.children) > 1:
            new_children = [factor_out(child, plan_node) for child in plan_node.children]
            link_parent_to_children(plan_node, new_children)
            if plan_node.function == Function.SUM:
                ret_plan_node = DistFactorOutRule.convert_sum_to_plus(plan_node)
        return ret_plan_node

    @staticmethod
    def convert_sum_to_plus(plan_node: FunctionNode) -> FunctionNode:
        parent = plan_node.parent
        children = plan_node.children
        while len(children) > 1:
            plus_children = children[0:2]
            plus_parent = FunctionNode(Function.PLUS, DEFAULT_AXIS)
            link_parent_to_children(plus_parent, plus_children)
            children = [plus_parent] + children[2:]
        if parent is not None:
            link_parent_to_children(parent, children)
        return children[0]


# Factor-in rules are adopted to optimize the RR case
def factor_in(child: PlanNode, parent: FunctionNode) -> list:
    new_children = [child]
    if isinstance(child, FunctionNode):
        if (
                parent.function in DISTRIBUTIVE_FUNCTIONS
                and child.function == parent.function
                and all([grandchild.out_ref_type == RefType.RR for grandchild in child.children])
        ):
            new_children = child.children
            parent.seps += child.seps
        elif (
                parent.function == Function.AVG
                and child.function == Function.SUM
                and all([grandchild.out_ref_type == RefType.RR for grandchild in child.children])
        ):
            new_children = child.children
            parent.seps += child.seps
    return new_children


class DistFactorInRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        while True:
            new_children = [
                new_child for child in plan_node.children for new_child in factor_in(child, plan_node)
            ]
            if same_list(new_children, plan_node.children):
                break
            link_parent_to_children(plan_node, new_children)
        return plan_node


def create_new_function_node(plan_node: FunctionNode, function: Function) -> FunctionNode:
    new_node = plan_node.replicate_node()
    new_node.function = function
    new_node.open_value = function.name.lower() + "("
    return new_node


def rewrite_average(plan_node: FunctionNode) -> FunctionNode:
    sum_node = create_new_function_node(plan_node, Function.SUM)
    sum_node.seps = []
    sum_node_children = [child.replicate_node() for child in plan_node.children]
    link_parent_to_children(sum_node, sum_node_children)

    count_node = create_new_function_node(plan_node, Function.COUNT)
    sum_node.seps = []
    count_node_children = [child.replicate_node() for child in plan_node.children]
    link_parent_to_children(count_node, count_node_children)

    divide = plan_node.replicate_node()
    divide.function = Function.DIVIDE
    divide.func_type = Token.OP_IN
    divide.open_value = "/"
    divide.close_value = None
    divide.seps = []

    link_parent_to_children(divide, [sum_node, count_node])
    return divide


class AverageRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.AVG:
            return rewrite_average(plan_node)
        return plan_node


# full_rewriting_rule_list = [AverageRule, PlusToSumRule, DistFactorOutRule, DistFactorInRule]
full_rewrite_rule_list = [DistFactorOutRule]
