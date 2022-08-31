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
    distributive_functions,
    from_function_to_open_value,
    close_value,
    FunctionType,
)
from forms.utils.reference import RefType, Ref, origin, axis_along_row
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
            plan_node.close_value = close_value
            plan_node.func_type = FunctionType.FUNC
        return plan_node


# Factor-out rules are adopted to optimize FF/FR/RF cases
def factor_out(child: PlanNode, parent: FunctionNode) -> PlanNode:
    new_child = child
    if isinstance(child, RefNode) and (
        child.out_ref_type != RefType.RR and child.out_ref_type != RefType.LIT
    ):
        if parent.function in distributive_functions:
            new_child = parent.replicate_node()
            new_child.seps = []
            link_parent_to_children(new_child, [child])
        elif parent.function == Function.AVG:
            new_child = parent.replicate_node()
            new_child.function = Function.SUM
            new_child.seps = []
            link_parent_to_children(new_child, [child])
    return new_child


class DistFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if len(plan_node.children) > 1:
            new_children = [factor_out(child, plan_node) for child in plan_node.children]
            link_parent_to_children(plan_node, new_children)
        return plan_node


class AlgebraicFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        return DistFactorOutRule.rewrite(plan_node)


# Factor-in rules are adopted to optimize the RR case
def factor_in(child: PlanNode, parent: FunctionNode) -> list:
    new_children = [child]
    if isinstance(child, FunctionNode):
        if (
            parent.function in distributive_functions
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


class AlgebraicFactorInRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        return DistFactorInRule.rewrite(plan_node)


if_push_down_threshold = 1


def roll_up_ref(ref_node: RefNode):
    axis = ref_node.out_ref_axis
    ref = ref_node.ref
    ref_node.ref = (
        Ref(ref.row, ref.col, ref.row, ref.last_col)
        if axis == axis_along_row
        else Ref(ref.row, ref.col, ref.last_row, ref.col)
    )
    ref_node.out_ref_type = RefType.RR


def create_new_function_node(plan_node: FunctionNode, function: Function) -> FunctionNode:
    new_node = plan_node.replicate_node()
    new_node.function = function
    new_node.open_value = function.name.lower() + "("
    return new_node


def if_push_down(plan_node: FunctionNode, new_function: Function) -> FunctionNode:
    if isinstance(plan_node.children[0], RefNode):
        ref_node = plan_node.children[0]
        ref = ref_node.ref
        axis = ref_node.out_ref_axis
        if (
            ref_node.out_ref_type != RefType.FF
            and ref.get_row_or_column_count(axis) > if_push_down_threshold
        ):
            new_parent = create_new_function_node(plan_node, new_function)
            new_parent.seps = []

            plan_node.ref = (
                Ref(origin, origin, ref.last_row - ref.row, origin)
                if axis == axis_along_row
                else Ref(origin, origin, origin, ref.last_col - ref.col)
            )
            plan_node.out_ref_type = ref_node.out_ref_type

            for child in plan_node.children:
                if isinstance(child, RefNode):
                    roll_up_ref(child)
            link_parent_to_children(new_parent, [plan_node])
            return new_parent

    return plan_node


class IfPushDownRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.SUMIF:
            return if_push_down(plan_node, Function.SUM)
        elif plan_node.function == Function.COUNTIF:
            return if_push_down(plan_node, Function.SUM)
        return plan_node


def rewrite_average_if(plan_node: FunctionNode) -> FunctionNode:
    sum_if = create_new_function_node(plan_node, Function.SUMIF)
    sum_if.seps = []
    sum_if_children = [child.replicate_node() for child in plan_node.children]
    link_parent_to_children(sum_if, sum_if_children)

    count_if = create_new_function_node(plan_node, Function.COUNTIF)
    sum_if.seps = []
    count_if_children = [child.replicate_node() for child in plan_node.children]
    link_parent_to_children(count_if, count_if_children)

    divide = plan_node.replicate_node()
    divide.function = Function.DIVIDE
    divide.func_type = Token.OP_IN
    divide.open_value = "/"
    divide.close_value = None
    divide.seps = []

    link_parent_to_children(divide, [sum_if, count_if])
    return divide


class AverageIfRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.AVERAGEIF:
            return rewrite_average_if(plan_node)
        return plan_node


factor_out_rule_list = [DistFactorOutRule, AlgebraicFactorOutRule]
factor_in_rule_list = [DistFactorInRule, AlgebraicFactorInRule]
if_push_down_rule_list = [AverageIfRule, IfPushDownRule]

full_rewriting_rule_list = [PlusToSumRule]
full_rewriting_rule_list.extend(factor_out_rule_list)
full_rewriting_rule_list.extend(if_push_down_rule_list)
full_rewriting_rule_list.extend(factor_in_rule_list)
