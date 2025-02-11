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

from forms.planner.plannode import PlanNode, RefNode, FunctionNode, LiteralNode
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
        return ret_plan_node


class DBDistFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        ret_plan_node = plan_node
        if len(plan_node.children) > 1 and plan_node.function in DISTRIBUTIVE_FUNCTIONS:
            dist_func = plan_node.function
            new_children = [
                DBDistFactorOutRule.add_dist_func(child, dist_func) for child in plan_node.children
            ]
            if dist_func == Function.MAX or dist_func == Function.MIN:
                comb_func = Function.GREATEST if dist_func == Function.MAX else Function.LEAST
                ret_plan_node = FunctionNode(comb_func)
                link_parent_to_children(ret_plan_node, new_children)
            else:
                comb_func = Function.PLUS
                while len(new_children) != 1:
                    new_child_right = new_children.pop()
                    new_child_left = new_children.pop()
                    comb_func_node = FunctionNode(comb_func)
                    link_parent_to_children(comb_func_node, [new_child_left, new_child_right])
                    new_children.append(comb_func_node)
                ret_plan_node = new_children[0]

        return ret_plan_node

    @staticmethod
    def add_dist_func(child: PlanNode, dist_func: Function) -> PlanNode:
        if isinstance(child, RefNode):
            func_node = FunctionNode(dist_func)
            link_parent_to_children(func_node, [child])
            return func_node
        return child

    @staticmethod
    def factor_out_sum(plan_node: FunctionNode) -> FunctionNode:
        parent = plan_node.parent
        children = plan_node.children
        while len(children) > 1:
            plus_children = children[0:2]
            plus_parent = FunctionNode(Function.PLUS)
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


def df_rewrite_average(plan_node: FunctionNode, is_if_variant: bool) -> FunctionNode:
    sum_node = plan_node.replicate_node_recursive()
    sum_node.function = Function.SUMIF if is_if_variant else Function.SUM

    count_node = plan_node.replicate_node_recursive()
    count_node.function = Function.COUNTIF if is_if_variant else Function.COUNT

    divide = FunctionNode(Function.DIVIDE)

    link_parent_to_children(divide, [sum_node, count_node])

    return divide


def db_rewrite_average(plan_node: FunctionNode, is_if_variant: bool) -> FunctionNode:
    sum_node = plan_node.replicate_node_recursive()
    sum_node.function = Function.SUMIF if is_if_variant else Function.SUM

    multiply = FunctionNode(Function.MULTIPLY)
    multiply.function = Function.MULTIPLY

    literal_node = LiteralNode(1.0, DEFAULT_AXIS)

    count_node = plan_node.replicate_node_recursive()
    count_node.function = Function.COUNTIF if is_if_variant else Function.COUNT

    divide = FunctionNode(Function.DIVIDE)

    link_parent_to_children(multiply, [sum_node, literal_node])
    link_parent_to_children(divide, [multiply, count_node])

    return divide


class AverageRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.AVG:
            return df_rewrite_average(plan_node, False)
        return plan_node


class DBAvgIfRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.AVERAGEIF:
            return db_rewrite_average(plan_node, True)
        elif plan_node.function == Function.AVG and len(plan_node.children) > 1:
            return db_rewrite_average(plan_node, False)
        return plan_node


def rewrite_sumif(plan_node: FunctionNode) -> FunctionNode:
    plus_node = FunctionNode(Function.PLUS)
    sumif_node = plan_node.replicate_node_recursive()
    if_node = FunctionNode(Function.IF)

    equal_node = FunctionNode(Function.EQUAL)
    null_node = LiteralNode("NULL")
    zero_node_a = LiteralNode(0)

    countif_node = plan_node.replicate_node_recursive()
    countif_node.function = Function.COUNTIF
    zero_node_b = LiteralNode(0)

    link_parent_to_children(equal_node, [countif_node, zero_node_b])
    link_parent_to_children(if_node, [equal_node, null_node, zero_node_a])
    link_parent_to_children(plus_node, [sumif_node, if_node])

    return plus_node


class DBSumIfRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        if plan_node.function == Function.SUMIF:
            return rewrite_sumif(plan_node)
        return plan_node


# df_full_rewrite_rule_list = [AverageRule, PlusToSumRule, DistFactorOutRule, DistFactorInRule]
df_full_rewrite_rule_list = []
db_full_rewrite_rule_list = [DBAvgIfRule, DBDistFactorOutRule, DBSumIfRule]
