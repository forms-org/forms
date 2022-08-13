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

from forms.planner.plannode import PlanNode, RefNode, LiteralNode, FunctionNode
from forms.utils.functions import Function, distributive_functions, algebraic_functions
from forms.utils.reference import RefType
from forms.utils.treenode import link_parent_to_children
from forms.utils.generic import same_list


class RewritingRule(ABC):
    @staticmethod
    @abstractmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


class PlusToSumRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        if isinstance(plan_node, FunctionNode) and plan_node.function == Function.PLUS:
            plan_node.function = Function.SUM
        return plan_node


# Factor-out rules are adopted to optimize FF/FR/RF cases
def factor_out(child: PlanNode, parent: FunctionNode) -> PlanNode:
    new_child = child
    if isinstance(child, RefNode) and (child.out_ref_type != RefType.RR and child.out_ref_type != RefType.LIT):
        if parent.function in distributive_functions:
            new_child = parent.replicate_node()
            link_parent_to_children(new_child, [child])
        elif parent.function == Function.AVG:
            new_child = parent.replicate_node()
            new_child.function = Function.SUM
            link_parent_to_children(new_child, [child])
    return new_child


class DistFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        if isinstance(plan_node, FunctionNode) and len(plan_node.children) > 1:
            new_children = [factor_out(child, plan_node) for child in plan_node.children]
            link_parent_to_children(plan_node, new_children)
        return plan_node


class AlgebraicFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        return DistFactorOutRule.rewrite(plan_node)


# Factor-in rules are adopted to optimize the RR case
def factor_in(child: PlanNode, parent: FunctionNode) -> list:
    new_children = [child]
    if isinstance(child, FunctionNode):
        if parent.function in distributive_functions and child.function == parent.function and \
                all([grandchild.out_ref_type == RefType.RR for grandchild in child.children]):
            new_children = child.children
        elif parent.function == Function.AVG and child.function == Function.SUM and \
                all([grandchild.out_ref_type == RefType.RR for grandchild in child.children]):
            new_children = child.children
    return new_children


class DistFactorInRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        if isinstance(plan_node, FunctionNode):
            while True:
                new_children = [new_child for child in plan_node.children
                                for new_child in factor_in(child, plan_node)]
                if same_list(new_children, plan_node.children):
                    break
                link_parent_to_children(plan_node, new_children)
        return plan_node


class AlgebraicFactorInRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        return DistFactorInRule.rewrite(plan_node)


factor_out_rule_list = [DistFactorOutRule, AlgebraicFactorOutRule]

factor_in_rule_list = [DistFactorInRule, AlgebraicFactorInRule]

full_rewriting_rule_list = [PlusToSumRule]
full_rewriting_rule_list.extend(factor_out_rule_list)
full_rewriting_rule_list.extend(factor_in_rule_list)
