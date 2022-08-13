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


class RewritingRule(ABC):
    @staticmethod
    @abstractmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


class PlusToSumRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


class DistFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


class DistFactorInRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


class AlgebraicFactorOutRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


class AlgebraicFactorInRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: PlanNode) -> PlanNode:
        pass


factor_out_rule_list = [DistFactorOutRule, AlgebraicFactorOutRule]

factor_in_rule_list = [DistFactorInRule, AlgebraicFactorInRule]

full_rewriting_rule_list = [PlusToSumRule]
full_rewriting_rule_list.extend(factor_out_rule_list)
full_rewriting_rule_list.extend(factor_in_rule_list)
