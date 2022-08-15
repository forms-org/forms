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
from forms.planner.logicalrule import RewritingRule
from forms.planner.plannode import PlanNode, FunctionNode, RefNode
from forms.utils.functions import distributive_functions, algebraic_functions
from forms.utils.optimizations import FRRFOptimization
from forms.utils.reference import RefType
from forms.utils.treenode import link_parent_to_children


class FactorOutPhysicalRule(RewritingRule):
    @staticmethod
    def rewrite(plan_node: FunctionNode) -> FunctionNode:
        new_plan_node = plan_node
        if plan_node.function in distributive_functions or plan_node.function in algebraic_functions:
            if (
                plan_node.fr_rf_optimization == FRRFOptimization.NOOPT
                and len(plan_node.children) == 1
                and isinstance(plan_node.children[0], RefNode)
            ):
                if (
                    plan_node.children[0].out_ref_type == RefType.RF
                    or plan_node.children[0].out_ref_type == RefType.FR
                ):
                    new_plan_node = plan_node.replicate_node()
                    link_parent_to_children(new_plan_node, [plan_node])
                    new_plan_node.fr_rf_optimization = FRRFOptimization.PHASETWO
                    plan_node.fr_rf_optimization = FRRFOptimization.PHASEONE

        return new_plan_node


full_physical_rule_list = [FactorOutPhysicalRule]
