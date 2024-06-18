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

from forms.planner.plannode import PlanNode, FunctionNode
from forms.planner.logicalrule import RewritingRule, db_full_rewrite_rule_list, df_full_rewrite_rule_list
from forms.planner.physicalrule import full_physical_rule_list
from forms.utils.treenode import link_parent_to_children


def apply_one_rule(plan_tree: PlanNode, rule: RewritingRule) -> PlanNode:
    if not isinstance(plan_tree, FunctionNode):
        return plan_tree
    new_plan_tree = rule.rewrite(plan_tree)
    new_children = [
        apply_one_rule(child, rule) if isinstance(child, FunctionNode) else child
        for child in new_plan_tree.children
    ]
    link_parent_to_children(new_plan_tree, new_children)
    return new_plan_tree


def rewrite_plan(
    root: PlanNode, df_enable_rewriting: bool = False, db_enable_rewriting: bool = False
) -> PlanNode:
    plan_tree = root
    if df_enable_rewriting or db_enable_rewriting:
        if df_enable_rewriting:
            for rule in df_full_rewrite_rule_list:
                plan_tree = apply_one_rule(plan_tree, rule)
        else:
            for rule in db_full_rewrite_rule_list:
                plan_tree = apply_one_rule(plan_tree, rule)

        for rule in full_physical_rule_list:
            plan_tree = apply_one_rule(plan_tree, rule)

    return plan_tree
