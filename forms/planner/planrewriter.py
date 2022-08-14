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

from forms.planner.plannode import PlanNode
from forms.planner.logicalrule import RewritingRule, full_rewriting_rule_list
from forms.planner.physicalrule import full_physical_rule_list
from forms.core.config import FormSConfig
from forms.utils.treenode import link_parent_to_children


def apply_one_rule(plan_tree: PlanNode, rule: RewritingRule) -> PlanNode:
    new_plan_tree = rule.rewrite(plan_tree)
    new_children = [rule.rewrite(child) for child in new_plan_tree.children]
    link_parent_to_children(new_plan_tree, new_children)
    return new_plan_tree


class PlanRewriter:
    def __init__(self, form_config: FormSConfig):
        self.forms_config = form_config

    def rewrite_plan(self, root: PlanNode) -> PlanNode:
        plan_tree = root
        if self.forms_config.enable_logical_rewriting:
            for rule in full_rewriting_rule_list:
                plan_tree = apply_one_rule(plan_tree, rule)

        if self.forms_config.enable_physical_opt:
            for rule in full_physical_rule_list:
                plan_tree = apply_one_rule(plan_tree, rule)

        return plan_tree
