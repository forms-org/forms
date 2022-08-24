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

import pytest

from forms.parser.parser import parse_formula
from forms.planner.planrewriter import PlanRewriter
from forms.planner.plannode import PlanNode, FunctionNode, RefNode
from forms.core.forms import config
from forms.core.config import forms_config
from forms.utils.functions import Function
from forms.utils.optimizations import FRRFOptimization
from forms.utils.reference import default_axis


@pytest.fixture(autouse=True)
def execute_before_and_after_one_test():
    config(enable_logical_rewriting=True, enable_physical_opt=True)

    yield


def gen_one_test_case(formula: str) -> PlanNode:
    root = parse_formula(formula, default_axis)
    root.validate()
    root.populate_ref_info()

    plan_rewriter = PlanRewriter(forms_config)
    root = plan_rewriter.rewrite_plan(root)
    return root


def test_plus_to_sum_rewriter():
    root = gen_one_test_case("=A1 + B1")

    assert type(root) == FunctionNode
    assert root.function == Function.SUM


def test_dist_factor_out_rewriter():
    forms_config.enable_physical_opt = False
    root = gen_one_test_case("=SUM(A1:B1, $A$1:B1)")

    assert root.function == Function.SUM
    assert any(
        [child.function == Function.SUM for child in root.children if isinstance(child, FunctionNode)]
    )


def test_algebraic_factor_out_rewriter():
    forms_config.enable_physical_opt = False
    root = gen_one_test_case("=Average(A1:B1, $A$1:B1)")

    assert root.function == Function.AVG
    assert any(
        [child.function == Function.SUM for child in root.children if isinstance(child, FunctionNode)]
    )


def test_dist_factor_in_rewriter():
    forms_config.enable_physical_opt = False
    root = gen_one_test_case("=SUM(SUM(A1:B1),C1:D2)")

    assert root.function == Function.SUM
    assert all(isinstance(child, RefNode) for child in root.children)


def test_algebraic_factor_in_rewriter():
    forms_config.enable_physical_opt = False
    root = gen_one_test_case("=Average(SUM(A1:B1), C1:D2)")

    assert root.function == Function.AVG
    assert all(isinstance(child, RefNode) for child in root.children)


def test_physical_opt():
    forms_config.enable_physical_opt = True
    root = gen_one_test_case("=SUM($A$1:B1)")

    assert root.function == Function.SUM
    assert root.fr_rf_optimization == FRRFOptimization.PHASETWO

    assert root.children[0].function == Function.SUM
    assert root.children[0].fr_rf_optimization == FRRFOptimization.PHASEONE
