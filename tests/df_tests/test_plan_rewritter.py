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
from forms.planner.plannode import PlanNode, FunctionNode, RefNode
from forms.core.config import DFConfig
from forms.planner.planrewriter import rewrite_plan
from forms.utils.functions import Function, FunctionExecutor
from forms.utils.reference import DEFAULT_AXIS
from forms.utils.validator import validate


def gen_one_test_case(formula: str) -> PlanNode:
    root = parse_formula(formula, DEFAULT_AXIS)
    root.populate_ref_info()
    validate(FunctionExecutor.DF_EXECUTOR, 1000, 1000, root)

    df_config = DFConfig(True)
    root = rewrite_plan(root, df_enable_rewriting=df_config.df_enable_rewriting)
    return root


def test_plus_to_sum_rewriter():
    root = gen_one_test_case("=A1 + B1")

    assert type(root) == FunctionNode
    assert root.function == Function.SUM


def test_dist_factor_out_rewriter():
    root = gen_one_test_case("=SUM(A1:B1, $A$1:B1)")

    assert root.function == Function.SUM
    assert any(
        [child.function == Function.SUM for child in root.children if isinstance(child, FunctionNode)]
    )


def test_dist_factor_in_rewriter():
    root = gen_one_test_case("=SUM(SUM(A1:B1),C1:D2)")

    assert root.function == Function.SUM
    assert all(isinstance(child, RefNode) for child in root.children)


def test_average_rewriter():
    root = gen_one_test_case("=Average(A1:B1, C1:D2)")

    assert root.function == Function.DIVIDE
    assert root.children[0].function == Function.SUM
    assert root.children[1].function == Function.COUNT
    assert all(isinstance(child, RefNode) for child in root.children[0].children)
    assert all(isinstance(child, RefNode) for child in root.children[1].children)
