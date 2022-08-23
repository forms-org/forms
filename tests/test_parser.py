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

from forms.parser.parser import parse_formula
from forms.planner.plannode import FunctionNode, RefNode
from forms.utils.functions import Function
from forms.utils.reference import Ref, RefType, default_axis


def evaluate_single_reference(ref_str: str,
                              expected_ref: Ref,
                              expected_ref_type: RefType):
    root = parse_formula(f"=SUM({ref_str})", default_axis)
    assert type(root) == FunctionNode
    assert root.function == Function.SUM

    children = root.children
    child = children[0]

    assert len(children) == 1
    assert type(child) == RefNode
    assert child.ref == expected_ref
    assert child.out_ref_type == expected_ref_type


def test_parser_single_formula_and_reference():
    expected_ref = Ref(0, 0, 0, 1)
    evaluate_single_reference("A1:B1", expected_ref, RefType.RR)

    expected_ref = Ref(100, 26, 100, 27)
    evaluate_single_reference("AA$101:AB101", expected_ref, RefType.FR)


def test_parser_multiple_formula_and_reference():
    root = parse_formula("=SUM(A1 + B1 + 1, A2:B2)", default_axis)
    assert isinstance(root, FunctionNode)
    assert root.function == Function.SUM

    left_child = root.children[0]
    right_child = root.children[1]

    expected_right_ref = Ref(1, 0, 1, 1)
    assert right_child.ref == expected_right_ref
    assert left_child.function == Function.PLUS

    grand_left_child = left_child.children[0]
    grand_right_child = left_child.children[1]

    expected_left_ref = Ref(0, 0)
    expected_right_ref = Ref(0, 1)
    grand_left_child.ref == expected_left_ref
    grand_right_child.ref == expected_right_ref


def test_parser_arithmetic_expression_one():
    root = parse_formula("=A1 + A1 + A1 + A1", default_axis)
    expected_ref = Ref(0, 0)
    count = 0
    while isinstance(root, FunctionNode):
        assert root.function == Function.PLUS
        assert root.children[1].ref == expected_ref
        root = root.children[0]
        count += 1
    assert count == 3


def test_parser_arithmetic_expression_two():
    root = parse_formula("=A1 + (A1 + (A1 + A1))", default_axis)
    expected_ref = Ref(0, 0)
    count = 0
    while isinstance(root, FunctionNode):
        assert root.function == Function.PLUS
        assert root.children[0].ref == expected_ref
        root = root.children[1]
        count += 1
    assert count == 3


def test_parser_arithmetic_expression_three():
    root = parse_formula("=A1 + 1 + 1 + 1", default_axis)
    expected_literal = 1.0
    count = 0
    while isinstance(root, FunctionNode):
        assert root.function == Function.PLUS
        assert root.children[1].literal == expected_literal
        root = root.children[0]
        count += 1
    assert count == 3
