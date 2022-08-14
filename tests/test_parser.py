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

from forms.parser.parser import *
from forms.utils.functions import Function
from forms.utils.reference import Ref


def test_parser_single_formula_and_reference():
    root = parse_formula("=SUM(A1:B1)")
    assert type(root) == FunctionNode
    assert root.function == Function.SUM

    children = root.children
    child = children[0]
    expected_ref = Ref(0, 0, 0, 1)

    assert len(children) == 1
    assert type(child) == RefNode
    assert child.ref == expected_ref


def test_parser_multiple_formula_and_reference():
    root = parse_formula("=SUM(SUM(A1:B1), A2:B2)")
    assert root.function == Function.SUM

    left_child = root.children[0]
    right_child = root.children[1]

    expected_right_ref = Ref(1, 0, 1, 1)
    assert right_child.ref == expected_right_ref
    assert left_child.function == Function.SUM

    grand_child = left_child.children[0]

    expected_left_ref = Ref(0, 0, 0, 1)
    grand_child.ref == expected_left_ref
