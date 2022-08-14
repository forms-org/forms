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

import forms
from forms.parser.parser import *


def test_parser():
    forms.config(2, "simple")
    root = parse_formula("=SUM(A1:B1)")
    assert type(root) == FunctionNode
    children = root.children
    assert len(children) == 1
    child = children[0]
    assert type(child) == RefNode
