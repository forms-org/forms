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


class PlanNode:
    def __init__(self, node_id: int):
        self._node_id = node_id
        self._parent = None
        self._children = None
        self._output_type = None

    def set_parent(self, parent):
        self._parent = parent

    def set_children(self, children):
        self._children = children

    def compute_output_type(self):
        pass


class FormulaNode(PlanNode):
    def __init__(self, node_id: int):
        super().__init__(node_id)


class RefNode(PlanNode):
    def __init__(self, node_id: int):
        super().__init__(node_id)
